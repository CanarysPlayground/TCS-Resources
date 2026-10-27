#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "pyjwt[crypto]>=2.8.0",
#     "cryptography>=42.0.0",
#     "openpyxl>=3.1.0",
# ]
# ///
"""
Dependencies:
  - pip install "pyjwt[crypto]>=2.8.0" "cryptography>=42.0.0" "openpyxl>=3.1.0"

Usage:
  python repo_settings_enforcer.py --config config.json --repos repos.csv
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Dependency Validation
try:
    import jwt
except ImportError:
    print(
        "[FATAL] Required dependency 'pyjwt' with crypto support is not installed.\n"
        "Install using: pip install \"pyjwt[crypto]>=2.8.0\" \"cryptography>=42.0.0\"",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False


# Indian Standard Time (IST) Zone (+05:30)
IST_TIMEZONE = timezone(timedelta(hours=5, minutes=30))


def get_current_ist_str() -> str:
    """Returns human-readable, non-DB formatted IST datetime: e.g. 27-Oct-2026 04:30:15 PM IST."""
    return datetime.now(IST_TIMEZONE).strftime("%d-%b-%Y %I:%M:%S %p IST")


def get_filename_timestamp() -> str:
    """Returns safe timestamp string for appending to filenames: e.g. 20261027_163015."""
    return datetime.now(IST_TIMEZONE).strftime("%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Structured Logging
# ---------------------------------------------------------------------------
def _log(level: str, message: str) -> None:
    timestamp = datetime.now(IST_TIMEZONE).strftime("%d-%b-%Y %I:%M:%S %p IST")
    print(f"[{timestamp}] [{level:<7}] {message}")


def log_info(msg: str) -> None:
    _log("INFO", msg)


def log_warn(msg: str) -> None:
    _log("WARN", msg)


def log_error(msg: str) -> None:
    _log("ERROR", msg)


def log_success(msg: str) -> None:
    _log("SUCCESS", msg)


# ---------------------------------------------------------------------------
# Data Models & Config Validation
# ---------------------------------------------------------------------------
class RepoTarget:
    """Represents a repository and its targeted ruleset IDs."""

    def __init__(self, name: str, ruleset_ids: List[int]):
        self.name: str = name
        self.ruleset_ids: List[int] = ruleset_ids
        self.repo_id: Optional[int] = None
        self.actions_enabled: bool = False
        self.actions_status_detail: str = "PENDING"
        self.ruleset_results: Dict[int, Dict[str, str]] = {}
        self.error: Optional[str] = None

    @property
    def overall_status(self) -> str:
        if self.error or not self.actions_enabled:
            return "FAILED"
        if any(res.get("status") == "FAILED" for res in self.ruleset_results.values()):
            return "FAILED"
        return "SUCCESS"


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Malformed JSON in configuration file: {e}")

    # Required enterprise keys
    if not config.get("organization"):
        raise ValueError("Missing 'organization' in config.json")


    api_url = config.get("api_base_url", "https://api.github.com").strip().rstrip("/")
    config["api_base_url"] = api_url

    has_app = bool(
        config.get("github_app", {}).get("app_id")
        and config.get("github_app", {}).get("private_key_path")
    )
    has_token = bool(config.get("auth_token"))

    if not (has_app or has_token):
        raise ValueError("Must provide either 'github_app' credentials or 'auth_token' in config.json")

    if has_app and not has_token:
        key_path = Path(config["github_app"]["private_key_path"])
        if not key_path.is_file():
            raise FileNotFoundError(
                f"GitHub App private key not found at: {key_path}\n"
                "Please place your .pem private key file at this location or update 'private_key_path' in config.json."
            )


    if "actions_policy" not in config:
        config["actions_policy"] = {"enabled": True, "allowed_actions": "all"}

    return config


def parse_ruleset_ids(raw_value: Any) -> List[int]:
    """Parses single ID, comma-separated, semicolon-separated, or list of ruleset IDs."""
    if isinstance(raw_value, list):
        return [int(x) for x in raw_value if str(x).strip().isdigit()]

    if not raw_value:
        return []

    cleaned = re.split(r"[,\s;]+", str(raw_value).strip())
    ids: List[int] = []
    for item in cleaned:
        if item.isdigit():
            ids.append(int(item))
    return sorted(list(set(ids)))


def load_and_validate_repos(csv_path: str, default_ruleset_ids: List[int]) -> List[RepoTarget]:
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"Repositories CSV file not found: {csv_path}")

    repo_name_regex = re.compile(r"^[a-zA-Z0-9._-]+$")
    valid_targets: List[RepoTarget] = []
    seen: Set[str] = set()

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV file is empty or missing headers: {csv_path}")

        # Match columns case-insensitively
        repo_col = None
        ruleset_col = None

        for col in reader.fieldnames:
            normalized = col.strip().lower()
            if normalized in ("repository_name", "repo_name", "repository", "repo"):
                repo_col = col
            elif normalized in ("ruleset_ids", "ruleset_id", "rulesets", "ruleset"):
                ruleset_col = col

        if not repo_col:
            raise ValueError(
                f"CSV must contain a 'repository_name' column. Available headers: {reader.fieldnames}"
            )

        for line_no, row in enumerate(reader, start=2):
            raw_repo = row.get(repo_col, "")
            if not raw_repo:
                continue
            repo_name = raw_repo.strip()
            if not repo_name:
                continue

            if not repo_name_regex.match(repo_name):
                log_warn(f"Line {line_no}: Skipping invalid repository name '{repo_name}'.")
                continue

            if repo_name in seen:
                log_warn(f"Line {line_no}: Skipping duplicate repository '{repo_name}'.")
                continue

            seen.add(repo_name)

            # Determine ruleset IDs
            row_rulesets = []
            if ruleset_col and row.get(ruleset_col):
                row_rulesets = parse_ruleset_ids(row.get(ruleset_col))

            target_rulesets = row_rulesets if row_rulesets else list(default_ruleset_ids)
            valid_targets.append(RepoTarget(repo_name, target_rulesets))

    if not valid_targets:
        raise ValueError(f"No valid repository records found in {csv_path}")

    log_info(f"Loaded {len(valid_targets)} valid target repositories from {csv_path}")
    return valid_targets


# ---------------------------------------------------------------------------
# GitHub App Authentication Lifecycle
# ---------------------------------------------------------------------------
class GitHubAuthManager:
    """Handles GitHub App JWT signing, installation tokens, and token auto-refresh."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.api_base_url = config.get("api_base_url", "https://api.github.com").rstrip("/")
        self.cached_token: Optional[str] = None
        self.token_expiry_epoch: float = 0.0

        self.static_token = config.get("auth_token", "").strip()
        self.app_cfg = config.get("github_app", {})

    def _generate_app_jwt(self) -> str:
        app_id = str(self.app_cfg["app_id"])
        key_path = Path(self.app_cfg["private_key_path"])

        with open(key_path, "r", encoding="utf-8") as f:
            private_key = f.read()

        now = int(time.time())
        payload = {
            "iat": now - 300,
            "exp": now + 300,
            "iss": app_id,
        }
        return jwt.encode(payload, private_key, algorithm="RS256")

    def get_token(self) -> str:
        if self.static_token:
            return self.static_token

        if self.cached_token and time.time() < (self.token_expiry_epoch - 120):
            return self.cached_token

        log_info("Generating new GitHub App installation access token...")
        jwt_token = self._generate_app_jwt()
        installation_id = self.app_cfg.get("installation_id")

        if not installation_id:
            raise ValueError("Missing 'installation_id' in github_app configuration")

        token_url = f"{self.api_base_url}/app/installations/{installation_id}/access_tokens"
        headers = [
            f"Authorization: Bearer {jwt_token}",
            "Accept: application/vnd.github+json",
            "X-GitHub-Api-Version: 2022-11-28",
        ]

        status_code, _, body = run_curl_command(
            method="POST",
            url=token_url,
            headers=headers,
            json_payload={},
        )

        if status_code != 201:
            raise RuntimeError(
                f"Failed to obtain installation token (HTTP {status_code}): {body}"
            )

        token_data = json.loads(body)
        self.cached_token = token_data["token"]
        expires_at_str = token_data.get("expires_at")
        if expires_at_str:
            exp_dt = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
            self.token_expiry_epoch = exp_dt.timestamp()
        else:
            self.token_expiry_epoch = time.time() + 3500

        log_info("Successfully acquired GitHub App installation token.")
        return self.cached_token


# ---------------------------------------------------------------------------
# cURL-Based API Engine with Rate Limiting
# ---------------------------------------------------------------------------
def run_curl_command(
    method: str,
    url: str,
    headers: List[str],
    json_payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 60,
) -> Tuple[int, Dict[str, str], str]:
    """Executes an HTTP request using the native curl command."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as header_file:
        header_path = header_file.name

    payload_path = None
    try:
        cmd = [
            "curl",
            "-s",
            "-S",
            "-X",
            method,
            url,
            "-D",
            header_path,
            "--max-time",
            str(timeout_seconds),
        ]

        for h in headers:
            cmd.extend(["-H", h])

        if json_payload is not None:
            with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as p_file:
                json.dump(json_payload, p_file)
                payload_path = p_file.name
            cmd.extend(["-d", f"@{payload_path}"])

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(f"curl process failed (code {result.returncode}): {result.stderr.strip()}")

        body = result.stdout

        # Parse response headers
        parsed_headers: Dict[str, str] = {}
        status_code = 0
        if os.path.exists(header_path):
            with open(header_path, "r", encoding="utf-8", errors="ignore") as hf:
                for line in hf:
                    line_clean = line.strip()
                    if line_clean.startswith("HTTP/"):
                        parts = line_clean.split(" ", 2)
                        if len(parts) >= 2 and parts[1].isdigit():
                            status_code = int(parts[1])
                    elif ":" in line_clean:
                        k, v = line_clean.split(":", 1)
                        parsed_headers[k.strip().lower()] = v.strip()

        return status_code, parsed_headers, body

    finally:
        if os.path.exists(header_path):
            try:
                os.remove(header_path)
            except OSError:
                pass
        if payload_path and os.path.exists(payload_path):
            try:
                os.remove(payload_path)
            except OSError:
                pass


def execute_github_api_request(
    auth_mgr: GitHubAuthManager,
    method: str,
    endpoint: str,
    json_payload: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any]]:
    """Executes a GitHub REST API request with proactive and reactive rate limit handling."""
    api_base = (config or {}).get("api_base_url", "https://api.github.com").rstrip("/")
    url = f"{api_base}{endpoint}" if endpoint.startswith("/") else f"{api_base}/{endpoint}"

    rl_cfg = (config or {}).get("rate_limit", {})
    max_retries = rl_cfg.get("max_retries", 5)
    backoff_factor = rl_cfg.get("backoff_factor_seconds", 5)
    min_remaining_threshold = rl_cfg.get("min_remaining_threshold", 15)

    for attempt in range(1, max_retries + 1):
        token = auth_mgr.get_token()
        headers = [
            f"Authorization: Bearer {token}",
            "Accept: application/vnd.github+json",
            "X-GitHub-Api-Version: 2022-11-28",
            "Content-Type: application/json",
            "User-Agent: GitHub-EMU-Settings-Enforcer/2.0",
        ]

        status_code, resp_headers, body = run_curl_command(
            method=method,
            url=url,
            headers=headers,
            json_payload=json_payload,
        )

        remaining = int(resp_headers.get("x-ratelimit-remaining", 9999))
        reset_epoch = int(resp_headers.get("x-ratelimit-reset", 0))

        if remaining <= min_remaining_threshold and reset_epoch > 0:
            sleep_duration = max(reset_epoch - int(time.time()), 1) + 2
            log_warn(
                f"Rate limit remaining ({remaining}) is below threshold ({min_remaining_threshold}). "
                f"Pausing execution for {sleep_duration}s until reset..."
            )
            time.sleep(sleep_duration)

        if status_code in (403, 429):
            retry_after = resp_headers.get("retry-after")
            is_rate_limit = "rate limit" in body.lower() or remaining == 0 or retry_after

            if is_rate_limit:
                if retry_after and retry_after.isdigit():
                    sleep_time = int(retry_after) + 2
                elif reset_epoch > 0:
                    sleep_time = max(reset_epoch - int(time.time()), 1) + 3
                else:
                    sleep_time = backoff_factor * (2 ** (attempt - 1))

                log_warn(
                    f"Hit rate limit on {method} {endpoint} (HTTP {status_code}). "
                    f"Backing off for {sleep_time}s (Attempt {attempt}/{max_retries})..."
                )
                time.sleep(sleep_time)
                continue

        if status_code in (500, 502, 503, 504):
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            log_warn(
                f"GitHub server error (HTTP {status_code}). Retrying in {sleep_time}s "
                f"(Attempt {attempt}/{max_retries})..."
            )
            time.sleep(sleep_time)
            continue

        parsed_body = {}
        if body.strip():
            try:
                parsed_body = json.loads(body)
            except json.JSONDecodeError:
                parsed_body = {"raw_text": body}

        return status_code, parsed_body

    raise RuntimeError(f"Request failed after {max_retries} attempts: {method} {endpoint}")


# ---------------------------------------------------------------------------
# Business Operations: Actions Permissions & Multi-Ruleset Assignment
# ---------------------------------------------------------------------------
def resolve_repo_id(
    auth_mgr: GitHubAuthManager, org: str, repo: str, config: Dict[str, Any]
) -> Tuple[bool, Optional[int], str]:
    status, body = execute_github_api_request(auth_mgr, "GET", f"/repos/{org}/{repo}", config=config)
    if status == 200:
        return True, body["id"], ""
    if status == 404:
        return False, None, f"Repository '{org}/{repo}' not found."
    return False, None, f"HTTP {status}: {body.get('message', str(body))}"


def set_actions_permissions(
    auth_mgr: GitHubAuthManager, org: str, repo: str, config: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Sets GitHub Actions permissions. Enforces:
      enabled: True
      allowed_actions: "all" (Allow all workflows and actions)
    """
    policy = config.get("actions_policy", {"enabled": True, "allowed_actions": "all"})
    status, body = execute_github_api_request(
        auth_mgr,
        "PUT",
        f"/repos/{org}/{repo}/actions/permissions",
        json_payload=policy,
        config=config,
    )
    if status in (200, 204):
        return True, "Actions enabled: Allow all workflows and actions"
    return False, f"HTTP {status}: {body.get('message', str(body))}"


class RulesetAuditResult:
    """Detailed audit for a ruleset update."""

    def __init__(self, ruleset_id: int):
        self.ruleset_id: int = ruleset_id
        self.ruleset_name: str = ""
        self.status: str = "PENDING"
        self.repos_before: int = 0
        self.repos_after: int = 0
        self.newly_added_ids: List[int] = []
        self.already_present_ids: List[int] = []
        self.error_message: Optional[str] = None


def apply_ruleset_batch(
    auth_mgr: GitHubAuthManager,
    org: str,
    ruleset_id: int,
    target_repo_ids: Set[int],
    config: Dict[str, Any],
) -> RulesetAuditResult:
    """Fetches ruleset, performs atomic deduplicated merge, and updates via PUT."""
    audit = RulesetAuditResult(ruleset_id)

    log_info(f"Fetching Organization Ruleset {ruleset_id} details...")
    status, ruleset_data = execute_github_api_request(
        auth_mgr, "GET", f"/orgs/{org}/rulesets/{ruleset_id}", config=config
    )

    if status != 200:
        audit.status = "FAILED"
        audit.error_message = f"Failed to retrieve ruleset: HTTP {status} - {ruleset_data.get('message', ruleset_data)}"
        return audit

    audit.ruleset_name = ruleset_data.get("name", f"Ruleset-{ruleset_id}")
    conditions = ruleset_data.get("conditions", {})
    repo_condition = conditions.get("repository_id", {})
    existing_ids = set(repo_condition.get("repository_ids", []))

    audit.repos_before = len(existing_ids)
    audit.newly_added_ids = sorted(list(target_repo_ids.difference(existing_ids)))
    audit.already_present_ids = sorted(list(target_repo_ids.intersection(existing_ids)))

    if not audit.newly_added_ids:
        audit.status = "ENFORCED"
        audit.repos_after = len(existing_ids)
        log_info(f"Ruleset '{audit.ruleset_name}' ({ruleset_id}): All {len(target_repo_ids)} target repositories are already actively enforced.")
        return audit

    conditions.pop("repository_property", None)
    conditions.pop("repository_name", None)

    merged_ids = sorted(list(existing_ids.union(target_repo_ids)))
    repo_id_payload: Dict[str, Any] = {"repository_ids": merged_ids}
    if "do_not_enforce_on_create" in repo_condition:
        repo_id_payload["do_not_enforce_on_create"] = repo_condition["do_not_enforce_on_create"]
    conditions["repository_id"] = repo_id_payload

    update_payload = {
        "name": ruleset_data.get("name"),
        "target": ruleset_data.get("target"),
        "enforcement": ruleset_data.get("enforcement"),
        "bypass_actors": ruleset_data.get("bypass_actors", []),
        "conditions": conditions,
        "rules": ruleset_data.get("rules", []),
    }

    log_info(
        f"Updating Ruleset '{audit.ruleset_name}' (ID {ruleset_id}): Adding {len(audit.newly_added_ids)} repo(s) "
        f"(Count: {audit.repos_before} -> {len(merged_ids)})..."
    )

    put_status, put_body = execute_github_api_request(
        auth_mgr,
        "PUT",
        f"/orgs/{org}/rulesets/{ruleset_id}",
        json_payload=update_payload,
        config=config,
    )

    if put_status == 200:
        audit.status = "ENFORCED"
        audit.repos_after = len(merged_ids)
        log_success(f"Ruleset '{audit.ruleset_name}' ({ruleset_id}): Successfully enforced. Total repos: {len(merged_ids)} (+{len(audit.newly_added_ids)} newly attached).")
    else:
        audit.status = "FAILED"
        audit.error_message = f"HTTP {put_status} - {put_body.get('message', put_body)}"
        log_error(f"Ruleset '{audit.ruleset_name}' ({ruleset_id}) update failed: {audit.error_message}")

    return audit


# ---------------------------------------------------------------------------
# Single CSV & Single Excel Report Generators (Complete Run Coverage)
# ---------------------------------------------------------------------------
def generate_single_csv_report(
    config: Dict[str, Any],
    targets: List[RepoTarget],
    ruleset_audits: Dict[int, RulesetAuditResult],
    output_dir: str = ".",
    timestamp_suffix: Optional[str] = None,
) -> str:
    """Generates exactly ONE comprehensive CSV audit report covering the full run with timestamp in filename."""
    ts_file = timestamp_suffix or get_filename_timestamp()
    repo_csv_name = f"enforcement_report_{ts_file}.csv"
    repo_csv_path = os.path.join(output_dir, repo_csv_name)

    ist_str = get_current_ist_str()

    try:
        f = open(repo_csv_path, "w", newline="", encoding="utf-8")
    except PermissionError:
        repo_csv_name = f"enforcement_report_{ts_file}_alt.csv"
        repo_csv_path = os.path.join(output_dir, repo_csv_name)
        log_warn(f"CSV file is locked. Saving output to: {repo_csv_path}")
        f = open(repo_csv_path, "w", newline="", encoding="utf-8")

    with f:
        writer = csv.writer(f)
        writer.writerow([
            "organization",
            "repository_name",
            "repo_id",
            "actions_status",
            "actions_policy",
            "target_ruleset_ids",
            "ruleset_enforcement_status",
            "overall_status",
            "error_details",
            "execution_timestamp_ist",
        ])
        for t in targets:
            ruleset_details_list = []
            for r_id in t.ruleset_ids:
                audit_info = ruleset_audits.get(r_id)
                r_name = audit_info.ruleset_name if audit_info else f"Ruleset-{r_id}"
                res = t.ruleset_results.get(r_id, {})
                status_str = res.get("status", "NOT_PROCESSED")
                action_str = res.get("action", "")
                display = f"{r_name} (ID {r_id}): {status_str}"
                if action_str:
                    display += f" [{action_str}]"
                ruleset_details_list.append(display)

            writer.writerow([
                config["organization"],
                t.name,
                t.repo_id or "",
                "ENABLED" if t.actions_enabled else "FAILED",
                "Allow all workflows and actions",
                ", ".join(map(str, t.ruleset_ids)),
                "; ".join(ruleset_details_list),
                t.overall_status,
                t.error or "",
                ist_str,
            ])

    log_info(f"Generated single comprehensive CSV report: {repo_csv_path}")
    return repo_csv_path


def generate_single_excel_report(
    config: Dict[str, Any],
    targets: List[RepoTarget],
    ruleset_audits: Dict[int, RulesetAuditResult],
    output_dir: str = ".",
    timestamp_suffix: Optional[str] = None,
) -> Optional[str]:
    """
    Generates exactly ONE multi-sheet Excel report covering the full run:
      Sheet 1: Summary (Executive Dashboard with center-aligned values & IST datetime)
      Sheet 2: Actions Enablement (Actions configuration per repo, without execution timestamp)
      Sheet 3: Ruleset Enablement (Ruleset enforcement status per repo)
    """
    if not OPENPYXL_AVAILABLE:
        log_warn("openpyxl is not installed. Skipping Excel (.xlsx) report generation.")
        return None

    ts_file = timestamp_suffix or get_filename_timestamp()
    excel_file = f"enforcement_report_{ts_file}.xlsx"
    excel_path = os.path.join(output_dir, excel_file)

    wb = openpyxl.Workbook()
    ist_str = get_current_ist_str()

    font_header = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    fill_header = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    fill_success = PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid")  # Soft Green
    fill_failed = PatternFill(start_color="FCE5CD", end_color="FCE5CD", fill_type="solid")   # Soft Red
    font_bold = Font(name="Calibri", size=11, bold=True)
    border_thin = Border(
        left=Side(style="thin", color="D9D9D9"),
        right=Side(style="thin", color="D9D9D9"),
        top=Side(style="thin", color="D9D9D9"),
        bottom=Side(style="thin", color="D9D9D9"),
    )

    align_center = Alignment(horizontal="center", vertical="center")
    align_left = Alignment(horizontal="left", vertical="center")

    # =========================================================================
    # SHEET 1: Summary (Executive Dashboard)
    # =========================================================================
    ws_summary = wb.active
    ws_summary.title = "Summary"
    ws_summary.views.sheetView[0].showGridLines = True

    ws_summary.append(["GitHub EMU Data Residency - Batch Enforcement Summary"])
    ws_summary.cell(row=1, column=1).font = Font(name="Calibri", size=14, bold=True, color="1F4E78")
    ws_summary.append([])

    actions_success = sum(1 for t in targets if t.actions_enabled)
    actions_failed = len(targets) - actions_success

    ruleset_fully_enforced_repos = sum(
        1 for t in targets if t.ruleset_results and all(r.get("status") == "ENFORCED" for r in t.ruleset_results.values())
    )

    summary_kpis = [
        ("Execution Date & Time (IST)", ist_str),
        ("Target Organization", config["organization"]),
        ("API Base Tenant URL", config["api_base_url"]),
        ("Actions Policy", "Allow all workflows and actions"),
        ("Total Repositories Processed", len(targets)),
        ("Actions Successfully Enabled", actions_success),
        ("Actions Failures", actions_failed),
        ("Repositories with Rulesets Enforced", ruleset_fully_enforced_repos),
        ("Rulesets Evaluated", len(ruleset_audits)),
        ("Overall Batch Execution Status", "SUCCESS" if all(t.overall_status == "SUCCESS" for t in targets) else "FAILURES_DETECTED"),
    ]

    for label, val in summary_kpis:
        ws_summary.append([label, val])
        r = ws_summary.max_row
        c1 = ws_summary.cell(row=r, column=1)
        c2 = ws_summary.cell(row=r, column=2)

        c1.font = font_bold
        c1.border = border_thin
        c1.alignment = align_left

        c2.border = border_thin
        c2.alignment = align_center
        if val == "SUCCESS":
            c2.font = font_bold
            c2.fill = fill_success
        elif val == "FAILURES_DETECTED":
            c2.font = font_bold
            c2.fill = fill_failed

    ws_summary.append([])
    ws_summary.append(["Ruleset Governance Overview"])
    ws_summary.cell(row=ws_summary.max_row, column=1).font = Font(name="Calibri", size=12, bold=True, color="1F4E78")

    ruleset_overview_headers = [
        "Ruleset ID",
        "Ruleset Name",
        "Status",
        "Total Active Repos",
        "Newly Enforced",
        "Pre-existing Enforced",
        "Details",
    ]
    ws_summary.append(ruleset_overview_headers)
    hdr_row = ws_summary.max_row
    for c_idx in range(1, len(ruleset_overview_headers) + 1):
        cell = ws_summary.cell(row=hdr_row, column=c_idx)
        cell.font = font_header
        cell.fill = fill_header
        cell.alignment = align_center

    for r_id, audit in ruleset_audits.items():
        row_vals = [
            audit.ruleset_id,
            audit.ruleset_name,
            audit.status,
            audit.repos_after,
            len(audit.newly_added_ids),
            len(audit.already_present_ids),
            audit.error_message or "All target repositories actively protected",
        ]
        ws_summary.append(row_vals)
        curr_r = ws_summary.max_row
        for c_idx in range(1, len(row_vals) + 1):
            cell = ws_summary.cell(row=curr_r, column=c_idx)
            cell.border = border_thin
            if c_idx in (1, 3, 4, 5, 6):  # ID, Status, and counts are center-aligned
                cell.alignment = align_center
            else:
                cell.alignment = align_left

        st_cell = ws_summary.cell(row=curr_r, column=3)
        st_cell.fill = fill_success if audit.status == "ENFORCED" else fill_failed

    ws_summary.column_dimensions["A"].width = 34
    ws_summary.column_dimensions["B"].width = 36
    ws_summary.column_dimensions["C"].width = 16
    ws_summary.column_dimensions["D"].width = 20
    ws_summary.column_dimensions["E"].width = 18
    ws_summary.column_dimensions["F"].width = 22
    ws_summary.column_dimensions["G"].width = 45

    # =========================================================================
    # SHEET 2: Actions Enablement (Actions Only, No Execution Timestamp)
    # =========================================================================
    ws_actions = wb.create_sheet(title="Actions Enablement")
    ws_actions.views.sheetView[0].showGridLines = True

    actions_headers = [
        "Repository Name",
        "Repository ID",
        "Actions Status",
        "Actions Permission Policy",
        "Details / Error",
    ]
    ws_actions.append(actions_headers)
    for cell in ws_actions[1]:
        cell.font = font_header
        cell.fill = fill_header
        cell.alignment = align_center

    for t in targets:
        row_vals = [
            t.name,
            t.repo_id or "N/A",
            "ENABLED" if t.actions_enabled else "FAILED",
            "Allow all workflows and actions",
            t.actions_status_detail if t.actions_enabled else (t.error or t.actions_status_detail),
        ]
        ws_actions.append(row_vals)
        curr_r = ws_actions.max_row

        for c_idx in range(1, len(row_vals) + 1):
            cell = ws_actions.cell(row=curr_r, column=c_idx)
            cell.border = border_thin
            if c_idx in (2, 3, 4):  # Repo ID, Status, Policy center-aligned
                cell.alignment = align_center
            else:
                cell.alignment = align_left

        status_cell = ws_actions.cell(row=curr_r, column=3)
        status_cell.fill = fill_success if t.actions_enabled else fill_failed

    ws_actions.freeze_panes = "A2"
    for col in ws_actions.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        col_letter = get_column_letter(col[0].column)
        ws_actions.column_dimensions[col_letter].width = max(max_len + 4, 16)

    # =========================================================================
    # SHEET 3: Ruleset Enablement (Ruleset Enforcement Only)
    # =========================================================================
    ws_ruleset = wb.create_sheet(title="Ruleset Enablement")
    ws_ruleset.views.sheetView[0].showGridLines = True

    ruleset_headers = [
        "Repository Name",
        "Repository ID",
        "Target Ruleset ID",
        "Ruleset Name",
        "Enforcement Status",
        "Membership Action",
        "Details",
    ]
    ws_ruleset.append(ruleset_headers)
    for cell in ws_ruleset[1]:
        cell.font = font_header
        cell.fill = fill_header
        cell.alignment = align_center

    for t in targets:
        for r_id in t.ruleset_ids:
            audit = ruleset_audits.get(r_id)
            r_name = audit.ruleset_name if audit else f"Ruleset-{r_id}"
            res = t.ruleset_results.get(r_id, {})
            enforcement_st = res.get("status", "FAILED")
            action_type = res.get("action", "Not Applied")
            detail_msg = audit.error_message if (audit and audit.status == "FAILED") else "Protected by organization ruleset"

            row_vals = [
                t.name,
                t.repo_id or "N/A",
                r_id,
                r_name,
                enforcement_st,
                action_type,
                detail_msg,
            ]
            ws_ruleset.append(row_vals)
            curr_r = ws_ruleset.max_row

            for c_idx in range(1, len(row_vals) + 1):
                cell = ws_ruleset.cell(row=curr_r, column=c_idx)
                cell.border = border_thin
                if c_idx in (2, 3, 5, 6):  # Repo ID, Ruleset ID, Status, Action center-aligned
                    cell.alignment = align_center
                else:
                    cell.alignment = align_left

            st_cell = ws_ruleset.cell(row=curr_r, column=5)
            st_cell.fill = fill_success if enforcement_st == "ENFORCED" else fill_failed

    ws_ruleset.freeze_panes = "A2"
    for col in ws_ruleset.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        col_letter = get_column_letter(col[0].column)
        ws_ruleset.column_dimensions[col_letter].width = max(max_len + 4, 16)

    try:
        wb.save(excel_path)
    except PermissionError:
        excel_path = os.path.join(output_dir, f"enforcement_report_{ts_file}_alt.xlsx")
        log_warn(
            f"'{excel_file}' is currently open in Excel or locked by another application. "
            f"Saving output report to: {excel_path}"
        )
        wb.save(excel_path)

    log_info(f"Generated 3-sheet Excel audit report: {excel_path}")
    return excel_path


# ---------------------------------------------------------------------------
# Main Orchestration Loop
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enforce GitHub Actions and Multi-Ruleset targeting for GitHub EMU Data Residency."
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to JSON configuration file (default: config.json)",
    )
    parser.add_argument(
        "--repos",
        default="repos.csv",
        help="Path to CSV file with repositories and ruleset_ids (default: repos.csv)",
    )
    args = parser.parse_args()

    print("=" * 76)
    print(" GitHub EMU Data Residency - Actions & Multi-Ruleset Enforcer")
    print("=" * 76)

    # 1. Load Configuration
    try:
        config = load_config(args.config)
        log_info(f"Loaded configuration from: {args.config}")
        log_info(f"Target Organization: {config['organization']}")
        log_info(f"API Base URL: {config['api_base_url']}")
    except Exception as e:
        log_error(f"Configuration error: {e}")
        sys.exit(1)

    default_rulesets = parse_ruleset_ids(config.get("default_ruleset_ids", []))
    if not default_rulesets and "ruleset_id" in config:
        default_rulesets = [int(config["ruleset_id"])]

    try:
        targets = load_and_validate_repos(args.repos, default_rulesets)
    except Exception as e:
        log_error(f"Repositories CSV validation error: {e}")
        sys.exit(1)

    org = config["organization"]
    auth_mgr = GitHubAuthManager(config)

    try:
        auth_mgr.get_token()
    except Exception as e:
        log_error(f"Authentication failure: {e}")
        sys.exit(1)

    ruleset_to_repos: Dict[int, Set[int]] = defaultdict(set)
    repo_id_to_target: Dict[int, RepoTarget] = {}

    log_info(f"Initiating Actions enforcement for {len(targets)} repositories...")

    for idx, target in enumerate(targets, start=1):
        log_info(f"[{idx}/{len(targets)}] Processing '{org}/{target.name}'...")

        ok, repo_id, err_msg = resolve_repo_id(auth_mgr, org, target.name, config)
        if not ok or repo_id is None:
            target.error = err_msg
            target.actions_status_detail = "FAILED_ACCESS"
            log_error(f"Failed to resolve {target.name}: {err_msg}")
            continue

        target.repo_id = repo_id
        repo_id_to_target[repo_id] = target

        act_ok, act_msg = set_actions_permissions(auth_mgr, org, target.name, config)
        if not act_ok:
            target.error = act_msg
            target.actions_status_detail = "FAILED_ACTIONS"
            log_error(f"Failed to enable Actions on {target.name}: {act_msg}")
            continue

        target.actions_enabled = True
        target.actions_status_detail = "Actions enabled: Allow all workflows and actions"
        log_success(f"Actions verified (allow all workflows & actions) on {target.name} (ID: {repo_id})")

        for r_id in target.ruleset_ids:
            ruleset_to_repos[r_id].add(repo_id)

    ruleset_audits: Dict[int, RulesetAuditResult] = {}
    all_ruleset_ids = sorted(list(ruleset_to_repos.keys()))

    if all_ruleset_ids:
        log_info(f"Processing multi-ruleset assignments across {len(all_ruleset_ids)} ruleset(s)...")

        for r_id in all_ruleset_ids:
            target_ids = ruleset_to_repos[r_id]
            audit = apply_ruleset_batch(auth_mgr, org, r_id, target_ids, config)
            ruleset_audits[r_id] = audit

            for repo_id in target_ids:
                target_obj = repo_id_to_target.get(repo_id)
                if target_obj:
                    if audit.status == "ENFORCED":
                        action_type = "Newly Added" if repo_id in audit.newly_added_ids else "Already Present"
                        target_obj.ruleset_results[r_id] = {
                            "status": "ENFORCED",
                            "action": action_type,
                        }
                    else:
                        target_obj.ruleset_results[r_id] = {
                            "status": "FAILED",
                            "action": "Failed",
                        }
                        if not target_obj.error:
                            target_obj.error = f"Ruleset {r_id} update failed: {audit.error_message}"
    else:
        log_warn("No rulesets queued for update.")

    run_timestamp = get_filename_timestamp()
    csv_out = generate_single_csv_report(config, targets, ruleset_audits, timestamp_suffix=run_timestamp)
    xlsx_out = generate_single_excel_report(config, targets, ruleset_audits, timestamp_suffix=run_timestamp)

    print("\n" + "=" * 76)
    print(" ENFORCEMENT SUMMARY")
    print("=" * 76)
    total_successful = sum(1 for t in targets if t.overall_status == "SUCCESS")
    total_failed = len(targets) - total_successful

    print(f" Organization         : {org}")
    print(f" Total Repos in CSV   : {len(targets)}")
    print(f" Fully Successful     : {total_successful}")
    print(f" Repos with Failures  : {total_failed}")
    print(f" Rulesets Processed   : {len(ruleset_audits)}")

    for r_id, audit in ruleset_audits.items():
        print(f"   - Ruleset {r_id} ({audit.ruleset_name}): {audit.status} (Total: {audit.repos_after}, Added: {len(audit.newly_added_ids)}, Existing: {len(audit.already_present_ids)})")

    print(f" Reports Generated    : {csv_out}, {xlsx_out}")

    if total_failed > 0 or any(a.status == "FAILED" for a in ruleset_audits.values()):
        print("\nFailures detected. Refer to the generated Excel/CSV reports for complete details.")
        print("=" * 76)
        sys.exit(1)

    print("=" * 76)
    log_success("All repository settings and multi-ruleset associations applied successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
