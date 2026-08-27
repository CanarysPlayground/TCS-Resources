import subprocess
import importlib
import importlib.util

_REQUIRED_PACKAGES = {
    "requests": "requests>=2.31.0",
    "openpyxl": "openpyxl>=3.1.2",
}

def _ensure_packages() -> None:
    """Install any missing third-party packages via pip at startup."""
    missing = [
        pip_spec
        for import_name, pip_spec in _REQUIRED_PACKAGES.items()
        if importlib.util.find_spec(import_name) is None
    ]
    if not missing:
        return

    print(f"[Bootstrap] Installing missing packages: {', '.join(missing)}")
    result = subprocess.run(
        [subprocess.sys.executable, "-m", "pip", "install", *missing],
        check=False,
    )
    if result.returncode != 0:
        print(
            "[Bootstrap] ERROR: Could not install required packages.\n"
            f"Please run manually:  pip install {' '.join(missing)}",
            file=subprocess.sys.stderr,
        )
        raise SystemExit(1)

    # Invalidate importlib caches so newly installed packages are importable
    importlib.invalidate_caches()
    print("[Bootstrap] Packages installed successfully.\n")

_ensure_packages()
# ---------------------------------------------------------------------------

import csv
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Optional dependency: openpyxl (for Excel output)
# ---------------------------------------------------------------------------
try:
    import openpyxl
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIG_FILE = SCRIPT_DIR / "config.json"
REPOS_FILE = SCRIPT_DIR / "repos.csv"

REQUIRED_CONFIG_KEYS = ["gitlab_url", "private_token"]

# Output column headers
REPORT_HEADERS = [
    "Org / Group Name",
    "Repository Name",
    "Repository Path",
    "Branch Name",
    "File Path",
    "Folder Path",
    "File Name",
    "File Size (MB)",
    "File Size (Bytes)",
    "Last Commit ID",
    "File URL",
    "Scanned At",
]

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_level: str, log_file: str) -> logging.Logger:
    """Configure console + rotating file logging."""
    logger = logging.getLogger("gitlab_scanner")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Rotating file handler (10 MB × 5 backups)
    log_path = SCRIPT_DIR / log_file
    fh = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Config & repos loading
# ---------------------------------------------------------------------------

def load_config(path: Path) -> Dict[str, Any]:
    """Load and validate config.json."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    missing = [k for k in REQUIRED_CONFIG_KEYS if not cfg.get(k)]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")

    if cfg.get("private_token", "").startswith("YOUR_"):
        raise ValueError(
            "Please replace 'YOUR_GITLAB_PRIVATE_TOKEN_HERE' in config.json with a real token."
        )

    # Apply defaults
    cfg.setdefault("size_threshold_mb", 100)
    cfg.setdefault("output_format", "excel")
    cfg.setdefault("output_file", "large_files_report")
    cfg.setdefault("branches_to_scan", [])
    cfg.setdefault("scan_all_branches", False)
    cfg.setdefault("default_branch_only", True)
    cfg.setdefault("request_timeout_seconds", 30)
    cfg.setdefault("max_retries", 3)
    cfg.setdefault("retry_delay_seconds", 2)
    cfg.setdefault("log_level", "INFO")
    cfg.setdefault("log_file", "gitlab_scanner.log")
    cfg.setdefault("concurrent_workers", 4)
    cfg.setdefault("per_page", 100)

    return cfg


def load_repos(path: Path) -> List[Dict[str, str]]:
    """Load repos.csv. Returns list of dicts with 'org_name' and 'repo_path'."""
    if not path.exists():
        raise FileNotFoundError(f"Repos file not found: {path}")

    repos: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("repos.csv is empty or has no headers.")

        headers_lower = [h.strip().lower() for h in reader.fieldnames]
        if "repo_path" not in headers_lower:
            raise ValueError(
                "repos.csv must contain a 'repo_path' column "
                "(GitLab namespace/project, e.g. my-group/my-project)."
            )

        for row_num, row in enumerate(reader, start=2):
            # Normalise keys
            normalised = {k.strip().lower(): (v or "").strip() for k, v in row.items()}
            repo_path = normalised.get("repo_path", "")
            org_name = normalised.get("org_name", repo_path.split("/")[0] if "/" in repo_path else repo_path)

            if not repo_path:
                continue  # skip blank rows

            repos.append(
                {
                    "org_name": org_name,
                    "repo_path": repo_path,
                }
            )

    return repos


# ---------------------------------------------------------------------------
# GitLab API client
# ---------------------------------------------------------------------------

class GitLabClient:
    """Thin wrapper around the GitLab REST API v4."""

    def __init__(self, cfg: Dict[str, Any], logger: logging.Logger):
        self.base_url = cfg["gitlab_url"].rstrip("/") + "/api/v4"
        self.session = requests.Session()
        self.session.headers.update({"PRIVATE-TOKEN": cfg["private_token"]})
        self.timeout = cfg["request_timeout_seconds"]
        self.max_retries = cfg["max_retries"]
        self.retry_delay = cfg["retry_delay_seconds"]
        self.per_page = cfg["per_page"]
        self.logger = logger

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """GET with retry logic. Returns parsed JSON."""
        url = f"{self.base_url}{endpoint}"
        params = params or {}

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", self.retry_delay * attempt))
                    self.logger.warning("Rate limited. Waiting %ds before retry …", retry_after)
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.RequestException as exc:
                self.logger.warning(
                    "Attempt %d/%d failed for %s: %s", attempt, self.max_retries, url, exc
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                else:
                    raise

    def _get_paginated(self, endpoint: str, params: Optional[Dict] = None) -> List[Any]:
        """Follow GitLab pagination and return all items."""
        params = dict(params or {})
        params["per_page"] = self.per_page
        page = 1
        items: List[Any] = []

        while True:
            params["page"] = page
            url = f"{self.base_url}{endpoint}"

            for attempt in range(1, self.max_retries + 1):
                try:
                    resp = self.session.get(url, params=params, timeout=self.timeout)

                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", self.retry_delay * attempt))
                        self.logger.warning("Rate limited. Waiting %ds …", retry_after)
                        time.sleep(retry_after)
                        continue

                    resp.raise_for_status()
                    batch = resp.json()
                    items.extend(batch)

                    total_pages = int(resp.headers.get("X-Total-Pages", 1))
                    if page >= total_pages or not batch:
                        return items
                    page += 1
                    break  # success → next page

                except requests.exceptions.RequestException as exc:
                    self.logger.warning(
                        "Attempt %d/%d failed for %s: %s", attempt, self.max_retries, url, exc
                    )
                    if attempt < self.max_retries:
                        time.sleep(self.retry_delay * attempt)
                    else:
                        raise

        return items  # unreachable but satisfies type checkers

    # ------------------------------------------------------------------
    # Domain helpers
    # ------------------------------------------------------------------

    def get_project(self, repo_path: str) -> Optional[Dict]:
        """Fetch project metadata. Returns None if not found."""
        encoded = requests.utils.quote(repo_path, safe="")
        try:
            return self._get(f"/projects/{encoded}")
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def get_branches(self, project_id: int) -> List[str]:
        """Return list of branch names for a project."""
        branches = self._get_paginated(f"/projects/{project_id}/repository/branches")
        return [b["name"] for b in branches]

    def get_tree(self, project_id: int, branch: str, path: str = "", recursive: bool = True) -> List[Dict]:
        """Return repository tree items (files + dirs)."""
        return self._get_paginated(
            f"/projects/{project_id}/repository/tree",
            params={"ref": branch, "path": path, "recursive": recursive},
        )

    def get_blobs(self, project_id: int, branch: str) -> List[Dict]:
        """Return only blob (file) entries from the full recursive tree."""
        tree = self.get_tree(project_id, branch, recursive=True)
        return [item for item in tree if item.get("type") == "blob"]

    def get_file_metadata(self, project_id: int, file_path: str, branch: str) -> Optional[Dict]:
        """
        Fetch file metadata including size.
        Uses the repository/files endpoint which returns size without downloading content.
        """
        encoded_path = requests.utils.quote(file_path, safe="")
        try:
            return self._get(
                f"/projects/{project_id}/repository/files/{encoded_path}",
                params={"ref": branch},
            )
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                self.logger.debug("File not found: %s @ %s", file_path, branch)
                return None
            raise


# ---------------------------------------------------------------------------
# Scanner logic
# ---------------------------------------------------------------------------

def determine_branches(
    client: GitLabClient,
    project_id: int,
    default_branch: str,
    cfg: Dict[str, Any],
    logger: logging.Logger,
) -> List[str]:
    """Determine which branches to scan based on config."""
    if cfg.get("scan_all_branches"):
        branches = client.get_branches(project_id)
        logger.info("  Found %d branch(es) to scan (all branches mode).", len(branches))
        return branches

    if cfg.get("branches_to_scan"):
        return list(cfg["branches_to_scan"])

    # Default: only the project's default branch
    return [default_branch]


def scan_branch(
    client: GitLabClient,
    project_info: Dict,
    org_name: str,
    repo_path: str,
    branch: str,
    threshold_bytes: int,
    scanned_at: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Scan a single branch and return rows for large files."""
    project_id = project_info["id"]
    repo_name = project_info["name"]
    web_url = project_info.get("web_url", "")

    logger.info("    Scanning branch: %s", branch)

    try:
        blobs = client.get_blobs(project_id, branch)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("    Failed to fetch tree for branch '%s': %s", branch, exc)
        return []

    logger.info("    Found %d file(s) in tree. Checking sizes …", len(blobs))

    large_files: List[Dict[str, Any]] = []
    checked = 0

    for blob in blobs:
        file_path: str = blob.get("path", "")
        if not file_path:
            continue

        try:
            meta = client.get_file_metadata(project_id, file_path, branch)
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug("    Skipping %s: %s", file_path, exc)
            continue

        if meta is None:
            continue

        size_bytes: int = meta.get("size", 0)
        
        # Check if it might be a Git LFS pointer
        if size_bytes < 1024 and meta.get("encoding") == "base64":
            import base64
            try:
                content = base64.b64decode(meta.get("content", "")).decode("utf-8")
                if content.startswith("version https://git-lfs.github.com/spec/"):
                    for line in content.splitlines():
                        if line.startswith("size "):
                            size_bytes = int(line.split(" ")[1])
                            break
            except Exception as exc:
                logger.debug("    Failed to parse potential LFS pointer %s: %s", file_path, exc)

        checked += 1

        if size_bytes >= threshold_bytes:
            file_name = Path(file_path).name
            folder_path = str(Path(file_path).parent)
            if folder_path == ".":
                folder_path = "(root)"

            size_mb = round(size_bytes / (1024 * 1024), 3)
            last_commit = meta.get("last_commit_id", "")
            file_url = f"{web_url}/-/blob/{branch}/{file_path}"

            row = {
                "Org / Group Name": org_name,
                "Repository Name": repo_name,
                "Repository Path": repo_path,
                "Branch Name": branch,
                "File Path": file_path,
                "Folder Path": folder_path,
                "File Name": file_name,
                "File Size (MB)": size_mb,
                "File Size (Bytes)": size_bytes,
                "Last Commit ID": last_commit,
                "File URL": file_url,
                "Scanned At": scanned_at,
            }
            large_files.append(row)
            logger.info(
                "    * Large file: %s  (%.2f MB)",
                file_path,
                size_mb,
            )

    logger.info(
        "    Branch '%s' — checked %d file(s), %d large file(s) found.",
        branch,
        checked,
        len(large_files),
    )
    return large_files


def scan_repo(
    client: GitLabClient,
    org_name: str,
    repo_path: str,
    cfg: Dict[str, Any],
    scanned_at: str,
    logger: logging.Logger,
) -> Tuple[str, List[Dict[str, Any]]]:
    """Scan all configured branches of a single repo. Returns (repo_path, rows)."""
    threshold_bytes = int(cfg["size_threshold_mb"] * 1024 * 1024)
    all_rows: List[Dict[str, Any]] = []

    logger.info("Scanning repo: %s", repo_path)

    project = client.get_project(repo_path)
    if project is None:
        logger.warning("  Repo not found or no access: %s. Skipping.", repo_path)
        return repo_path, []

    default_branch: str = project.get("default_branch") or "main"
    branches = determine_branches(client, project["id"], default_branch, cfg, logger)

    for branch in branches:
        rows = scan_branch(
            client, project, org_name, repo_path, branch, threshold_bytes, scanned_at, logger
        )
        all_rows.extend(rows)

    logger.info(
        "Finished repo: %s — total large files found: %d", repo_path, len(all_rows)
    )
    return repo_path, all_rows


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def write_csv(rows: List[Dict[str, Any]], output_path: Path, logger: logging.Logger) -> None:
    """Write results to a CSV file."""
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV report written: %s", output_path)


def write_excel(rows: List[Dict[str, Any]], output_path: Path, logger: logging.Logger) -> None:
    """Write results to a styled Excel (.xlsx) file."""
    if not OPENPYXL_AVAILABLE:
        logger.warning("openpyxl not installed. Falling back to CSV output.")
        csv_path = output_path.with_suffix(".csv")
        write_csv(rows, csv_path, logger)
        return

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Large Files Report"

    # -- Header style
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", name="Calibri", size=11)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=False)
    left_align = Alignment(horizontal="left", vertical="center", wrap_text=False)

    for col_idx, header in enumerate(REPORT_HEADERS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center_align

    ws.row_dimensions[1].height = 22

    # -- Data rows
    even_fill = PatternFill("solid", fgColor="D6E4F0")

    for row_idx, data in enumerate(rows, start=2):
        fill = even_fill if row_idx % 2 == 0 else PatternFill("solid", fgColor="FFFFFF")
        for col_idx, key in enumerate(REPORT_HEADERS, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=data.get(key, ""))
            cell.fill = fill
            cell.font = Font(name="Calibri", size=10)
            cell.alignment = left_align

    # -- Hyperlink the "File URL" column
    url_col = REPORT_HEADERS.index("File URL") + 1
    for row_idx in range(2, len(rows) + 2):
        cell = ws.cell(row=row_idx, column=url_col)
        url_val = cell.value
        if url_val:
            cell.hyperlink = url_val
            cell.font = Font(name="Calibri", size=10, color="0563C1", underline="single")

    # -- Auto-fit column widths (rough heuristic)
    for col_idx, header in enumerate(REPORT_HEADERS, start=1):
        max_len = len(header)
        for row_idx in range(2, len(rows) + 2):
            val = ws.cell(row=row_idx, column=col_idx).value
            if val:
                max_len = max(max_len, min(len(str(val)), 60))
        ws.column_dimensions[get_column_letter(col_idx)].width = max_len + 4

    # -- Freeze header row
    ws.freeze_panes = "A2"

    # -- Auto-filter
    ws.auto_filter.ref = ws.dimensions

    # -- Summary sheet
    ws_summary = wb.create_sheet(title="Summary")
    summary_data = [
        ("Report Generated At", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("Total Large Files Found", len(rows)),
        ("Size Threshold", f"{rows[0].get('File Size (MB)', 'N/A')} MB threshold" if rows else "N/A"),
        ("Unique Repositories Scanned", len({r["Repository Path"] for r in rows})),
        ("Unique Branches Scanned", len({(r["Repository Path"], r["Branch Name"]) for r in rows})),
    ]
    for r_idx, (label, value) in enumerate(summary_data, start=1):
        ws_summary.cell(row=r_idx, column=1, value=label).font = Font(bold=True, name="Calibri")
        ws_summary.cell(row=r_idx, column=2, value=value).font = Font(name="Calibri")
    ws_summary.column_dimensions["A"].width = 35
    ws_summary.column_dimensions["B"].width = 40

    wb.save(output_path)
    logger.info("Excel report written: %s", output_path)


def write_report(rows: List[Dict[str, Any]], cfg: Dict[str, Any], logger: logging.Logger) -> None:
    """Dispatch to the correct writer based on config."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{cfg['output_file']}_{timestamp}"
    output_format = cfg["output_format"].lower()

    if output_format in ("excel", "xlsx"):
        output_path = SCRIPT_DIR / f"{base_name}.xlsx"
        write_excel(rows, output_path, logger)
    else:
        output_path = SCRIPT_DIR / f"{base_name}.csv"
        write_csv(rows, output_path, logger)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # -- Load config (pre-logger so errors go to stderr)
    try:
        cfg = load_config(CONFIG_FILE)
    except (FileNotFoundError, ValueError) as exc:
        sys.exit(f"[ERROR] {exc}")

    logger = setup_logging(cfg["log_level"], cfg["log_file"])

    logger.info("=" * 70)
    logger.info("GitLab Large File Inventory Script")
    logger.info("=" * 70)
    logger.info("GitLab URL     : %s", cfg["gitlab_url"])
    logger.info("Size Threshold : %s MB", cfg["size_threshold_mb"])
    logger.info("Output Format  : %s", cfg["output_format"])
    logger.info("Config file    : %s", CONFIG_FILE)
    logger.info("Repos file     : %s", REPOS_FILE)
    logger.info("=" * 70)

    # -- Load repos
    try:
        repos = load_repos(REPOS_FILE)
    except (FileNotFoundError, ValueError) as exc:
        logger.error(exc)
        sys.exit(1)

    if not repos:
        logger.error("No valid repos found in %s. Exiting.", REPOS_FILE)
        sys.exit(1)

    logger.info("Loaded %d repo(s) from repos.csv.", len(repos))

    # -- Initialize GitLab client
    client = GitLabClient(cfg, logger)

    # -- Verify connectivity
    try:
        user_info = client._get("/user")
        logger.info("Authenticated as: %s (%s)", user_info.get("name"), user_info.get("username"))
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed to authenticate with GitLab: %s", exc)
        sys.exit(1)

    scanned_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    all_large_files: List[Dict[str, Any]] = []
    workers = min(cfg["concurrent_workers"], len(repos))

    # -- Parallel repo scanning
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                scan_repo, client, r["org_name"], r["repo_path"], cfg, scanned_at, logger
            ): r
            for r in repos
        }

        for future in as_completed(futures):
            repo = futures[future]
            try:
                _, rows = future.result()
                all_large_files.extend(rows)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Unexpected error scanning '%s': %s", repo["repo_path"], exc, exc_info=True
                )

    # -- Write output
    logger.info("=" * 70)
    logger.info("Scan complete. Total large files found: %d", len(all_large_files))

    if all_large_files:
        # Sort: org → repo → branch → file path
        all_large_files.sort(
            key=lambda r: (
                r["Org / Group Name"],
                r["Repository Path"],
                r["Branch Name"],
                r["File Path"],
            )
        )
        write_report(all_large_files, cfg, logger)
    else:
        logger.info("No files exceeded the %s MB threshold. No report generated.", cfg["size_threshold_mb"])

    logger.info("Done.")


if __name__ == "__main__":
    main()
