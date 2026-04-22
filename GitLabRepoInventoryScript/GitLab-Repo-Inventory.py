# GitLab Repository Inventory â€” fetches all projects and writes a detailed CSV.
# Config: config.json (same directory) | Dependencies: pip install requests colorama

import csv
import json
import logging
import sys
import threading
import traceback
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from colorama import Fore, Style, init as colorama_init
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SCRIPT_DIR = Path(__file__).resolve().parent
TIMEOUT    = 30

colorama_init(autoreset=True)
_C_INFO  = Fore.CYAN
_C_OK    = Fore.GREEN
_C_WARN  = Fore.YELLOW
_C_BOLD  = Style.BRIGHT
_C_RESET = Style.RESET_ALL

_print_lock = threading.Lock()
_log        = logging.getLogger("gitlab_inventory")


def _cprint(msg: str, color: str = "") -> None:
    with _print_lock:
        print(f"{color}{msg}" if color else msg)


def setup_logging() -> None:
    ts       = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = SCRIPT_DIR / f"gitlab_inventory_{ts}.log"
    _log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                      datefmt="%Y-%m-%dT%H:%M:%SZ"))
    _log.addHandler(fh)
    _cprint(f"Log file : {log_path}", Style.DIM)


def load_config() -> dict:
    config_path = SCRIPT_DIR / "config.json"
    if not config_path.exists():
        sys.exit(f"[ERROR] config.json not found at: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        config = json.load(f)
    gl    = config.get("gitlab", {})
    url   = gl.get("url", "").strip().rstrip("/")
    token = gl.get("token", "").strip()
    errors = []
    if not url:
        errors.append("  - gitlab.url is missing or empty")
    if not token or token.startswith("YOUR_"):
        errors.append(
            "  - gitlab.token is missing or placeholder.\n"
            "    Create a PAT at: <gitlab-url>/-/user_settings/personal_access_tokens\n"
            "    Required scopes: read_api, read_registry"
        )
    if errors:
        sys.exit("[ERROR] config.json is incomplete:\n" + "\n".join(errors))
    return config


def build_session(token: str, verify_ssl: bool) -> requests.Session:
    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    adapter = HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    ))
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Private-Token": token})
    session.verify = verify_ssl
    return session


_thread_local = threading.local()


def _thread_session(token: str, verify_ssl: bool) -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = build_session(token, verify_ssl)
    return _thread_local.session


def paginate(session: requests.Session, url: str, params: dict) -> list:
    results, page = [], 1
    while True:
        resp = session.get(url, params={**params, "page": page}, timeout=TIMEOUT)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        results.extend(batch)
        page += 1
    return results


def _x_total(session: requests.Session, url: str, params: dict = None) -> int:
    """Single HEAD-like request using X-Total header. Returns 0 on 403/404."""
    resp = session.get(url, params={"per_page": 1, "page": 1, **(params or {})}, timeout=TIMEOUT)
    if resp.status_code in (403, 404):
        _log.warning("[SKIP] %s â€” HTTP %s", url, resp.status_code)
        return 0
    resp.raise_for_status()
    return int(resp.headers.get("X-Total", 0))


def get_projects(session: requests.Session, base_url: str, per_page: int, membership_only: bool) -> list:
    return paginate(session, f"{base_url}/api/v4/projects", {
        "membership": str(membership_only).lower(),
        "per_page": per_page,
        "statistics": "true",
    })


def get_commit_info(session: requests.Session, base_url: str, pid: int) -> tuple:
    """Returns (total_count, latest_date) in a single request."""
    url  = f"{base_url}/api/v4/projects/{pid}/repository/commits"
    resp = session.get(url, params={"per_page": 1, "page": 1}, timeout=TIMEOUT)
    if resp.status_code in (403, 404):
        _log.warning("[SKIP] commits %s â€” HTTP %s", pid, resp.status_code)
        return 0, None
    resp.raise_for_status()
    data = resp.json()
    return int(resp.headers.get("X-Total", 0)), (data[0]["committed_date"] if data else None)


def get_mr_stats(session: requests.Session, base_url: str, pid: int) -> tuple:
    """Returns (total_count, latest_created_at) in a single request."""
    url  = f"{base_url}/api/v4/projects/{pid}/merge_requests"
    resp = session.get(url, params={"state": "all", "order_by": "created_at",
                                    "sort": "desc", "per_page": 1, "page": 1}, timeout=TIMEOUT)
    if resp.status_code in (403, 404):
        _log.warning("[SKIP] MRs %s â€” HTTP %s", pid, resp.status_code)
        return 0, None
    resp.raise_for_status()
    data = resp.json()
    return int(resp.headers.get("X-Total", 0)), (data[0]["created_at"] if data else None)


def get_issue_counts(session: requests.Session, base_url: str, pid: int) -> tuple:
    url = f"{base_url}/api/v4/projects/{pid}/issues"
    return (_x_total(session, url, {"state": "opened"}),
            _x_total(session, url, {"state": "closed"}))


def _get_protected_branches(session: requests.Session, base_url: str, pid: int, per_page: int) -> list:
    try:
        return paginate(session, f"{base_url}/api/v4/projects/{pid}/protected_branches",
                        {"per_page": per_page})
    except requests.exceptions.HTTPError as e:
        _log.warning("[SKIP] protected_branches %s â€” %s", pid, e)
        return []


def _get_members(session: requests.Session, base_url: str, pid: int, per_page: int) -> list:
    try:
        return paginate(session, f"{base_url}/api/v4/projects/{pid}/members/all",
                        {"per_page": per_page})
    except requests.exceptions.HTTPError as e:
        _log.warning("[SKIP] members %s â€” %s", pid, e)
        return []


def _branch_summary(branches: list) -> dict:
    return {
        "protected_branch_count":       len(branches),
        "protected_branch_names":       "; ".join(b["name"] for b in branches),
        "force_push_blocked":           "; ".join(b["name"] for b in branches
                                                  if not b.get("allow_force_push", False)),
        "code_owner_approval_required": "; ".join(b["name"] for b in branches
                                                  if b.get("code_owner_approval_required", False)),
    }


def _member_counts(members: list) -> dict:
    return {
        "read_count":  sum(1 for m in members if m["access_level"] <= 20),
        "write_count": sum(1 for m in members if m["access_level"] == 30),
        "admin_count": sum(1 for m in members if m["access_level"] >= 40),
        "total_count": len(members),
    }


def collect_project_data(session: requests.Session, base_url: str, project: dict, per_page: int) -> dict:
    pid          = project["id"]
    stats        = project.get("statistics") or {}
    repo_size_mb = round((stats.get("repository_size") or 0) / (1024 * 1024), 2)
    lfs_size_mb  = round((stats.get("lfs_objects_size") or 0) / (1024 * 1024), 2)
    ns_kind      = project["namespace"]["kind"]
    fork_source  = (project["forked_from_project"]["path_with_namespace"]
                    if project.get("forked_from_project") else None)
    topics       = "; ".join(project.get("topics") or project.get("tag_list") or [])
    ci_config    = project.get("ci_config_path") or ".gitlab-ci.yml"

    total_commits, latest_commit_date = get_commit_info(session, base_url, pid)
    total_mr_count, latest_mr_date    = get_mr_stats(session, base_url, pid)

    issues_enabled             = project.get("issues_enabled", False)
    open_issues, closed_issues = get_issue_counts(session, base_url, pid) if issues_enabled else (0, 0)

    p = f"{base_url}/api/v4/projects/{pid}"
    label_count      = _x_total(session, f"{p}/labels")
    milestone_count  = _x_total(session, f"{p}/milestones")
    tag_count        = _x_total(session, f"{p}/repository/tags")
    pipeline_count   = _x_total(session, f"{p}/pipelines")
    ci_var_count     = _x_total(session, f"{p}/variables")
    webhook_count    = _x_total(session, f"{p}/hooks")
    deploy_key_count = _x_total(session, f"{p}/deploy_keys")
    snippet_count    = _x_total(session, f"{p}/snippets")
    registry_count   = (_x_total(session, f"{p}/registry/repositories")
                        if project.get("container_registry_enabled") else 0)

    branches = _get_protected_branches(session, base_url, pid, per_page)
    members  = _get_members(session, base_url, pid, per_page)

    return {
        "id": pid, "name": project["name"], "full_path": project["path_with_namespace"],
        "default_branch": project.get("default_branch"), "visibility": project.get("visibility"),
        "archived": project.get("archived", False), "namespace_type": ns_kind,
        "ownership": "User Owned" if ns_kind == "user" else "Group Owned",
        "repo_size_mb": repo_size_mb, "lfs_enabled": project.get("lfs_enabled", False),
        "lfs_size_mb": lfs_size_mb, "clone_url_http": project["http_url_to_repo"],
        "clone_url_ssh": project.get("ssh_url_to_repo"), "topics": topics, "fork_source": fork_source,
        "latest_commit_date": latest_commit_date, "total_commit_count": total_commits,
        "latest_mr_date": latest_mr_date, "total_mr_count": total_mr_count,
        "issues_enabled": issues_enabled, "open_issues": open_issues,
        "closed_issues": closed_issues, "total_issues": open_issues + closed_issues,
        "label_count": label_count, "milestone_count": milestone_count, "tag_count": tag_count,
        "ci_config_path": ci_config, "pipeline_count": pipeline_count, "ci_var_count": ci_var_count,
        "webhook_count": webhook_count, "deploy_key_count": deploy_key_count,
        "snippet_count": snippet_count,
        "container_registry_enabled": project.get("container_registry_enabled", False),
        "registry_repo_count": registry_count,
        "packages_enabled": project.get("packages_enabled", False),
        **_branch_summary(branches),
        **_member_counts(members),
        "wiki_enabled": project.get("wiki_enabled", False),
    }


CSV_HEADERS = [
    "Project ID", "Project Name", "Full Path", "Default Branch",
    "Visibility", "Archived", "Namespace Type", "Ownership",
    "Repo Size (MB)", "LFS Enabled", "LFS Size (MB)",
    "Clone URL (HTTP)", "Clone URL (SSH)", "Topics", "Fork Source",
    "Latest Commit Date", "Total Commit Count (All Time)",
    "Latest MR Date", "Total MR Count (All Time)",
    "Issues Enabled", "Open Issues", "Closed Issues", "Total Issues",
    "Label Count", "Milestone Count", "Tag Count",
    "CI Config Path", "Pipeline Count", "CI Variable Count",
    "Webhook Count", "Deploy Key Count", "Snippet Count",
    "Container Registry Enabled", "Registry Repo Count", "Packages Enabled",
    "Protected Branch Count", "Protected Branch Names",
    "Force Push Blocked Branches", "Code Owner Approval Required Branches",
    "Read User Count", "Write User Count", "Admin User Count", "Total Member Count",
    "Wiki Enabled",
]


def write_csv(file_path: str, inventory: list) -> None:
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        for p in inventory:
            writer.writerow([
                p["id"], p["name"], p["full_path"], p["default_branch"],
                p["visibility"], p["archived"], p["namespace_type"], p["ownership"],
                p["repo_size_mb"], p["lfs_enabled"], p["lfs_size_mb"],
                p["clone_url_http"], p["clone_url_ssh"], p["topics"], p["fork_source"],
                p["latest_commit_date"], p["total_commit_count"],
                p["latest_mr_date"], p["total_mr_count"],
                p["issues_enabled"], p["open_issues"], p["closed_issues"], p["total_issues"],
                p["label_count"], p["milestone_count"], p["tag_count"],
                p["ci_config_path"], p["pipeline_count"], p["ci_var_count"],
                p["webhook_count"], p["deploy_key_count"], p["snippet_count"],
                p["container_registry_enabled"], p["registry_repo_count"], p["packages_enabled"],
                p["protected_branch_count"], p["protected_branch_names"],
                p["force_push_blocked"], p["code_owner_approval_required"],
                p["read_count"], p["write_count"], p["admin_count"], p["total_count"],
                p["wiki_enabled"],
            ])


def main() -> None:
    config      = load_config()
    gl          = config["gitlab"]
    base_url    = gl["url"].rstrip("/")
    token       = gl["token"]
    per_page    = gl.get("per_page", 100)
    verify_ssl  = gl.get("verify_ssl", True)
    csv_file    = str(SCRIPT_DIR / config.get("output", {}).get("csv_file", "gitlab_repo_inventory.csv"))
    membership  = config.get("filters", {}).get("membership_only", True)
    max_workers = config.get("performance", {}).get("max_workers", 10)

    setup_logging()
    session = build_session(token, verify_ssl)

    _cprint("Verifying token...", _C_INFO)
    try:
        resp = session.get(f"{base_url}/api/v4/user", timeout=TIMEOUT)
        if resp.status_code == 401:
            sys.exit(
                "[ERROR] 401 Unauthorized: token is invalid or expired.\n"
                f"  Generate a PAT at: {base_url}/-/user_settings/personal_access_tokens"
            )
        if resp.status_code == 403:
            sys.exit(
                "[ERROR] 403 Forbidden: token lacks read_api scope.\n"
                "  Required scopes: read_api, read_registry\n"
                f"  Generate a PAT at: {base_url}/-/user_settings/personal_access_tokens"
            )
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        sys.exit(f"[ERROR] Cannot connect to {base_url}. Check gitlab.url in config.json.")
    _cprint("Token OK.\n", _C_OK)
    _log.info("Token verified. Fetching project list (membership_only=%s).", membership)

    _cprint("Fetching project list...", _C_INFO)
    projects = get_projects(session, base_url, per_page, membership)
    total    = len(projects)
    _log.info("Found %d project(s).", total)
    _cprint(f"Found {_C_BOLD}{total}{_C_RESET}{_C_INFO} project(s).\n", _C_INFO)

    inventory: list = []
    failed:    list = []
    done  = 0
    width = len(str(total))

    def _worker(project: dict) -> dict:
        nonlocal done
        data = collect_project_data(_thread_session(token, verify_ssl), base_url, project, per_page)
        with _print_lock:
            done += 1
            print(f"{_C_OK}  [{done:>{width}}/{total}]{_C_RESET} {project['path_with_namespace']}")
        return data

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_worker, p): p for p in projects}
        for future in as_completed(futures):
            proj = futures[future]
            name = proj["path_with_namespace"]
            try:
                inventory.append(future.result())
            except Exception as e:
                status = getattr(getattr(e, "response", None), "status_code", "?")
                _cprint(f"  [WARN] {name}: HTTP {status} â€” {e}", _C_WARN)
                _log.warning("FAILED %s: %s\n%s", name, e, traceback.format_exc())
                failed.append(name)

    write_csv(csv_file, inventory)
    _log.info("CSV written: %s | succeeded=%d failed=%d", csv_file, len(inventory), len(failed))
    _cprint(f"\nInventory written to: {csv_file}", _C_OK)
    _cprint(f"  Succeeded : {len(inventory)}", _C_OK)
    if failed:
        _cprint(f"  Failed    : {len(failed)}", _C_WARN)
        for name in failed:
            _cprint(f"    - {name}", _C_WARN)
    _log.info("Run complete.")


if __name__ == "__main__":
    main()

