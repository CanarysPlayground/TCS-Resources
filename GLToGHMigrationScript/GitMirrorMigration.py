#!/usr/bin/env python3
"""
GitLab -> GitHub Mirror Migration Script
"""
from __future__ import annotations
import abc
import calendar
import csv
import datetime
import json
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

try:
    __import__("openpyxl")  # availability probe; used lazily in _write_xlsx
    _XLSX_AVAILABLE = True
except ImportError:
    _XLSX_AVAILABLE = False

# ---------------------------------------------------------------------------
# Python version guard
# ---------------------------------------------------------------------------
if sys.version_info < (3, 8):
    sys.exit(
        f"Python 3.8 or later is required (you have {sys.version.split()[0]}). "
        "Please upgrade: https://www.python.org/downloads/"
    )


# ---------------------------------------------------------------------------
# Sentinel for user config errors (shown as WARNING, not ERROR)
# ---------------------------------------------------------------------------
class _MissingConfigError(ValueError):
    """Raised when required user configuration is missing (not a code bug)."""


# ---------------------------------------------------------------------------
# Script-relative paths -- resolved from the script's own directory so the
# script works correctly regardless of the current working directory.
# ---------------------------------------------------------------------------
_SCRIPT_DIR  = Path(__file__).resolve().parent
_RUN_TS      = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
_LOGS_DIR    = _SCRIPT_DIR / "logs"
_REPORTS_DIR = _SCRIPT_DIR / "reports"
_LOGS_DIR.mkdir(exist_ok=True)
_REPORTS_DIR.mkdir(exist_ok=True)
_LOG_FILE        = _LOGS_DIR    / f"mirror-migration-{_RUN_TS}.log"
_RESULTS_JSON    = _REPORTS_DIR / f"mirror-migration-{_RUN_TS}.json"
_RESULTS_CSV     = _REPORTS_DIR / f"mirror-migration-{_RUN_TS}.csv"
_RESULTS_XLSX    = _REPORTS_DIR / f"mirror-migration-{_RUN_TS}.xlsx"
_CHECKPOINT_FILE = _SCRIPT_DIR  / "mirror-migration-checkpoint.json"
_CONFIG_FILENAME = "mirror-config.json"
_REPOS_CSV_FILENAME = "repos.csv"

# ---------------------------------------------------------------------------
# Internal constants
# ---------------------------------------------------------------------------
_GITHUB_API_URL = "https://api.github.com"
_GITHUB_URL = "https://github.com"

# Pause API calls when remaining quota falls below this threshold.
_RATE_LIMIT_BUFFER = 100

# Maximum repository rows displayed in the pre-flight preview table.
# Repos beyond this limit are summarised as "... and N more".
_PREVIEW_MAX_ROWS = 25

# Default migration settings (all overridable in mirror-config.json)
_DEFAULT_MAX_WORKERS = 5
_DEFAULT_CLONE_TIMEOUT = 7200   # seconds
_DEFAULT_PUSH_TIMEOUT = 7200    # seconds
_DEFAULT_GIT_RETRIES = 3
_DEFAULT_API_RETRIES = 5
_DEFAULT_MIN_FREE_DISK_GB = 10.0  # minimum free disk space in system temp dir (GB)


_PLACEHOLDER_TOKENS = (
    "YOUR_", "xxxx", "glpat-xxxx", "ghp_xxxx",
    "fileserver.internal", "example.com",
)
_VALID_VISIBILITIES = frozenset({"private", "internal", "public"})
_VIS_ICONS = {"private": "\U0001f512", "internal": "\U0001f465", "public": "\U0001f310"}


def _fmt_ts() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# Colored console logging
# ---------------------------------------------------------------------------
class _ColorFormatter(logging.Formatter):
    """ANSI-colored formatter for interactive terminal sessions only."""

    _RESET = "\x1b[0m"
    _BOLD  = "\x1b[1m"
    _DIM   = "\x1b[2m"
    _LEVEL_COLORS = {
        logging.DEBUG:    "\x1b[36m",   # Cyan
        logging.INFO:     "\x1b[32m",   # Green
        logging.WARNING:  "\x1b[33m",   # Yellow
        logging.ERROR:    "\x1b[31m",   # Red
        logging.CRITICAL: "\x1b[35m",   # Magenta
    }
    _FMT = (
        "{dim}%(asctime)s{reset} "
        "{bold}{color}[%(levelname)s]{reset} "
        "{color}%(message)s{reset}"
    )

    def format(self, record: logging.LogRecord) -> str:
        color = self._LEVEL_COLORS.get(record.levelno, "")
        fmt = self._FMT.format(
            color=color, bold=self._BOLD, dim=self._DIM, reset=self._RESET
        )
        return logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S").format(record)


def _enable_windows_ansi() -> None:
    """Enable ANSI virtual terminal processing on Windows 10+. No-op elsewhere."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        import ctypes.wintypes
        kernel = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        if handle == -1:
            return
        mode = ctypes.wintypes.DWORD()
        if not kernel.GetConsoleMode(handle, ctypes.byref(mode)):
            return
        kernel.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    except Exception:
        pass


def _reconfigure_stdout_utf8() -> None:
    """Force UTF-8 on stdout/stderr (avoids cp1252 UnicodeEncodeError on Windows)."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def _build_logger() -> logging.Logger:
    """Build a colored console + plain UTF-8 file logger."""
    _reconfigure_stdout_utf8()
    _enable_windows_ansi()

    use_color = sys.stdout.isatty()
    console = logging.StreamHandler()
    console.setFormatter(
        _ColorFormatter()
        if use_color
        else logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
    )

    file_handler = logging.FileHandler(str(_LOG_FILE), encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console)
    root.addHandler(file_handler)
    return logging.getLogger(__name__)


log = _build_logger()


# ===========================================================================
# Authentication providers
# ===========================================================================

class _Auth(abc.ABC):
    """Abstract base for GitHub authentication."""

    @abc.abstractmethod
    def get_auth_header(self) -> str:
        """Return the value for the HTTP Authorization header."""

    @abc.abstractmethod
    def get_token_for_git(self) -> str:
        """Return a token suitable for embedding in a git HTTPS URL."""

    @property
    @abc.abstractmethod
    def mode_label(self) -> str:
        """Short label used in log messages."""


class PATAuth(_Auth):
    """Personal Access Token authentication (simple, stateless)."""

    def __init__(self, pat: str) -> None:
        self._pat = pat

    def get_auth_header(self) -> str:
        return f"Bearer {self._pat}"

    def get_token_for_git(self) -> str:
        return self._pat

    @property
    def mode_label(self) -> str:
        return "PAT"


class GitHubAppAuth(_Auth):

    _REFRESH_BUFFER_SECONDS = 300   # refresh 5 min before expiry
    _JWT_EXPIRY_SECONDS = 540       # 9 min (GitHub max is 10 min)

    def __init__(
        self,
        app_id: int,
        private_key_pem: str,
        installation_id: int,
        api_base: str,
    ) -> None:
        self._app_id = app_id
        self._private_key_pem = private_key_pem
        self._installation_id = installation_id
        self._api_base = api_base.rstrip("/")
        self._token: str = ""
        self._token_expires_at: float = 0.0
        self._lock = threading.Lock()
        self._check_dependencies()

    @staticmethod
    def _check_dependencies() -> None:
        try:
            __import__("jwt")
        except ImportError:
            raise RuntimeError(
                "GitHub App auth requires PyJWT and cryptography.\n"
                "Install with:  pip install PyJWT cryptography\n"
                "Then re-run the script."
            )

    def _generate_jwt(self) -> str:
        import jwt as pyjwt
        now = int(time.time())
        payload = {
            "iat": now - 60,                            # allow 60 s clock skew
            "exp": now + self._JWT_EXPIRY_SECONDS,
            "iss": str(self._app_id),
        }
        return pyjwt.encode(payload, self._private_key_pem, algorithm="RS256")

    def _refresh(self) -> None:

        jwt_token = self._generate_jwt()
        url = f"{self._api_base}/app/installations/{self._installation_id}/access_tokens"
        req = urllib.request.Request(
            url,
            data=b"{}",
            method="POST",
            headers={
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            # Parse the body to give targeted, actionable guidance.
            try:
                err_msg = json.loads(body).get("message", "")
            except Exception:
                err_msg = body

            if exc.code == 401 and "must generate a public key" in err_msg:
                raise RuntimeError(
                    f"GitHub App authentication failed (HTTP 401): {err_msg}\n"
                    "  The App exists on GitHub but has no private key registered.\n"
                    "  The .pem file must be generated by GitHub, not created manually.\n\n"
                    "  Fix (3 steps):\n"
                    "    1. Open: GitHub → Settings → Developer settings → GitHub Apps → your app\n"
                    "    2. Scroll to ‘Private keys’ → click ‘Generate a private key’\n"
                    "    3. A real .pem file will download — replace the current file with it,\n"
                    "       then set auth.app.private_key_path in mirror-config.json accordingly."
                ) from exc

            if exc.code == 401:
                raise RuntimeError(
                    f"GitHub App authentication failed (HTTP 401): {err_msg}\n"
                    "  Verify auth.app.app_id and auth.app.installation_id in mirror-config.json."
                ) from exc

            raise RuntimeError(
                f"Failed to obtain GitHub App installation token (HTTP {exc.code}): {err_msg}"
            ) from exc

        self._token = data["token"]

        # expires_at: "2023-01-01T00:00:00Z"  -- strip Z for Python <3.11 compat
        expires_str = data["expires_at"].rstrip("Z")
        expires_dt = datetime.datetime.strptime(expires_str, "%Y-%m-%dT%H:%M:%S")
        # calendar.timegm interprets naive struct_time as UTC
        self._token_expires_at = (
            calendar.timegm(expires_dt.timetuple()) - self._REFRESH_BUFFER_SECONDS
        )
        log.info("GitHub App: installation token refreshed (valid ~55 min)")

    def _ensure_valid_token(self) -> str:
        with self._lock:
            if time.time() >= self._token_expires_at:
                self._refresh()
            return self._token

    def get_auth_header(self) -> str:
        return f"Bearer {self._ensure_valid_token()}"

    def get_token_for_git(self) -> str:
        # GitHub App tokens use "x-access-token" as the username for HTTPS auth.
        return f"x-access-token:{self._ensure_valid_token()}"

    @property
    def mode_label(self) -> str:
        return "GitHub App"


# ===========================================================================
# Rate-limit-aware, retry-enabled GitHub REST client
# ===========================================================================

class GitHubClient:

    _BASE_BACKOFF = 2.0
    _MAX_BACKOFF = 600.0

    def __init__(self, auth: _Auth, api_base: str, max_retries: int = _DEFAULT_API_RETRIES) -> None:
        self._auth = auth
        self._api_base = api_base.rstrip("/")
        self._max_retries = max_retries
        self._rl_lock = threading.Lock()
        self._rl_remaining: int = 5000
        self._rl_reset_at: float = 0.0

    def _update_rate_limit(self, headers: dict) -> None:
        with self._rl_lock:
            try:
                self._rl_remaining = int(
                    headers.get("X-RateLimit-Remaining", self._rl_remaining)
                )
                self._rl_reset_at = float(
                    headers.get("X-RateLimit-Reset", self._rl_reset_at)
                )
            except (ValueError, TypeError):
                pass

    def _wait_if_rate_limited(self) -> None:
        with self._rl_lock:
            if self._rl_remaining > _RATE_LIMIT_BUFFER or self._rl_reset_at <= time.time():
                return
            wait = max(0.0, self._rl_reset_at - time.time() + 10)
        log.warning(
            f"API rate limit low ({self._rl_remaining} requests remaining). "
            f"Pausing {wait:.0f}s until quota resets..."
        )
        time.sleep(wait)

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
    ) -> tuple[int, dict]:
        """Execute a GitHub REST call. Returns (http_status, response_body_dict)."""
        url = f"{self._api_base}{path}"

        for attempt in range(self._max_retries):
            self._wait_if_rate_limited()

            data = json.dumps(body).encode() if body else None
            req = urllib.request.Request(
                url,
                data=data,
                method=method,
                headers={
                    "Authorization": self._auth.get_auth_header(),
                    "Accept": "application/vnd.github+json",
                    "Content-Type": "application/json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )

            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    self._update_rate_limit(dict(resp.headers))
                    return resp.status, json.loads(resp.read())

            except urllib.error.HTTPError as exc:
                resp_headers = dict(exc.headers) if exc.headers else {}
                self._update_rate_limit(resp_headers)
                try:
                    resp_body: dict = json.loads(exc.read())
                except Exception:
                    resp_body = {"message": str(exc)}

                msg = resp_body.get("message", "")

                # Secondary rate limit (403 with rate-limit body)
                if exc.code == 403 and (
                    "secondary rate limit" in msg.lower()
                    or "rate limit" in msg.lower()
                ):
                    backoff = min(
                        self._MAX_BACKOFF,
                        self._BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 15),
                    )
                    log.warning(
                        f"Secondary rate limit hit "
                        f"(attempt {attempt + 1}/{self._max_retries}). "
                        f"Backing off {backoff:.0f}s..."
                    )
                    time.sleep(backoff)
                    continue

                # Primary rate limit
                if exc.code == 429:
                    retry_after = int(resp_headers.get("Retry-After", 60))
                    log.warning(f"429 Too Many Requests. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                # Transient server errors
                if exc.code in (500, 502, 503, 504):
                    backoff = min(self._MAX_BACKOFF, self._BASE_BACKOFF * (2 ** attempt))
                    log.warning(f"HTTP {exc.code} (transient). Retrying in {backoff:.0f}s...")
                    time.sleep(backoff)
                    continue

                # Non-retryable (400, 401, 404, 422, ...)
                return exc.code, resp_body

            except (urllib.error.URLError, OSError) as exc:
                backoff = min(self._MAX_BACKOFF, self._BASE_BACKOFF * (2 ** attempt))
                log.warning(f"Network error: {exc}. Retrying in {backoff:.0f}s...")
                time.sleep(backoff)

        raise RuntimeError(
            f"GitHub API call failed after {self._max_retries} retries: {method} {path}"
        )


# ===========================================================================
# Domain models
# ===========================================================================

@dataclass
class RepoSpec:
    namespace: str      # GitLab group / namespace
    project: str        # GitLab project name
    target_org: str     # GitHub organisation that will own this repo
    target_name: str    # Desired repo name on GitHub
    visibility: str = "private"
    branch_include: list[str] = field(default_factory=list)  # per-repo include patterns
    branch_exclude: list[str] = field(default_factory=list)  # per-repo exclude patterns
    branch_from_global: bool = False  # True when both columns were blank → inherited from global config


@dataclass
class MigrationConfig:
    auth: _Auth
    github_default_org: str  # fallback org when target_org is blank in repos.csv
    github_api_url: str
    github_url: str
    gitlab_url: str
    gitlab_pat: str = field(repr=False)  # never expose credential in repr / logs
    max_workers: int = 5
    clone_timeout: int = 7200
    push_timeout: int = 7200
    git_max_retries: int = 3
    dry_run: bool = False                              # log intent only; skip all writes
    branch_include: list[str] = field(default_factory=list)  # regex include patterns; empty = all
    branch_exclude: list[str] = field(default_factory=list)  # regex exclude patterns; empty = none
    min_free_disk_gb: float = 10.0                     # min free temp-dir space before starting
    repos: list[RepoSpec] = field(default_factory=list)


@dataclass
class _RepoResult:
    source: str              # namespace/project
    target: str              # org/target_name
    status: str              # "succeeded" | "failed" | "skipped" | "dry-run"
    default_branch: str      # actual source default branch (empty if unknown)
    error: str               # error message on failure (empty on success)
    duration_seconds: float  # wall-clock seconds for this repo
    completed_at: str        # "YYYY-MM-DD HH:MM:SS" human-readable timestamp
    branch_count: int = 0             # branches pushed (post-filter)
    head_commit_sha: str = ""         # HEAD commit SHA from source bare clone
    gh_branch_count: int = 0          # branch count on GitHub after push
    gh_head_commit_sha: str = ""      # HEAD commit SHA on GitHub after push
    validation_status: str = ""       # "match" | "mismatch" | "unknown" | "dry-run"
    validation_notes: str = ""        # human-readable diff notes


@dataclass
class _MirrorResult:
    """Return type of _mirror_repo -- avoids a wide positional tuple."""
    ok: bool
    default_branch: str = ""
    error: str = ""
    branch_count: int = 0
    head_commit_sha: str = ""
    gh_branch_count: int = 0
    gh_head_commit_sha: str = ""
    validation_status: str = ""
    validation_notes: str = ""


# ===========================================================================
# Checkpoint / Resume
# ===========================================================================

class MigrationState:

    def __init__(self, state_file: Path) -> None:
        self._file = state_file
        self._lock = threading.Lock()
        self._state: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if self._file.exists():
            try:
                self._state = json.loads(self._file.read_text(encoding="utf-8"))
                n_done = sum(1 for v in self._state.values() if v == "succeeded")
                log.info(
                    f"Checkpoint found: {self._file.name} "
                    f"({n_done} succeeded, {len(self._state)} total entries)"
                )
            except Exception as exc:
                log.warning(f"Could not read checkpoint ({exc}). Starting fresh.")
                self._state = {}

    def _save(self) -> None:
        """Atomic write: write to .tmp then rename, so a crash never corrupts the file."""
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
        tmp.replace(self._file)

    def is_succeeded(self, key: str) -> bool:
        with self._lock:
            return self._state.get(key) == "succeeded"

    def record(self, key: str, status: str) -> None:
        with self._lock:
            self._state[key] = status
            self._save()


# ===========================================================================
# Git subprocess helper
# ===========================================================================

def _run_git(
    cmd: list[str],
    cwd: Path | None = None,
    timeout: int = 3600,
    log_cmd: list[str] | None = None,
) -> tuple[int, str, str]:

    display = " ".join(log_cmd if log_cmd is not None else cmd)
    log.debug(f"git: {display}")

    env = os.environ.copy()
    # Prevent git from blocking on interactive credential prompts.
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_ASKPASS"] = "echo"
    # Suppress OS credential managers (Keychain on macOS, Windows Credential Manager).
    env["GIT_CONFIG_COUNT"] = "1"
    env["GIT_CONFIG_KEY_0"] = "credential.helper"
    env["GIT_CONFIG_VALUE_0"] = ""

    kwargs: dict = {
        "capture_output": True,
        "text": True,
        "encoding": "utf-8",
        "errors": "replace",
        "timeout": timeout,
        "env": env,
        **({"cwd": str(cwd.resolve())} if cwd is not None else {}),
        **({"creationflags": 0x08000000} if sys.platform == "win32" else {}),  # CREATE_NO_WINDOW
    }

    try:
        result = subprocess.run(cmd, **kwargs)
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except FileNotFoundError:
        return (
            1, "",
            "'git' not found on PATH. Install: https://git-scm.com/downloads",
        )
    except subprocess.TimeoutExpired:
        return 1, "", f"git timed out after {timeout}s: {display}"


# ===========================================================================
# GitHub repo operations
# ===========================================================================

def _create_github_repo(
    client: GitHubClient,
    org: str,
    name: str,
    visibility: str,
) -> bool:

    _body = {
        "name": name,
        "private": visibility != "public",
        "auto_init": False,
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False,
    }
    status, resp = client.request(
        "POST",
        f"/orgs/{org}/repos",
        body={**_body, "visibility": visibility},
    )
    if status in (200, 201):
        log.info(f"[{name}] GitHub repo created ({visibility})")
        return True

    msg = resp.get("message", "unknown")
    if status == 422 and "already exists" in msg.lower():
        log.info(f"[{name}] GitHub repo already exists")
        return True

    # org endpoint 404 means the PAT belongs to a user account, not an org.
    if status == 404:
        log.debug(f"[{name}] Org endpoint returned 404 -- trying user-level creation")
        status, resp = client.request(
            "POST",
            "/user/repos",
            body={**_body, "visibility": "private" if visibility == "internal" else visibility},
        )
        if status in (200, 201):
            log.info(f"[{name}] GitHub repo created under user account ({visibility})")
            return True
        msg = resp.get("message", "unknown")

    log.error(f"[{name}] Failed to create GitHub repo (HTTP {status}): {msg}")
    return False


def _get_default_branch(mirror_dir: Path) -> str:
    """Read the HEAD symbolic-ref from a bare clone to get the source default branch.

    Returns the branch name exactly as set on the source (e.g. 'main', 'master',
    'develop', 'trunk').  Returns empty string if unresolvable.
    """
    code, out, _ = _run_git(
        ["git", "symbolic-ref", "HEAD"],
        cwd=mirror_dir,
        timeout=15,
    )
    if code != 0 or not out:
        return ""
    prefix = "refs/heads/"
    return out[len(prefix):] if out.startswith(prefix) else out


def _count_branches(mirror_dir: Path) -> int:
    """Count refs/heads/ entries remaining in a bare clone after filtering."""
    code, out, _ = _run_git(
        ["git", "for-each-ref", "--format=%(refname:short)", "refs/heads/"],
        cwd=mirror_dir,
        timeout=30,
    )
    return len([b for b in out.splitlines() if b.strip()]) if code == 0 else 0


def _get_head_commit_sha(mirror_dir: Path, branch: str) -> str:
    """Return the full commit SHA at refs/heads/<branch> in a bare clone."""
    if not branch:
        return ""
    code, out, _ = _run_git(
        ["git", "rev-parse", f"refs/heads/{branch}"],
        cwd=mirror_dir,
        timeout=15,
    )
    return out.strip() if code == 0 else ""


def _set_github_default_branch(
    client: GitHubClient,
    org: str,
    name: str,
    branch: str,
) -> None:
    """Set the GitHub repo's default branch to match the source."""
    status, resp = client.request(
        "PATCH",
        f"/repos/{org}/{name}",
        body={"default_branch": branch},
    )
    if status == 200:
        log.info(f"[{name}] Default branch set to '{branch}'")
    else:
        log.warning(
            f"[{name}] Could not set default branch to '{branch}' "
            f"(HTTP {status}: {resp.get('message', 'unknown')})"
        )


def _get_github_branch_count(client: GitHubClient, org: str, repo: str) -> int:
    """Return the total number of branches in a GitHub repo (handles pagination)."""
    page, total = 1, 0
    while True:
        st, resp = client.request(
            "GET", f"/repos/{org}/{repo}/branches?per_page=100&page={page}"
        )
        if st != 200 or not isinstance(resp, list):
            break
        total += len(resp)
        if len(resp) < 100:
            break
        page += 1
    return total


def _get_github_head_commit_sha(
    client: GitHubClient, org: str, repo: str, branch: str
) -> str:
    """Return the HEAD commit SHA of a branch via the GitHub Git refs API."""
    if not branch or branch == "dry-run":
        return ""
    st, resp = client.request("GET", f"/repos/{org}/{repo}/git/ref/heads/{branch}")
    if st == 200 and isinstance(resp, dict):
        return resp.get("object", {}).get("sha", "")
    return ""


def _compute_validation(
    branch_count: int,
    head_sha: str,
    gh_branch_count: int,
    gh_head_sha: str,
) -> tuple[str, str]:
   
    if not gh_branch_count and not gh_head_sha:
        return "unknown", "GitHub validation data unavailable"
    notes: list[str] = []
    if branch_count and gh_branch_count and branch_count != gh_branch_count:
        notes.append(f"branch count: source={branch_count} github={gh_branch_count}")
    if head_sha and gh_head_sha and head_sha != gh_head_sha:
        notes.append(f"HEAD SHA: source={head_sha[:8]}... github={gh_head_sha[:8]}...")
    return ("mismatch", "; ".join(notes)) if notes else ("match", "")


# ===========================================================================
# Git retry helper and branch filter
# ===========================================================================

def _git_with_retry(
    cmd: list[str],
    log_cmd: list[str],
    timeout: int,
    max_retries: int,
    label: str,
    action: str,
    cwd: Path | None = None,
    cleanup_dir: Path | None = None,
) -> tuple[bool, str]:

    err = ""
    for attempt in range(max_retries):
        code, _, err = _run_git(cmd, log_cmd=log_cmd, cwd=cwd, timeout=timeout)
        if code == 0:
            return True, ""
        if attempt < max_retries - 1:
            backoff = 30 * (attempt + 1)
            log.warning(
                f"[{label}] {action} failed (attempt {attempt + 1}/{max_retries}). "
                f"Retrying in {backoff}s: {err}"
            )
            if cleanup_dir is not None:
                shutil.rmtree(str(cleanup_dir), ignore_errors=True)
            time.sleep(backoff)
    return False, f"{action} failed after {max_retries} attempts: {err}"


def _filter_branches(
    mirror_dir: Path,
    include_patterns: list[str],
    exclude_patterns: list[str],
    label: str,
) -> None:
 
    if not include_patterns and not exclude_patterns:
        return
    code, out, _ = _run_git(
        ["git", "for-each-ref", "--format=%(refname:short)", "refs/heads/"],
        cwd=mirror_dir,
        timeout=30,
    )
    if code != 0 or not out:
        return
    branches = [b.strip() for b in out.splitlines() if b.strip()]
    if not branches:
        return

    default = _get_default_branch(mirror_dir)
    kept = list(branches)
    if include_patterns:
        kept = [b for b in kept if any(re.search(p, b) for p in include_patterns)]
    if exclude_patterns:
        kept = [b for b in kept if not any(re.search(p, b) for p in exclude_patterns)]

    # Always preserve the default branch regardless of filter outcome.
    if default and default not in kept:
        kept.append(default)
        log.debug(f"[{label}] Default branch '{default}' preserved despite filter rules")

    to_delete = set(branches) - set(kept)
    if not to_delete:
        return

    sample = ", ".join(sorted(to_delete)[:8])
    suffix = f", +{len(to_delete) - 8} more" if len(to_delete) > 8 else ""
    log.info(f"[{label}] Branch filter: {len(kept)} kept, {len(to_delete)} removed ({sample}{suffix})")
    for branch in to_delete:
        c, _, e = _run_git(
            ["git", "update-ref", "-d", f"refs/heads/{branch}"],
            cwd=mirror_dir,
            timeout=15,
        )
        if c != 0:
            log.warning(f"[{label}] Could not remove branch '{branch}': {e}")


# ===========================================================================
# Core per-repo mirror operation
# ===========================================================================

def _mirror_repo(
    spec: RepoSpec,
    config: MigrationConfig,
    client: GitHubClient,
) -> _MirrorResult:

    gitlab_base = config.gitlab_url.rstrip("/")
    github_base = config.github_url.rstrip("/")
    name = spec.target_name

    # Authenticated URLs -- NEVER passed to log or log_cmd.
    gl_scheme, gl_rest = gitlab_base.split("://", 1)
    clone_url = (
        f"{gl_scheme}://oauth2:{config.gitlab_pat}@{gl_rest}"
        f"/{spec.namespace}/{spec.project}.git"
    )
    push_url = (
        f"https://{config.auth.get_token_for_git()}@"
        f"{github_base.split('://', 1)[-1]}/{spec.target_org}/{name}.git"
    )
    safe_clone = f"{gitlab_base}/{spec.namespace}/{spec.project}.git"
    safe_push  = f"{github_base}/{spec.target_org}/{name}.git"

    if config.dry_run:
        log.info(f"[{name}] DRY RUN -- would clone {safe_clone} -> {safe_push}")
        return _MirrorResult(ok=True, default_branch="dry-run", validation_status="dry-run")

    tmp_dir = Path(tempfile.mkdtemp(prefix="gitmirror_"))
    mirror_dir = tmp_dir / f"{spec.project}.git"

    try:
        if not _create_github_repo(client, spec.target_org, name, spec.visibility):
            return _MirrorResult(ok=False, error="Failed to create GitHub repository")

        log.info(f"[{name}] Cloning {safe_clone}")
        ok, err = _git_with_retry(
            cmd=["git", "clone", "--mirror", clone_url, str(mirror_dir)],
            log_cmd=["git", "clone", "--mirror", safe_clone, str(mirror_dir)],
            timeout=config.clone_timeout,
            max_retries=config.git_max_retries,
            label=name,
            action="Clone",
            cleanup_dir=mirror_dir,
        )
        if not ok:
            return _MirrorResult(ok=False, error=err)

        _filter_branches(mirror_dir, spec.branch_include, spec.branch_exclude, name)

        branch_count   = _count_branches(mirror_dir)
        default_branch = _get_default_branch(mirror_dir)
        head_sha       = _get_head_commit_sha(mirror_dir, default_branch)
        if default_branch:
            log.info(f"[{name}] Default branch: '{default_branch}' | {branch_count} branch(es)")
        else:
            log.warning(f"[{name}] Could not determine source default branch")

        log.info(f"[{name}] Pushing to {safe_push}")
        ok, err = _git_with_retry(
            cmd=["git", "push", "--mirror", push_url],
            log_cmd=["git", "push", "--mirror", safe_push],
            timeout=config.push_timeout,
            max_retries=config.git_max_retries,
            label=name,
            action="Push",
            cwd=mirror_dir,
        )
        if not ok:
            return _MirrorResult(ok=False, error=err)

        if default_branch:
            _set_github_default_branch(client, spec.target_org, name, default_branch)

        gh_branches = _get_github_branch_count(client, spec.target_org, name)
        gh_head_sha = _get_github_head_commit_sha(client, spec.target_org, name, default_branch)
        v_status, v_notes = _compute_validation(branch_count, head_sha, gh_branches, gh_head_sha)

        return _MirrorResult(
            ok=True,
            default_branch=default_branch,
            branch_count=branch_count,
            head_commit_sha=head_sha,
            gh_branch_count=gh_branches,
            gh_head_commit_sha=gh_head_sha,
            validation_status=v_status,
            validation_notes=v_notes,
        )

    except Exception as exc:
        return _MirrorResult(ok=False, error=str(exc))

    finally:
        shutil.rmtree(str(tmp_dir), ignore_errors=True)


# ===========================================================================
# repos.csv loader
# ===========================================================================

def load_repos_csv(
    csv_path: Path,
    default_org: str = "",
    default_branch_include: list[str] | None = None,
    default_branch_exclude: list[str] | None = None,
) -> list[RepoSpec]:

    _global_inc = default_branch_include or []
    _global_exc = default_branch_exclude or []

    def _parse_patterns(raw_cell: str, defaults: list[str]) -> list[str]:
        """Split a semicolon-separated cell into a list of regex patterns."""
        raw_cell = (raw_cell or "").strip()
        return [p.strip() for p in raw_cell.split(";") if p.strip()] if raw_cell else defaults

    required_cols = {"namespace", "project", "target_name"}
    repos: list[RepoSpec] = []
    skipped = 0

    try:
        fh = csv_path.open(encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise FileNotFoundError(f"Cannot open {csv_path.name}: {exc}") from exc

    with fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise _MissingConfigError(f"{csv_path.name} is empty or has no header row.")

        actual = {c.strip().lower() for c in reader.fieldnames}
        missing = required_cols - actual
        if missing:
            raise ValueError(
                f"{csv_path.name} is missing required columns: {missing}. "
                "First row must be: namespace,project,target_name[,visibility]"
            )

        for i, row in enumerate(reader, start=2):
            ns = (row.get("namespace") or "").strip()
            proj = (row.get("project") or "").strip()
            tgt_org = (row.get("target_org") or "").strip() or default_org
            tgt = (row.get("target_name") or "").strip()
            vis = (row.get("visibility") or "private").strip().lower()

            if not ns or not proj or not tgt:
                log.warning(f"repos.csv row {i}: blank required field (namespace/project/target_name) -- skipped")
                skipped += 1
                continue

            if not tgt_org:
                log.warning(
                    f"repos.csv row {i}: [{ns}/{proj}] has no target_org in CSV "
                    f"and no github.default_org set in mirror-config.json -- skipped"
                )
                skipped += 1
                continue

            if vis not in _VALID_VISIBILITIES:
                log.warning(
                    f"repos.csv row {i}: invalid visibility '{vis}' -- defaulting to 'private'"
                )
                vis = "private"

            inc_raw = (row.get("branch_include") or "").strip()
            exc_raw = (row.get("branch_exclude") or "").strip()
            inc = _parse_patterns(inc_raw, _global_inc)
            exc = _parse_patterns(exc_raw, _global_exc)
            from_global = not inc_raw and not exc_raw

            repos.append(RepoSpec(ns, proj, tgt_org, tgt, vis, inc, exc, from_global))

    if skipped:
        log.warning(f"Skipped {skipped} invalid row(s) in repos.csv")

    if not repos:
        raise _MissingConfigError(
            f"repos.csv is empty -- no repositories to migrate.\n"
            f"  Open {csv_path} and add at least one row, e.g.:\n"
            f"    namespace,project,target_org,target_name,visibility\n"
            f"    my-group,my-repo,my-github-org,my-repo,private"
        )

    log.info(f"Loaded {len(repos)} repo(s) from {csv_path.name}")
    return repos


# ===========================================================================
# Config loading and validation
# ===========================================================================

def _is_placeholder(value: str) -> bool:
    return not value or any(tok in value for tok in _PLACEHOLDER_TOKENS)


def _check_placeholders(raw: dict) -> None:
    """Scan mirror-config.json for unfilled placeholder values. Reports all issues at once."""
    issues: list[str] = []
    auth = raw.get("auth", {})
    mode = str(auth.get("mode", "pat")).lower()

    if mode == "pat":
        if _is_placeholder(auth.get("pat", "")):
            issues.append(
                "auth.pat -- replace with your GitHub Personal Access Token "
                "(GitHub -> Settings -> Developer settings -> Personal access tokens)"
            )
    elif mode == "app":
        app = auth.get("app", {})
        app_id = app.get("app_id")
        if not isinstance(app_id, int) or app_id <= 0:
            issues.append("auth.app.app_id -- replace with your numeric GitHub App ID")
        key_path_val = app.get("private_key_path", "")
        if not key_path_val or _is_placeholder(key_path_val):
            issues.append(
                "auth.app.private_key_path -- path to your GitHub App .pem private key file"
            )
        inst_id = app.get("installation_id")
        if not isinstance(inst_id, int) or inst_id <= 0:
            issues.append(
                "auth.app.installation_id -- replace with your numeric GitHub App Installation ID"
            )
    else:
        issues.append(f"auth.mode -- invalid value '{mode}': must be 'pat' or 'app'")

    gl = raw.get("gitlab", {})
    if _is_placeholder(gl.get("url", "")):
        issues.append(
            "gitlab.url -- replace with your GitLab instance URL "
            "(e.g. https://gitlab.com or https://gitlab.your-company.com)"
        )
    if _is_placeholder(gl.get("pat", "")):
        issues.append(
            "gitlab.pat -- replace with your GitLab Personal Access Token "
            "(GitLab -> User Settings -> Access Tokens, scope: read_repository)"
        )

    if issues:
        numbered = "\n".join(f"  {i + 1}. {m}" for i, m in enumerate(issues))
        raise _MissingConfigError(
            f"mirror-config.json has {len(issues)} unfilled value(s):\n"
            f"{numbered}\n\n"
            "Fill in all values and run again."
        )


def load_config(config_path: Path, repos_csv_path: Path) -> MigrationConfig:
    """Parse mirror-config.json + repos.csv. Returns a validated MigrationConfig."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Create mirror-config.json next to this script."
        )

    try:
        raw = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"mirror-config.json: invalid JSON at line {exc.lineno}: {exc.msg}"
        ) from exc

    for section in ("auth", "github", "gitlab"):
        if section not in raw:
            raise ValueError(f"mirror-config.json is missing required section: '{section}'")

    _check_placeholders(raw)

    gh = raw["github"]
    gl = raw["gitlab"]
    auth_raw = raw["auth"]
    mig = raw.get("migration", {})

    mode = str(auth_raw.get("mode", "pat")).lower()
    api_base = gh.get("api_url", _GITHUB_API_URL)

    if mode == "pat":
        auth: _Auth = PATAuth(auth_raw["pat"])
    else:
        app_conf = auth_raw["app"]
        key_path = Path(app_conf["private_key_path"])
        if not key_path.is_absolute():
            key_path = (_SCRIPT_DIR / key_path).resolve()
        if not key_path.exists():
            raise FileNotFoundError(
                f"GitHub App private key not found: {key_path}\n"
                "Ensure 'auth.app.private_key_path' points to the .pem file."
            )
        private_key = key_path.read_text(encoding="utf-8")
        auth = GitHubAppAuth(
            app_id=int(app_conf["app_id"]),
            private_key_pem=private_key,
            installation_id=int(app_conf["installation_id"]),
            api_base=api_base,
        )

    log.info(f"Auth mode : {auth.mode_label}")

    default_org = gh.get("default_org", "").strip()

    branches_conf  = raw.get("branches", {})
    branch_include = [str(p) for p in branches_conf.get("include", [])]
    branch_exclude = [str(p) for p in branches_conf.get("exclude", [])]

    repos = load_repos_csv(repos_csv_path, default_org, branch_include, branch_exclude)

    # Clamp configurable integers to safe operating bounds.
    # max_workers=0  → ThreadPoolExecutor raises ValueError immediately.
    # max_workers>20 → likely to exhaust file descriptors / overwhelm the source.
    # git_max_retries<1 → the for-loop body never executes (no attempt is made).
    raw_workers = int(mig.get("max_workers", _DEFAULT_MAX_WORKERS))
    if not (1 <= raw_workers <= 20):
        raise ValueError(
            f"mirror-config.json: migration.max_workers must be between 1 and 20 "
            f"(got {raw_workers})"
        )

    raw_retries = int(mig.get("git_retry_count", _DEFAULT_GIT_RETRIES))
    if raw_retries < 1:
        raise ValueError(
            f"mirror-config.json: migration.git_retry_count must be at least 1 "
            f"(got {raw_retries})"
        )

    dry_run = bool(mig.get("dry_run", False))
    if dry_run:
        log.info("DRY RUN mode enabled -- no repos will be created or pushed")

    min_free_disk_gb = float(mig.get("min_free_disk_gb", _DEFAULT_MIN_FREE_DISK_GB))

    return MigrationConfig(
        auth=auth,
        github_default_org=default_org,
        github_api_url=api_base,
        github_url=gh.get("url", _GITHUB_URL),
        gitlab_url=gl["url"],
        gitlab_pat=gl["pat"],
        max_workers=raw_workers,
        clone_timeout=int(mig.get("clone_timeout_seconds", _DEFAULT_CLONE_TIMEOUT)),
        push_timeout=int(mig.get("push_timeout_seconds", _DEFAULT_PUSH_TIMEOUT)),
        git_max_retries=raw_retries,
        dry_run=dry_run,
        branch_include=branch_include,
        branch_exclude=branch_exclude,
        min_free_disk_gb=min_free_disk_gb,
        repos=repos,
    )


# ===========================================================================
# Signal handling -- graceful shutdown on Ctrl+C / SIGTERM
# ===========================================================================

_shutdown_event = threading.Event()


def _handle_signal(signum: int, frame: object) -> None:
    log.warning(
        f"Signal {signum} received -- finishing active repositories then shutting down. "
        "Re-run the script to resume from checkpoint."
    )
    _shutdown_event.set()


signal.signal(signal.SIGINT, _handle_signal)
if sys.platform != "win32":
    signal.signal(signal.SIGTERM, _handle_signal)


# ===========================================================================
# Pre-flight connectivity and environment checks
# ===========================================================================

def _check_github_connectivity(client: GitHubClient) -> None:
    """Validate GitHub credentials and connectivity via GET /rate_limit (zero quota cost)."""
    status, resp = client.request("GET", "/rate_limit")
    if status == 401:
        raise RuntimeError("GitHub authentication failed: token is invalid or expired")
    if status != 200:
        raise RuntimeError(f"GitHub API unreachable (HTTP {status})")
    core = resp.get("resources", {}).get("core", {})
    log.info(
        f"GitHub connectivity OK  |  rate limit: "
        f"{core.get('remaining', '?')}/{core.get('limit', '?')} requests remaining"
    )


def _check_gitlab_connectivity(gitlab_url: str, gitlab_pat: str) -> None:
    """Validate GitLab credentials and connectivity via GET /api/v4/user."""
    url = f"{gitlab_url.rstrip('/')}/api/v4/user"
    req = urllib.request.Request(url, headers={"PRIVATE-TOKEN": gitlab_pat})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        log.info(f"GitLab connectivity OK  |  authenticated as: {data.get('username', '?')}")
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            raise RuntimeError("GitLab authentication failed: PAT is invalid or expired")
        raise RuntimeError(f"GitLab API error (HTTP {exc.code}) at {gitlab_url}")
    except (urllib.error.URLError, OSError) as exc:
        raise RuntimeError(f"Cannot reach GitLab at {gitlab_url}: {exc}")


def _check_disk_space(min_free_gb: float) -> None:
    """Ensure sufficient free disk space in the system temp directory."""
    tmp = Path(tempfile.gettempdir())
    free_gb = shutil.disk_usage(tmp).free / (1024 ** 3)
    if free_gb < min_free_gb:
        raise RuntimeError(
            f"Insufficient disk space: {free_gb:.1f} GB free in {tmp}, "
            f"need at least {min_free_gb:.1f} GB. "
            "Free up space or lower migration.min_free_disk_gb in mirror-config.json."
        )


def _preflight_checks(config: MigrationConfig, client: GitHubClient) -> None:
    """Run all pre-flight checks before any migration work begins."""
    _check_disk_space(config.min_free_disk_gb)
    _check_github_connectivity(client)
    if not config.dry_run:
        _check_gitlab_connectivity(config.gitlab_url, config.gitlab_pat)


# ===========================================================================
# Timing metrics
# ===========================================================================

def _compute_metrics(results: list[_RepoResult]) -> dict:
    """Compute timing percentiles for all processed (non-skipped) repos."""
    durations = sorted(
        r.duration_seconds for r in results
        if r.status not in ("skipped", "dry-run") and r.duration_seconds > 0
    )
    if not durations:
        return {}
    n = len(durations)

    def _pct(p: float) -> float:
        return round(durations[min(int(n * p / 100), n - 1)], 1)

    return {
        "count": n,
        "min_seconds": round(durations[0], 1),
        "max_seconds": round(durations[-1], 1),
        "mean_seconds": round(sum(durations) / n, 1),
        "p50_seconds": _pct(50),
        "p95_seconds": _pct(95),
        "p99_seconds": _pct(99),
    }


# ===========================================================================
# Report writing
# ===========================================================================

def _write_reports(
    results: list[_RepoResult],
    run_started: str,
    config: MigrationConfig,
    elapsed_seconds: float = 0.0,
) -> None:
    """Write timestamped JSON and XLSX (or CSV fallback) reports to the reports folder."""
    succeeded = [r for r in results if r.status == "succeeded"]
    failed    = [r for r in results if r.status == "failed"]
    skipped   = [r for r in results if r.status == "skipped"]
    dry_run   = [r for r in results if r.status == "dry-run"]

    target_orgs = sorted({r.target.split("/")[0] for r in results if "/" in r.target})

    # ---- JSON ----------------------------------------------------------
    report: dict = {
        "run_timestamp": run_started,
        "auth_mode": config.auth.mode_label,
        "dry_run": config.dry_run,
        "target_orgs": target_orgs,
        "gitlab_url": config.gitlab_url,
        "total_elapsed_seconds": round(elapsed_seconds, 1),
        "summary": {
            "total_in_csv": len(config.repos),
            "processed_this_run": len(succeeded) + len(failed) + len(dry_run),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "skipped_already_done": len(skipped),
            "dry_run": len(dry_run),
        },
        "timing_metrics": _compute_metrics(results),
        "repos": [
            {
                "source": r.source,
                "target": r.target,
                "status": r.status,
                "default_branch": r.default_branch,
                "branch_count": r.branch_count,
                "head_commit_sha": r.head_commit_sha,
                "gh_branch_count": r.gh_branch_count,
                "gh_head_commit_sha": r.gh_head_commit_sha,
                "validation_status": r.validation_status,
                "validation_notes": r.validation_notes,
                "error": r.error,
                "duration_seconds": round(r.duration_seconds, 1),
                "duration_minutes": round(r.duration_seconds / 60, 3),
                "completed_at": r.completed_at,
            }
            for r in results
        ],
    }
    _RESULTS_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    log.info(f"JSON report : {_RESULTS_JSON}")

    # ---- XLSX / CSV ----------------------------------------------------
    if _XLSX_AVAILABLE:
        _write_xlsx(results, report, config, elapsed_seconds)
        log.info(f"XLSX report : {_RESULTS_XLSX}")
    else:
        _write_csv_fallback(results)
        log.info(f"CSV report  : {_RESULTS_CSV}")
        log.warning(
            "openpyxl not installed -- plain CSV written (no multi-sheet support). "
            "Install with:  pip install openpyxl"
        )

    log.info(f"Log file    : {_LOG_FILE}")


def _write_csv_fallback(results: list[_RepoResult]) -> None:
    """Write a plain CSV report when openpyxl is not available."""
    with _RESULTS_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "source", "target", "status", "default_branch",
            "duration_min", "branch_count", "head_commit_sha",
            "error", "completed_at",
        ])
        for r in results:
            writer.writerow([
                r.source, r.target, r.status, r.default_branch,
                round(r.duration_seconds / 60, 3) if r.duration_seconds else "",
                r.branch_count or "", r.head_commit_sha, r.error, r.completed_at,
            ])


def _write_xlsx(
    results: list[_RepoResult],
    report: dict,
    config: MigrationConfig,
    elapsed_seconds: float,
) -> None:
    """Write a multi-sheet XLSX report: Repositories, Run Summary, Validation."""
    import openpyxl
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    def _fill(hex_color: str) -> PatternFill:
        return PatternFill("solid", fgColor=hex_color)

    HDR_FILL     = _fill("1F4E79")
    HDR_FONT     = Font(bold=True, color="FFFFFF", size=10)
    STATUS_FILLS = {
        "succeeded": _fill("C6EFCE"),
        "failed":    _fill("FFC7CE"),
        "skipped":   _fill("F2F2F2"),
        "dry-run":   _fill("FFEB9C"),
    }
    VAL_FILLS = {
        "match":    _fill("C6EFCE"),
        "mismatch": _fill("FFC7CE"),
        "unknown":  _fill("FFEB9C"),
    }
    LEFT   = Alignment(horizontal="left",   vertical="center")
    CENTER = Alignment(horizontal="center", vertical="center")

    def _header_row(ws, headers: list) -> None:
        for ci, hdr in enumerate(headers, 1):
            c = ws.cell(row=1, column=ci, value=hdr)
            c.font = HDR_FONT
            c.fill = HDR_FILL
            c.alignment = CENTER
        ws.freeze_panes = "A2"
        ws.row_dimensions[1].height = 22

    def _col_widths(ws, widths: list) -> None:
        for ci, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(ci)].width = w

    wb = openpyxl.Workbook()

    # ---- Sheet 1: Repositories ----------------------------------------
    ws_repos = wb.active
    ws_repos.title = "Repositories"
    _header_row(ws_repos, [
        "Source (GitLab)", "Target (GitHub)", "Status", "Default Branch",
        "Duration (min)", "Branches Pushed", "Source HEAD SHA", "Error", "Completed At",
    ])
    for ri, r in enumerate(results, 2):
        rf = STATUS_FILLS.get(r.status, _fill("FFFFFF"))
        for ci, val in enumerate([
            r.source,
            r.target,
            r.status,
            r.default_branch,
            round(r.duration_seconds / 60, 3) if r.duration_seconds else "",
            r.branch_count or "",
            r.head_commit_sha,
            r.error,
            r.completed_at,
        ], 1):
            c = ws_repos.cell(row=ri, column=ci, value=val)
            c.fill = rf
            c.alignment = LEFT
    _col_widths(ws_repos, [45, 40, 12, 18, 14, 16, 44, 50, 22])

    # ---- Sheet 2: Run Summary -----------------------------------------
    ws_sum = wb.create_sheet("Run Summary")
    ws_sum.column_dimensions["A"].width = 24
    ws_sum.column_dimensions["B"].width = 42

    m = report.get("timing_metrics", {})
    summary_rows: list = [
        ("Run Timestamp",      report["run_timestamp"]),
        ("Auth Mode",          config.auth.mode_label),
        ("GitLab URL",         config.gitlab_url),
        ("GitHub URL",         config.github_url),
        ("Concurrent Workers", str(config.max_workers)),
        ("Dry Run",            str(config.dry_run)),
        ("", ""),
        ("RESULTS", ""),
        ("Total in CSV",       str(len(config.repos))),
        ("Succeeded",          str(sum(1 for r in results if r.status == "succeeded"))),
        ("Failed",             str(sum(1 for r in results if r.status == "failed"))),
        ("Skipped (done)",     str(sum(1 for r in results if r.status == "skipped"))),
        ("Dry-Run Simulated",  str(sum(1 for r in results if r.status == "dry-run"))),
        ("", ""),
        ("TIMING", ""),
        ("Total Duration",     f"{int(elapsed_seconds // 60)}m {int(elapsed_seconds % 60):02d}s"),
    ]
    if m:
        summary_rows += [
            ("Min Duration",   f"{m.get('min_seconds', 0)}s"),
            ("Max Duration",   f"{m.get('max_seconds', 0)}s"),
            ("Mean Duration",  f"{m.get('mean_seconds', 0)}s"),
            ("P50 Duration",   f"{m.get('p50_seconds', 0)}s"),
            ("P95 Duration",   f"{m.get('p95_seconds', 0)}s"),
            ("P99 Duration",   f"{m.get('p99_seconds', 0)}s"),
        ]

    section_font = Font(bold=True, color="1F4E79", size=10)
    label_font   = Font(bold=True, size=10)
    section_fill = _fill("DDEEFF")
    for ri, (label, value) in enumerate(summary_rows, 1):
        cl = ws_sum.cell(row=ri, column=1, value=label)
        cv = ws_sum.cell(row=ri, column=2, value=value)
        cl.alignment = LEFT
        cv.alignment = LEFT
        if label in ("RESULTS", "TIMING"):
            cl.font = section_font
            cl.fill = section_fill
            cv.fill = section_fill
        elif label:
            cl.font = label_font

    # ---- Sheet 3: Validation ------------------------------------------
    ws_val = wb.create_sheet("Validation")
    _header_row(ws_val, [
        "Source (GitLab)", "Target (GitHub)", "Migration Status",
        "Source Branches", "GitHub Branches", "Branches Match",
        "Source HEAD SHA", "GitHub HEAD SHA", "HEAD Commit Match",
        "Validation Status", "Validation Notes",
    ])
    for ri, r in enumerate(
        (r for r in results if r.status not in ("skipped", "dry-run")), 2
    ):
        b_ok = bool(r.branch_count and r.gh_branch_count)
        h_ok = bool(r.head_commit_sha and r.gh_head_commit_sha)
        rf   = VAL_FILLS.get(r.validation_status, _fill("FFFFFF"))
        for ci, val in enumerate([
            r.source,
            r.target,
            r.status,
            r.branch_count or "",
            r.gh_branch_count or "",
            "\u2713" if b_ok and r.branch_count == r.gh_branch_count else ("\u2717" if b_ok else ""),
            r.head_commit_sha,
            r.gh_head_commit_sha,
            "\u2713" if h_ok and r.head_commit_sha == r.gh_head_commit_sha else ("\u2717" if h_ok else ""),
            r.validation_status,
            r.validation_notes,
        ], 1):
            c = ws_val.cell(row=ri, column=ci, value=val)
            c.fill = rf
            c.alignment = LEFT
    _col_widths(ws_val, [45, 40, 18, 16, 16, 16, 44, 44, 18, 14, 50])

    wb.save(str(_RESULTS_XLSX))


# ===========================================================================
# Per-repo worker (runs in thread pool)
# ===========================================================================

def _migrate_one(
    spec: RepoSpec,
    config: MigrationConfig,
    client: GitHubClient,
    state: MigrationState,
) -> _RepoResult:
    """Migrate one repository. Designed to be submitted to a ThreadPoolExecutor."""
    key = f"{spec.namespace}/{spec.project}"
    t_start = time.monotonic()

    mr = _mirror_repo(spec, config, client)
    if config.dry_run:
        status = "dry-run"
    else:
        status = "succeeded" if mr.ok else "failed"
        state.record(key, status)

    return _RepoResult(
        source=key,
        target=f"{spec.target_org}/{spec.target_name}",
        status=status,
        default_branch=mr.default_branch,
        error=mr.error,
        duration_seconds=time.monotonic() - t_start,
        completed_at=_fmt_ts(),
        branch_count=mr.branch_count,
        head_commit_sha=mr.head_commit_sha,
        gh_branch_count=mr.gh_branch_count,
        gh_head_commit_sha=mr.gh_head_commit_sha,
        validation_status=mr.validation_status,
        validation_notes=mr.validation_notes,
    )


# ===========================================================================
# Pre-flight preview and confirmation
# ===========================================================================

def _print_migration_preview(
    config: MigrationConfig,
    pending: list[RepoSpec],
    already_done: list[RepoSpec],
    config_path: Path | None = None,
    repos_csv_path: Path | None = None,
) -> bool:

    W = 72
    BORDER  = "\u2550" * W   # ═══
    DIVIDER = "\u2500" * W   # ───

    def _header(title: str) -> None:
        pad = (W - len(title) - 2) // 2
        print(f"\n\u2554{BORDER}\u2557")
        print(f"\u2551{' ' * pad} {title} {' ' * (W - pad - len(title) - 1)}\u2551")
        print(f"\u255a{BORDER}\u255d")

    def _kv(icon: str, label: str, value: str, width: int = 20) -> None:
        print(f"  {icon}  {label:<{width}} {value}")

    def _divider() -> None:
        print(f"  {DIVIDER}")

    _header("GitLab → GitHub Mirror Migration  │  Pre-flight Preview")

    # ---- configuration panel ------------------------------------------
    print()
    if config.dry_run:
        print("  \u26a0\ufe0f  DRY RUN MODE \u2014 no repositories will be created or modified")
        print()

    _kv("\U0001f99a", "Source  (GitLab)",   config.gitlab_url)
    _kv("\U0001f431", "Target  (GitHub)",   config.github_url)
    _kv("\U0001f511", "Auth mode",          config.auth.mode_label)
    _kv("\U0001f9f5", "Concurrent workers", str(config.max_workers))
    _kv("\u23f1\ufe0f",  "Clone timeout",     f"{config.clone_timeout} s")
    _kv("\u23f1\ufe0f",  "Push timeout",      f"{config.push_timeout} s")
    _kv("\U0001f504", "Git retries",        str(config.git_max_retries))
    if config.branch_include:
        _kv("\U0001f33f", "Branch include",  ', '.join(config.branch_include))
    if config.branch_exclude:
        _kv("\u274c",    "Branch exclude",   ', '.join(config.branch_exclude))

    # ---- checkpoint summary -------------------------------------------
    print()
    _divider()
    if already_done:
        _kv("\u2714\ufe0f", "Already done",
            f"{len(already_done)} repo(s) \u2014 will be skipped (checkpoint)")
    _kv("\U0001f4e6", "Pending", f"{len(pending)} repo(s) to migrate")
    _divider()

    if not pending:
        print()
        print("  \u2705  All repositories have already been migrated. Nothing to do.")
        print()
        return True

    # ---- repository preview table -------------------------------------
    show     = pending[:_PREVIEW_MAX_ROWS]
    overflow = len(pending) - len(show)

    HDR_SRC = "\U0001f99a Source (GitLab namespace/project)"
    HDR_TGT = "\U0001f431 Target (GitHub org/repo)"
    HDR_VIS = "\U0001f512 Visibility"
    HDR_BR  = "\U0001f33f Effective Branch Filter"

    # Show the branch column whenever ANY filter is active — either a global
    # default or a per-repo override.  Repos with no per-row values have already
    # had the global patterns merged in during CSV loading, so checking each
    # spec's lists is sufficient.
    any_branch_filter = (
        bool(config.branch_include or config.branch_exclude)
        or any(s.branch_include or s.branch_exclude for s in pending)
    )

    def _fmt_pats(pats: list[str], glyph: str) -> str:
        shown = [p[:17] + "\u2026" if len(p) > 17 else p for p in pats[:2]]
        extra = len(pats) - 2
        return f"{glyph} " + "; ".join(shown) + (f" +{extra}\u2026" if extra > 0 else "")

    def _branch_label(s: RepoSpec) -> str:
        if not s.branch_include and not s.branch_exclude:
            return "\u2714 all"
        src = "\U0001f310" if s.branch_from_global else "\U0001f4cb"
        parts = [
            p for p in [
                _fmt_pats(s.branch_include, "\u2795") if s.branch_include else "",
                _fmt_pats(s.branch_exclude, "\u2796") if s.branch_exclude else "",
            ] if p
        ]
        return f"{src} " + "  ".join(parts)

    # Compute column widths.
    # Source paths can be deep (group/subgroup/nested/project) -- use a generous
    # cap so full paths are visible without hard truncation on normal terminals.
    src_w = min(max(len(HDR_SRC), max(len(f"{s.namespace}/{s.project}") for s in show)), 72)
    tgt_w = min(max(len(HDR_TGT), max(len(f"{s.target_org}/{s.target_name}") for s in show)), 56)
    vis_w = max(len(HDR_VIS), max(len(s.visibility) + 3 for s in show))  # +3 for vis icon

    cols_w = [src_w, tgt_w, vis_w]
    if any_branch_filter:
        cols_w.append(max(len(HDR_BR), max(len(_branch_label(s)) for s in show)))

    def _tsep(top: bool = False, bottom: bool = False) -> str:
        lc, mc, rc = (
            ("\u250c", "\u252c", "\u2510") if top else
            ("\u2514", "\u2534", "\u2518") if bottom else
            ("\u251c", "\u253c", "\u2524")
        )
        return "  " + lc + (mc).join("\u2500" * (w + 2) for w in cols_w) + rc

    def _trow(*cells: str) -> str:
        parts = []
        for cell, w in zip(cells, cols_w):
            c = cell[:w - 3] + "\u2026" if len(cell) > w else cell
            parts.append(f" {c:<{w}} ")
        return "  \u2502" + "\u2502".join(parts) + "\u2502"

    headers = [HDR_SRC, HDR_TGT, HDR_VIS]
    if any_branch_filter:
        headers.append(HDR_BR)

    print()
    print(_tsep(top=True))
    print(_trow(*headers))
    print(_tsep())
    for spec in show:
        cells = [
            f"{spec.namespace}/{spec.project}",
            f"{spec.target_org}/{spec.target_name}",
            f"{_VIS_ICONS.get(spec.visibility, '')} {spec.visibility}",
        ]
        if any_branch_filter:
            cells.append(_branch_label(spec))
        print(_trow(*cells))
    print(_tsep(bottom=True))

    if any_branch_filter:
        print(
            "  \U0001f4cb per-repo (repos.csv)  "
            "\U0001f310 global fallback (mirror-config.json)  "
            "\u2714 all  \u2795 include  \u2796 exclude"
        )

    if overflow:
        print(f"\n  \u2139\ufe0f  ... and {overflow} more repo(s) not shown. See repos.csv for the full list.")

    # ---- confirmation prompt ------------------------------------------
    if not sys.stdin.isatty():
        print("  \U0001f916  Non-interactive mode \u2014 proceeding automatically.")
        print()
        return True

    try:
        prompt = (
            "  \u26a0\ufe0f  Proceed with DRY RUN? [y/N]: "
            if config.dry_run
            else "  \u25b6\ufe0f  Proceed with migration? [y/N]: "
        )
        answer = input(prompt).strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False

    print()
    if answer in ("y", "yes"):
        return True

    return False


# ===========================================================================
# Orchestration
# ===========================================================================

def migrate_all(config: MigrationConfig, config_path: Path, repos_csv_path: Path) -> None:
    """Migrate all repos concurrently with checkpoint, rate limiting, and graceful shutdown."""
    if shutil.which("git") is None:
        raise RuntimeError(
            "'git' is not installed or not on PATH. "
            "Install: https://git-scm.com/downloads"
        )

    # Single shared client and state object -- both are thread-safe.
    client = GitHubClient(
        auth=config.auth,
        api_base=config.github_api_url,
        max_retries=_DEFAULT_API_RETRIES,
    )
    _preflight_checks(config, client)
    state = MigrationState(_CHECKPOINT_FILE)

    # Split repos into already-done (skip) and pending (process).
    already_done: list[RepoSpec] = []
    pending: list[RepoSpec] = []
    for spec in config.repos:
        key = f"{spec.namespace}/{spec.project}"
        (already_done if state.is_succeeded(key) else pending).append(spec)

    skipped_results: list[_RepoResult] = [
        _RepoResult(
            source=f"{s.namespace}/{s.project}",
            target=f"{s.target_org}/{s.target_name}",
            status="skipped",
            default_branch="",
            error="",
            duration_seconds=0.0,
            completed_at=_fmt_ts(),
        )
        for s in already_done
    ]

    if not _print_migration_preview(config, pending, already_done, config_path, repos_csv_path):
        log.info("Migration cancelled at pre-flight preview.")
        return

    total = len(pending)
    if total == 0:
        log.info("All repositories have already been migrated. Nothing to do.")
        _write_reports(skipped_results, _fmt_ts(), config, elapsed_seconds=0.0)
        return

    run_started = _fmt_ts()
    wall_start  = time.monotonic()
    results: list[_RepoResult] = list(skipped_results)
    completed_count = 0

    with ThreadPoolExecutor(
        max_workers=config.max_workers,
        thread_name_prefix="gitmirror",
    ) as pool:
        all_futures: dict[Future, RepoSpec] = {
            pool.submit(_migrate_one, spec, config, client, state): spec
            for spec in pending
        }

        for future in as_completed(all_futures):
            spec = all_futures[future]
            try:
                result = future.result()
            except Exception as exc:
                # Defensive catch -- _migrate_one should never raise, but just in case.
                key = f"{spec.namespace}/{spec.project}"
                state.record(key, "failed")
                result = _RepoResult(
                    source=key,
                    target=f"{spec.target_org}/{spec.target_name}",
                    status="failed",
                    default_branch="",
                    error=str(exc),
                    duration_seconds=0.0,
                    completed_at=_fmt_ts(),
                )

            results.append(result)
            completed_count += 1
            pct = completed_count / total * 100
            elapsed = time.monotonic() - wall_start
            eta_secs = int((total - completed_count) / (completed_count / elapsed)) if completed_count < total and elapsed > 0 else 0
            eta_str = f" | ETA {eta_secs // 60}m {eta_secs % 60:02d}s" if eta_secs else ""
            status_icon = "\u2705" if result.status == "succeeded" else "\u274c"
            branch_info = f" \U0001f33f {result.default_branch}" if result.default_branch else ""
            log.info(
                f"{status_icon} [{completed_count}/{total} {pct:.1f}%{eta_str}] "
                f"{result.source} \u2192 {result.target}{branch_info}"
            )
            if result.status == "failed":
                log.error(f"  \u26a0\ufe0f  {result.error}")

            # Honour shutdown request: cancel any futures that haven't started yet.
            if _shutdown_event.is_set():
                cancelled = sum(1 for f in all_futures if f.cancel())
                log.warning(
                    f"Shutdown: cancelled {cancelled} pending task(s). "
                    "Run the script again to resume from checkpoint."
                )
                break

    # Final summary
    succeeded = [r for r in results if r.status == "succeeded"]
    failed    = [r for r in results if r.status == "failed"]
    skipped   = [r for r in results if r.status == "skipped"]
    elapsed   = time.monotonic() - wall_start

    log.info("\u2550" * 60)
    log.info("\U0001f3c1  Migration Run Complete")
    log.info("\u2500" * 60)
    log.info(f"  \u2705  Succeeded  :  {len(succeeded)}")
    log.info(f"  \u274c  Failed     :  {len(failed)}")
    log.info(f"  \u23ed\ufe0f  Skipped    :  {len(skipped)}  (already done)")
    log.info(f"  \u23f1\ufe0f  Elapsed    :  {int(elapsed // 60)}m {int(elapsed % 60):02d}s")
    log.info("\u2550" * 60)
    if failed:
        log.warning(f"  \u26a0\ufe0f  Failed repos: {', '.join(r.target for r in failed)}")
        log.info("  \U0001f504  Re-run the script to retry failed repositories automatically.")

    _write_reports(results, run_started, config, elapsed_seconds=elapsed)


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    config_path = _SCRIPT_DIR / _CONFIG_FILENAME
    repos_csv_path = _SCRIPT_DIR / _REPOS_CSV_FILENAME

    try:
        migration_config = load_config(config_path, repos_csv_path)
        migrate_all(migration_config, config_path, repos_csv_path)

    except _MissingConfigError as exc:
        log.warning(str(exc))
        sys.exit(1)
    except (ValueError, RuntimeError, FileNotFoundError, KeyError) as exc:
        log.error(str(exc))
        sys.exit(1)
    except Exception as exc:
        log.error(f"Unexpected error: {exc}")
        sys.exit(1)
