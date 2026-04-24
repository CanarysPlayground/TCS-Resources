# GitMirrorMigration.py — Git-Native Code Migration

A standalone script that migrates GitLab repositories to GitHub
using `git clone --mirror` + `git push --mirror`.  

---

## Files at a Glance

| File | Purpose | Edit? |
|---|---|---|
| `GitMirrorMigration.py` | The migration script | Never |
| `mirror-config.json` | Your credentials and settings | **Yes — fill in your values** |
| `repos.csv` | List of repositories to migrate | **Yes — one repo per row** |
| `mirror-migration-<TS>.log` | Generated at runtime — full log | No |
| `mirror-migration-<TS>.json` | Generated at runtime — JSON report | No |
| `mirror-migration-<TS>.csv` | Generated at runtime — CSV report | No |
| `mirror-migration-checkpoint.json` | Resume state across runs | No |

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.8+ | [Download](https://www.python.org/downloads/) — must be on PATH |
| git | [Download](https://git-scm.com/downloads) — must be on PATH |
| PyJWT + cryptography | Only when `auth.mode = "app"`. Install: `pip install PyJWT cryptography` |
| Network access | Machine must reach both GitLab instance and GitHub |

---

## Step-by-Step Setup

### Step 1 — Place the files

Put all files in the **same folder**:

```
my-migration/
  GitMirrorMigration.py
  mirror-config.json
  repos.csv
```

### Step 2 — Fill in `mirror-config.json`

#### `auth` section — choose one mode

**Mode `pat` (default — simple)**

```json
"auth": {
  "mode": "pat",
  "pat": "ghp_xxxxxxxxxxxxxxxxxxxx"
}
```

| Field | What to put here |
|---|---|
| `mode` | `"pat"` |
| `pat` | GitHub Personal Access Token. Create at: GitHub → Settings → Developer settings → Personal access tokens (classic) |

Required PAT scopes: **`repo`** (grants create + push). `read:org` alone is **not sufficient** to create repositories.

---

**Mode `app` (GitHub App — recommended for 1,000+ repos)**

GitHub Apps have a higher API rate limit: **15,000 requests/hour** vs 5,000 for PATs. Tokens auto-rotate every 55 minutes.

```json
"auth": {
  "mode": "app",
  "app": {
    "app_id": 123456,
    "private_key_path": "./my-app.pem",
    "installation_id": 78901234
  }
}
```

| Field | What to put here |
|---|---|
| `app_id` | Numeric App ID. Find it at: GitHub → Settings → Developer settings → GitHub Apps → your app |
| `private_key_path` | Path to the `.pem` file downloaded from your GitHub App settings. Relative paths resolve from the script's directory. |
| `installation_id` | Numeric Installation ID. Find it at: GitHub → Your Org → Settings → Installed GitHub Apps → Configure. The URL ends with `/installations/<ID>` |

Required GitHub App permissions:
- Repository: **Contents** (Read & Write)
- Repository: **Administration** (Read & Write)
- Organization: **Administration** (Read)

> **Switching between PAT and App:** Change only `auth.mode` — no other fields need to change.

---

#### `github` section

```json
"github": {
  "default_org": "your-github-org"
}
```

| Field | Required | What to put here |
|---|---|---|
| `default_org` | Optional | Fallback GitHub organisation used when a row in `repos.csv` has no `target_org`. Leave blank if every row specifies its own `target_org`. |
| `api_url` | Optional (GHES only) | REST API base URL for GitHub Enterprise Server, e.g. `https://ghes.company.com/api/v3` |
| `url` | Optional (GHES only) | Git host base URL for GHES, e.g. `https://ghes.company.com` |

---

#### `gitlab` section

```json
"gitlab": {
  "url": "https://gitlab.com",
  "pat": "glpat-xxxxxxxxxxxxxxxxxxxx"
}
```

| Field | What to put here |
|---|---|
| `url` | Base URL of your GitLab instance. Change to your self-hosted URL if not using GitLab.com. |
| `pat` | GitLab Personal Access Token. Create at: GitLab → User Settings → Access Tokens. Minimum scope: **`read_repository`** |

> **Self-signed TLS certificate?** Run once before the script: `git config --global http.sslCAInfo /path/to/ca-bundle.crt`

---

#### `migration` section (all optional)

```json
"migration": {
  "max_workers": 5,
  "clone_timeout_seconds": 7200,
  "push_timeout_seconds": 7200,
  "git_retry_count": 3
}
```

| Field | Default | Notes |
|---|---|---|
| `max_workers` | `5` | Concurrent repo migrations. Recommended: 5–10. GitHub App supports higher concurrency safely. Do not exceed 20. |
| `clone_timeout_seconds` | `7200` | Per-repo git clone timeout. Increase for very large repos (> 5 GB). |
| `push_timeout_seconds` | `7200` | Per-repo git push timeout. Increase for very large repos (> 5 GB). |
| `git_retry_count` | `3` | How many times to retry a failed clone or push before marking a repo as failed. |

---

### Step 3 — Fill in `repos.csv`

```csv
namespace,project,target_org,target_name,visibility
my-gitlab-group,service-auth,github-org-a,service-auth,private
my-gitlab-group,service-payments,github-org-b,service-payments,private
my-gitlab-group/sub-group,onlinebookstore,github-org-a,onlinebookstore,internal
```

| Column | Required | What to put here |
|---|---|---|
| `namespace` | Yes | GitLab group or sub-group path (as it appears in the GitLab URL) |
| `project` | Yes | GitLab project name (as it appears in the GitLab URL) |
| `target_org` | No* | GitHub organisation to create this repo in. Overrides `github.default_org` per row. |
| `target_name` | Yes | Name the repository will have on GitHub |
| `visibility` | No | `private` (default) · `internal` · `public` |

\* A row with no `target_org` uses `github.default_org` from `mirror-config.json`. If neither is set, the row is skipped with a warning.

> **Multi-org migrations** are fully supported — each row can target a different GitHub org.  
> The source URL is built as: `https://gitlab.com/{namespace}/{project}.git`

---

### Step 4 — Run the script

```bash
python GitMirrorMigration.py
```

The script always finds `mirror-config.json` and `repos.csv` relative to itself, regardless of your current working directory.

---

## What Happens When You Run It

```
Pre-flight
  ├── Verify git is on PATH
  ├── Load mirror-config.json
  ├── Load and validate repos.csv
  └── Load checkpoint file (skip already-succeeded repos)

Per-repo migration (concurrent, max_workers at a time)
  ├── Create GitHub repository (skips creation if it already exists)
  ├── git clone --mirror <gitlab-url>  (all branches, tags, history)
  ├── Detect source default branch (symbolic-ref HEAD)
  ├── git push --mirror <github-url>
  ├── Set GitHub default branch to match source (exact — not hardcoded to "main")
  └── Record result to checkpoint file

Final
  ├── Print summary (Succeeded / Failed / Skipped)
  ├── Write mirror-migration-<TS>.json
  └── Write mirror-migration-<TS>.csv
```

---

## Resuming an Interrupted Run

Simply re-run `python GitMirrorMigration.py`.

- Already-**succeeded** repos are skipped instantly (read from `mirror-migration-checkpoint.json`).
- **Failed** repos are retried automatically.
- The checkpoint file is written atomically after each repo, so a crash or Ctrl+C loses at most one in-flight result.

---

## GitHub Rate Limits

These are handled automatically — no manual intervention needed.

| Limit type | Behaviour |
|---|---|
| Primary rate limit (quota exhausted) | Workers pause until the quota reset time reported by GitHub |
| Secondary rate limit (burst, 403) | Exponential backoff with jitter: 2 s → 4 s → 8 s → … up to 10 min |
| 429 Too Many Requests | Waits the exact `Retry-After` duration from the response header |
| Transient 5xx errors | Up to 5 retries with exponential backoff |
| GitHub App token expiry (1 hr) | Token refreshed automatically 5 minutes before expiry |

Rate limits by auth mode:
- **PAT:** 5,000 REST requests/hour
- **GitHub App:** 15,000 REST requests/hour ← recommended for large fleets

---

## Output Files

All output files are written next to `GitMirrorMigration.py`.

| File | Description |
|---|---|
| `mirror-migration-<TS>.log` | Full timestamped log of every step |
| `mirror-migration-<TS>.json` | Structured JSON report (summary + per-repo detail) |
| `mirror-migration-<TS>.csv` | Spreadsheet-friendly report (opens in Excel) |
| `mirror-migration-checkpoint.json` | Resume state — do not delete mid-migration |

### Example CSV report

```
source,target,status,default_branch,error,duration_seconds,completed_at
my-group/service-auth,org-a/service-auth,succeeded,main,,42.3,2026-04-23T10:05:12
my-group/service-payments,org-b/service-payments,succeeded,master,,38.1,2026-04-23T10:05:51
my-group/legacy-app,org-a/legacy-app,failed,,Push failed: authentication error,0.0,2026-04-23T10:06:02
```

---

## Troubleshooting

| Error / Warning | Likely cause | Fix |
|---|---|---|
| `auth.pat has unfilled value` | PAT not set in config | Add your GitHub PAT to `mirror-config.json` |
| `git clone --mirror` fails | Wrong GitLab URL or PAT expired | Verify `gitlab.url` and `gitlab.pat` in config |
| `git push --mirror` fails | Wrong GitHub PAT or org | Verify `auth.pat` and `target_org` in repos.csv |
| `Failed to create GitHub repo (HTTP 403)` | PAT missing `repo` scope | Regenerate PAT with `repo` scope |
| `Failed to create GitHub repo (HTTP 404)` | Org does not exist or PAT has no access | Check the `target_org` value and PAT org access |
| `no target_org ... skipped` | Row has no org and `default_org` is blank | Set `target_org` in the CSV row or set `github.default_org` in config |
| `GitHub App auth requires PyJWT` | Missing dependency | Run: `pip install PyJWT cryptography` |
| `GitHub App private key not found` | Wrong path in config | Check `auth.app.private_key_path` points to your `.pem` file |
| `git timed out after 7200s` | Very large repository | Increase `clone_timeout_seconds` / `push_timeout_seconds` in `migration` section |
| `Could not determine source default branch` | Detached HEAD or empty repo | Default branch will not be set on GitHub; set it manually |
| `mirror-config.json: invalid JSON` | Syntax error in config | Validate with a JSON linter (e.g. https://jsonlint.com) |

---

## Security Notes

- `mirror-config.json` contains your GitHub and GitLab PATs — **keep it private**.
- Add these files to `.gitignore`:

```
mirror-config.json
repos.csv
*.pem
mirror-migration-*.log
mirror-migration-*.json
mirror-migration-*.csv
mirror-migration-checkpoint.json
```

- Credentials are embedded in HTTPS URLs **in memory only** — never written to disk or printed in logs.
- OS credential managers (Windows Credential Manager, macOS Keychain) are suppressed via `GIT_CONFIG_VALUE_0=credential.helper=` so credentials only come from the config file.
