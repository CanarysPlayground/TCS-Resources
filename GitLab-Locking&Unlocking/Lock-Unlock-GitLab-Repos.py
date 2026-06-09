import requests
import sys
import logging
import json
import csv
import argparse
import urllib.parse
from typing import List, Optional, Tuple, Union
from pathlib import Path
import concurrent.futures
from time import sleep
import os

# Enable ANSI escape sequences on Windows
if os.name == 'nt':
    os.system('color')

class ColorFormatter(logging.Formatter):
    """Custom logging formatter with colors."""
    COLORS = {
        logging.DEBUG: '\033[94m',      # Blue
        logging.INFO: '\033[92m',       # Green
        logging.WARNING: '\033[93m',    # Yellow
        logging.ERROR: '\033[91m',      # Red
        logging.CRITICAL: '\033[91m\033[1m' # Bold Red
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        formatter = logging.Formatter(f'%(asctime)s - {color}%(levelname)s{self.RESET} - {color}%(message)s{self.RESET}')
        return formatter.format(record)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Remove any existing handlers to prevent duplicates
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(ColorFormatter())
logger.addHandler(console_handler)

def validate_config(config):
    """Validate the configuration values"""
    required_fields = ['url', 'token', 'per_page', 'verify_ssl']
    for field in required_fields:
        if field not in config:
            raise KeyError(f"Missing required field: {field}")
    
    if not isinstance(config['per_page'], int):
        raise ValueError("per_page must be an integer")
    if not isinstance(config['verify_ssl'], bool):
        raise ValueError("verify_ssl must be a boolean")
    if not config['url'].startswith(('http://', 'https://')):
        raise ValueError("url must start with http:// or https://")
    return True

def load_config():
    """Load configuration from config.json with validation"""
    config_path = Path(__file__).parent / 'config.json'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
    try:
        with open(config_path) as f:
            config_data = json.load(f)
        
        if 'gitlab' not in config_data:
            raise KeyError("Missing 'gitlab' section in config.json")
            
        config = config_data['gitlab']
        validate_config(config)
        return config
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON format in config.json")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"Configuration validation error: {e}")
        raise

# Load configuration with error handling
try:
    config = load_config()
    GITLAB_TOKEN = config['token']
    GITLAB_URL = config['url']
    PER_PAGE = config['per_page']
    VERIFY_SSL = config['verify_ssl']
except Exception as e:
    logger.error(f"Failed to load configuration: {e}")
    sys.exit(1)

def encode_project_id(project_id: Union[int, str]) -> str:
    """URL encode the project ID or path (GitLab API supports both)"""
    return urllib.parse.quote(str(project_id), safe='')

def resolve_project_identifier(identifier: str) -> Optional[int]:
    """Resolve a project name, path, or ID to a numeric project ID."""
    if identifier.isdigit():
        return int(identifier)
        
    if '/' in identifier:
        encoded_path = encode_project_id(identifier)
        url = f"{GITLAB_URL}/api/v4/projects/{encoded_path}"
        headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}
        try:
            response = requests.get(url, headers=headers, verify=VERIFY_SSL)
            if response.status_code == 200:
                return response.json()['id']
            else:
                logger.error(f"Failed to find project with path '{identifier}': {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching project path '{identifier}': {str(e)}")
            return None
            
    # Search by pure project name
    url = f"{GITLAB_URL}/api/v4/projects"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}
    params = {'search': identifier, 'per_page': 100}
    
    try:
        response = requests.get(url, headers=headers, params=params, verify=VERIFY_SSL)
        if response.status_code == 200:
            projects = response.json()
            # Exact match on name or path
            exact_matches = [p for p in projects if p['name'] == identifier or p['path'] == identifier]
            
            if len(exact_matches) == 1:
                return exact_matches[0]['id']
            elif len(exact_matches) > 1:
                logger.error(f"Ambiguous project name '{identifier}': found {len(exact_matches)} matches. Please use the full path (namespace/project) or ID.")
                return None
            else:
                logger.error(f"No exact match found for project name '{identifier}'.")
                return None
        else:
            logger.error(f"Failed to search for project '{identifier}': {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error resolving project '{identifier}': {str(e)}")
        return None

def load_project_ids() -> List[str]:
    """Load project IDs or paths from repos.csv."""
    repos_path = Path(__file__).parent / 'repos.csv'

    if not repos_path.exists():
        raise FileNotFoundError(f"Repository list file not found: {repos_path}")

    project_ids: List[str] = []
    
    possible_headers = ['project_identifier', 'project_id', 'project_name', 'repo_id', 'repo_name', 'project']
    
    with open(repos_path, newline='', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        if not reader.fieldnames:
            raise ValueError("repos.csv is empty")
            
        # Find which header exists in the CSV
        target_header = None
        for header in possible_headers:
            if header in reader.fieldnames:
                target_header = header
                break
                
        if not target_header:
            raise ValueError(f"repos.csv must contain one of these columns: {', '.join(possible_headers)}")

        for row in reader:
            project_id_str = (row.get(target_header) or '').strip()
            if project_id_str:
                project_ids.append(project_id_str)

    if not project_ids:
        raise ValueError("No valid project identifiers found in repos.csv")

    # Preserve order and remove duplicates.
    return list(dict.fromkeys(project_ids))


def get_project_info(project_id: int, identifier: str) -> Optional[dict]:
    """Get project information including default branch."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}

    try:
        response = requests.get(url, headers=headers, verify=VERIFY_SSL)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to fetch project '{identifier}': {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error fetching project '{identifier}': {str(e)}")
        return None


def protect_branch(project_id: int, branch_name: str, identifier: str) -> bool:
    """Protect a branch with no-push access (read-only)."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/protected_branches"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}
    
    payload = {
        'name': branch_name,
        'push_access_level': 0,  # No access for push
        'merge_access_level': 30,  # Maintainer can merge
    }

    try:
        response = requests.post(url, headers=headers, json=payload, verify=VERIFY_SSL)
        
        if response.status_code in (200, 201):
            logger.info(f"Successfully protected branch '{branch_name}' in project '{identifier}'")
            return True
        elif response.status_code == 409:
            logger.info(f"Branch '{branch_name}' is already protected in project '{identifier}'")
            return True
        else:
            logger.error(f"Failed to protect branch '{branch_name}' in project '{identifier}': {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error protecting branch '{branch_name}' in project '{identifier}': {str(e)}")
        return False


def unprotect_branch(project_id: int, branch_name: str, identifier: str) -> bool:
    """Remove protection from a branch."""
    encoded_branch = urllib.parse.quote(branch_name, safe='')
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/protected_branches/{encoded_branch}"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}

    try:
        response = requests.delete(url, headers=headers, verify=VERIFY_SSL)
        
        if response.status_code == 204:
            logger.info(f"Successfully unprotected branch '{branch_name}' in project '{identifier}'")
            return True
        elif response.status_code == 404:
            logger.info(f"Branch '{branch_name}' is not protected in project '{identifier}'")
            return True
        else:
            logger.error(f"Failed to unprotect branch '{branch_name}' in project '{identifier}': {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error unprotecting branch '{branch_name}' in project '{identifier}': {str(e)}")
        return False


def archive_project(project_id: int, identifier: str) -> bool:
    """Archive a project to make it read-only."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/archive"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}

    try:
        response = requests.post(url, headers=headers, verify=VERIFY_SSL)
        if response.status_code in (200, 201):
            logger.info(f"Successfully archived project '{identifier}'")
            return True
        elif response.status_code == 400 and 'already archived' in response.text.lower():
            logger.info(f"Project '{identifier}' is already archived.")
            return True
        elif response.status_code == 403:
            logger.error(f"Permission denied to archive project '{identifier}'")
            return False
        else:
            logger.error(f"Failed to archive project '{identifier}': {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error archiving project '{identifier}': {str(e)}")
        return False


def unarchive_project(project_id: int, identifier: str) -> bool:
    """Unarchive a project to restore read-write access."""
    url = f"{GITLAB_URL}/api/v4/projects/{project_id}/unarchive"
    headers = {'PRIVATE-TOKEN': GITLAB_TOKEN}

    try:
        response = requests.post(url, headers=headers, verify=VERIFY_SSL)
        if response.status_code in (200, 201):
            logger.info(f"Successfully unarchived project '{identifier}'")
            return True
        elif response.status_code == 400 and 'not archived' in response.text.lower():
            logger.info(f"Project '{identifier}' is not archived.")
            return True
        elif response.status_code == 403:
            logger.error(f"Permission denied to unarchive project '{identifier}'")
            return False
        else:
            logger.error(f"Failed to unarchive project '{identifier}': {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error unarchiving project '{identifier}': {str(e)}")
        return False


def perform_project_action(project_id_str: str, action: str) -> bool:
    """Perform the requested action on the project."""
    project_id = resolve_project_identifier(project_id_str)
    if not project_id:
        return False
        
    if action in ('archive', 'unarchive'):
        if action == 'archive':
            return archive_project(project_id, project_id_str)
        else:
            return unarchive_project(project_id, project_id_str)
    
    # For lock/unlock, we need project info
    project = get_project_info(project_id, project_id_str)
    if not project:
        logger.error(f"Could not retrieve project info for project '{project_id_str}'")
        return False

    default_branch = project.get('default_branch')
    if not default_branch:
        logger.error(f"No default branch found for project '{project_id_str}'")
        return False

    logger.info(f"Processing project '{project_id_str}' (branch: '{default_branch}')")

    if action == 'lock':
        return protect_branch(project_id, default_branch, project_id_str)
    elif action == 'unlock':
        return unprotect_branch(project_id, default_branch, project_id_str)
    
    return False


def update_projects_batch(project_ids: List[str], action: str, max_workers: int = 2) -> Tuple[int, int]:
    """Process projects in parallel."""
    success_count = 0
    failure_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for project_id in project_ids:
            future = executor.submit(perform_project_action, project_id, action)
            future.project_id = project_id
            futures.append(future)
            sleep(1)  # Rate limiting

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logger.error(f"Error processing project {future.project_id}: {e}")
                failure_count += 1

    return success_count, failure_count


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments for the action."""
    parser = argparse.ArgumentParser(
        description="Manage GitLab repositories (lock/unlock branches, archive/unarchive projects) from repos.csv"
    )
    parser.add_argument(
        '--action',
        choices=['lock', 'unlock', 'archive', 'unarchive'],
        default='lock',
        help="Operation to perform: 'lock'/'unlock' protects/unprotects default branch, 'archive'/'unarchive' manages project status"
    )
    return parser.parse_args()

def main():
    args = parse_arguments()
    action = args.action
    
    if GITLAB_TOKEN == '' or GITLAB_TOKEN == 'your_access_token':
        logger.error("Please configure your GitLab token in config.json!")
        sys.exit(1)
    
    action_titles = {
        'lock': 'locking', 'unlock': 'unlocking',
        'archive': 'archiving', 'unarchive': 'unarchiving'
    }
    action_title = action_titles[action]
    action_past = {
        'lock': 'locked', 'unlock': 'unlocked',
        'archive': 'archived', 'unarchive': 'unarchived'
    }[action]

    logger.info(f"Starting GitLab project {action_title} process from repos.csv")

    try:
        project_ids = load_project_ids()
    except Exception as e:
        logger.error(f"Failed to load repos.csv: {e}")
        sys.exit(1)

    logger.info(f"Loaded {len(project_ids)} project identifiers")

    # Update projects in parallel
    success_count, failure_count = update_projects_batch(project_ids, action=action)

    logger.info(f"Project {action_title} completed")
    logger.info(f"Successfully {action_past}: {success_count} projects")
    logger.info(f"Failed to update: {failure_count} projects")

if __name__ == "__main__":
    main()
