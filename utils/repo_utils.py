import subprocess
from pathlib import Path
# labels for paths
repo_name = "aiidalab-alps-files"
home_dir = Path("/home/jovyan")  # Explicitly set /home/jovyan
target_dir = home_dir / "opt"
config_files = target_dir / repo_name  # Ensure `repo_name` is defined
config_path = home_dir / ".ssh" 
configuration_file = config_files / "config.yml"
GIT_REPO_PATH = config_files
GIT_URL = "https://github.com/nanotech-empa/aiidalab-alps-files.git"  # files needed on daint
GIT_REMOTE = "origin"
BRANCH = "main"
def clone_repository():
    """Clone the repository if it does not exist."""
    try:
        #print("🔄 Cloning repository...")
        result = subprocess.run(
            ["git", "clone", "-b", BRANCH, GIT_URL, GIT_REPO_PATH],
            capture_output=True,
            text=True,
            check=True
        )
        return True  # Repo was successfully cloned
    except subprocess.CalledProcessError:
        return False  # Failed to clone

def get_latest_remote_commit():
    """Fetch the latest commit hash from the remote repository."""
    try:
        result = subprocess.run(
            ["git", "ls-remote", GIT_REMOTE, BRANCH],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.split()[0] if result.stdout else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def get_local_commit():
    """Get the latest local commit hash."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def pull_latest_changes():
    """Pull the latest changes from the remote repository."""
    try:
        result = subprocess.run(
            ["git", "pull", GIT_REMOTE, BRANCH],
            cwd=GIT_REPO_PATH,
            capture_output=True,
            text=True,
            check=True
        )
        return "Already up to date" not in result.stdout
    except subprocess.CalledProcessError:
        return False