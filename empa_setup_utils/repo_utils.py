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
BRANCH = "integration/surfaces-v2.0.0a0"


def _run_git(args, cwd=GIT_REPO_PATH, check=True):
    """Run a git command and return the completed process."""
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=check,
    )


def clone_repository(branch=BRANCH):
    """Clone the repository if it does not exist."""
    try:
        subprocess.run(
            ["git", "clone", "-b", branch, GIT_URL, GIT_REPO_PATH],
            capture_output=True,
            text=True,
            check=True,
        )
        return True  # Repo was successfully cloned
    except subprocess.CalledProcessError:
        return False  # Failed to clone


def get_latest_remote_commit(branch=BRANCH):
    """Fetch the latest commit hash from the remote repository."""
    try:
        result = _run_git(
            ["ls-remote", GIT_REMOTE, branch],
            cwd=GIT_REPO_PATH,
        )
        return result.stdout.split()[0] if result.stdout else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_local_commit():
    """Get the latest local commit hash."""
    try:
        result = _run_git(["rev-parse", "HEAD"], cwd=GIT_REPO_PATH)
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_current_branch():
    """Return the currently checked out branch in the config repository."""
    try:
        result = _run_git(["branch", "--show-current"], cwd=GIT_REPO_PATH)
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_local_branches():
    """Return local branches available in the config repository."""
    try:
        result = _run_git(
            ["for-each-ref", "--format=%(refname:short)", "refs/heads"],
            cwd=GIT_REPO_PATH,
        )
        return [branch.strip() for branch in result.stdout.splitlines() if branch.strip()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []


def get_remote_branches():
    """Return remote branches available in the config repository."""
    try:
        result = _run_git(["ls-remote", "--heads", GIT_REMOTE], cwd=GIT_REPO_PATH)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []

    branches = []
    for line in result.stdout.splitlines():
        if "refs/heads/" in line:
            branches.append(line.rsplit("refs/heads/", 1)[1].strip())
    return branches


def available_config_branches():
    """Return branches suitable for the developer branch selector."""
    branches = [BRANCH]
    if GIT_REPO_PATH.exists():
        current = get_current_branch()
        branches.extend(branch for branch in [current] if branch)
        branches.extend(get_local_branches())
        branches.extend(get_remote_branches())

    return list(dict.fromkeys(branches))


def repository_has_uncommitted_changes():
    """Return True if the config repository has uncommitted changes."""
    try:
        result = _run_git(["status", "--porcelain"], cwd=GIT_REPO_PATH)
        return bool(result.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def switch_config_branch(branch=BRANCH):
    """Switch the config repository to a local or remote branch."""
    current_branch = get_current_branch()
    if current_branch == branch:
        return True, ""

    if repository_has_uncommitted_changes():
        return (
            False,
            "The config repository has uncommitted changes. Commit or stash them "
            f"before switching from '{current_branch}' to '{branch}'.",
        )

    try:
        if branch in get_local_branches():
            _run_git(["switch", branch], cwd=GIT_REPO_PATH)
            return True, ""
        if branch in get_remote_branches():
            _run_git(["switch", "-c", branch, "--track", f"{GIT_REMOTE}/{branch}"])
            return True, ""
    except subprocess.CalledProcessError as error:
        return False, error.stderr.strip()

    return False, f"Config branch '{branch}' was not found locally or on {GIT_REMOTE}."


def pull_latest_changes(branch=BRANCH):
    """Pull the latest changes from the remote repository."""
    try:
        _run_git(["pull", GIT_REMOTE, branch], cwd=GIT_REPO_PATH)
        return True
    except subprocess.CalledProcessError:
        return False
