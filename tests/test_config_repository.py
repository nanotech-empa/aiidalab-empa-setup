import subprocess

import pytest

from empa_setup_utils import control, repo_utils


def git(path, *arguments):
    return subprocess.run(
        ["git", "-C", str(path), *arguments],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.fixture
def repositories(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    source = tmp_path / "source"
    checkout = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "--bare", str(remote)], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "init", "-b", "main", str(source)], check=True, capture_output=True
    )
    for key, value in [("user.name", "Test"), ("user.email", "test@example.org")]:
        git(source, "config", key, value)
    (source / "config.yml").write_text("schema_version: 1\n")
    git(source, "add", "config.yml")
    git(source, "commit", "-m", "Initial configuration")
    git(source, "remote", "add", "origin", str(remote))
    git(source, "push", "-u", "origin", "main")
    subprocess.run(
        ["git", "clone", "-b", "main", str(remote), str(checkout)],
        check=True,
        capture_output=True,
    )
    monkeypatch.setattr(repo_utils, "GIT_REPO_PATH", checkout)
    return source, checkout


def test_switch_to_a_remote_branch_created_after_the_clone(repositories):
    source, checkout = repositories
    git(source, "switch", "-c", "preview/new-code")
    (source / "config.yml").write_text("schema_version: 1\npreview: true\n")
    git(source, "commit", "-am", "Add preview configuration")
    git(source, "push", "origin", "preview/new-code")
    assert repo_utils.switch_config_branch("preview/new-code") == (True, "")
    assert git(checkout, "branch", "--show-current") == "preview/new-code"
    assert (checkout / "config.yml").read_text().endswith("preview: true\n")


def test_dirty_checkout_is_preserved_when_selecting_another_branch(repositories):
    _, checkout = repositories
    (checkout / "config.yml").write_text("uncommitted changes\n")
    success, message = repo_utils.switch_config_branch("other")
    assert not success and "uncommitted" in message
    assert (checkout / "config.yml").read_text() == "uncommitted changes\n"


def test_pull_refuses_to_merge_diverged_configuration(repositories):
    source, checkout = repositories
    for key, value in [("user.name", "Test"), ("user.email", "test@example.org")]:
        git(checkout, "config", key, value)
    (checkout / "local.txt").write_text("local work\n")
    git(checkout, "add", "local.txt")
    git(checkout, "commit", "-m", "Local work")
    before = git(checkout, "rev-parse", "HEAD")
    (source / "remote.txt").write_text("remote work\n")
    git(source, "add", "remote.txt")
    git(source, "commit", "-m", "Remote work")
    git(source, "push", "origin", "main")
    assert not repo_utils.pull_latest_changes("main")
    assert git(checkout, "rev-parse", "HEAD") == before
    assert not git(checkout, "status", "--porcelain")


def test_branch_listing_uses_only_cached_git_state(repositories, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Startup must not query remote branches")

    monkeypatch.setattr(repo_utils, "get_remote_branches", forbidden)
    assert "main" in repo_utils.available_config_branches()


def test_get_config_reads_cache_until_explicit_synchronization(
    repositories, monkeypatch
):
    source, checkout = repositories
    monkeypatch.setattr(control, "GIT_REPO_PATH", checkout)
    before = git(checkout, "rev-parse", "HEAD")
    (source / "config.yml").write_text(
        "schema_version: 1\nvariables: {message: latest}\n"
    )
    git(source, "commit", "-am", "Update configuration")
    git(source, "push", "origin", "main")
    ok, _, cached = control.get_config(file_path=checkout / "config.yml")
    assert ok and "message" not in cached.get("variables", {})
    assert git(checkout, "rev-parse", "HEAD") == before
    ok, message = control.check_repository()
    assert ok, message
    assert git(checkout, "rev-parse", "HEAD") == git(source, "rev-parse", "HEAD")
    ok, _, fresh = control.get_config(file_path=checkout / "config.yml")
    assert ok and fresh["variables"]["message"] == "latest"
    assert "Please apply" not in message


def test_failed_remote_lookup_does_not_allow_cached_inspection(
    repositories, monkeypatch
):
    _, checkout = repositories
    monkeypatch.setattr(control, "GIT_REPO_PATH", checkout)
    monkeypatch.setattr(control, "get_latest_remote_commit", lambda **kwargs: None)
    before = git(checkout, "rev-parse", "HEAD")
    ok, message = control.check_repository()
    assert not ok and "Cannot synchronize" in message
    assert git(checkout, "rev-parse", "HEAD") == before


def test_first_explicit_sync_creates_the_configuration_parent(
    repositories, tmp_path, monkeypatch
):
    source, _ = repositories
    target = tmp_path / "new-home" / "opt" / "configuration"
    monkeypatch.setattr(repo_utils, "GIT_REPO_PATH", target)
    monkeypatch.setattr(control, "GIT_REPO_PATH", target)
    monkeypatch.setattr(
        repo_utils, "GIT_URL", git(source, "remote", "get-url", "origin")
    )
    assert repo_utils.available_config_branches() == ["main"]
    assert not target.parent.exists()
    ok, message = control.check_repository()
    assert ok, message
    assert (target / "config.yml").is_file()
