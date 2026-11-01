import subprocess

import pytest

from empa_setup_utils import repo_utils


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
