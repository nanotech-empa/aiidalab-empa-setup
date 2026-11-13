import hashlib
import subprocess
import tarfile

from scripts.snapshot_instance import safe_url, source_snapshot


def test_snapshot_preserves_actual_dirty_source_without_generated_notebooks_or_env(
    tmp_path,
):
    root = tmp_path / "app"
    root.mkdir()

    def git(*args):
        subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)

    git("init")
    (root / "start.py").write_text("original = True\n")
    (root / ".env").write_text("TOKEN=not-to-export\n")
    (root / "start-1.ipynb").write_text("generated")
    git("add", ".")
    git(
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.org",
        "commit",
        "-m",
        "initial",
    )
    (root / "start.py").write_text("corrected = True\n")
    (root / "new_module.py").write_text("new = True\n")
    output = tmp_path / "output"
    output.mkdir()
    report = source_snapshot(root, output)
    archive_path = output / report["archive"]
    assert hashlib.sha256(archive_path.read_bytes()).hexdigest() == report["sha256"]
    with tarfile.open(archive_path) as archive:
        assert archive.getnames() == ["new_module.py", "start.py"]
        assert archive.extractfile("start.py").read() == b"corrected = True\n"


def test_source_urls_never_record_authentication_tokens():
    assert (
        safe_url("https://user:secret@github.com/owner/repo?token=secret")
        == "https://github.com/owner/repo"
    )
    assert (
        safe_url("git+https://secret@github.com/owner/repo")
        == "git+https://github.com/owner/repo"
    )
    assert (
        safe_url("git@github.com:owner/repo.git") == "ssh://github.com/owner/repo.git"
    )
