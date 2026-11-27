"""Check both provider choices at the maintained bootstrap entry point."""

import hashlib
import importlib.util
import io
import tarfile
from pathlib import Path

import pytest


@pytest.fixture
def installer():
    path = Path(__file__).parents[1] / "scripts/install-empa-apps.py"
    spec = importlib.util.spec_from_file_location("empa_app_bootstrap", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "with_codex,dry_run", [(False, False), (True, False), (True, True)]
)
def test_optional_cli_is_selected_only_for_apply(
    installer, tmp_path, monkeypatch, with_codex, dry_run
):
    monkeypatch.setattr(installer, "checkout", lambda *args: None)
    monkeypatch.setattr(installer.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(installer.subprocess, "run", lambda *args, **kwargs: None)
    installed = []
    monkeypatch.setattr(installer, "install_codex", installed.append)
    installer.install(
        tmp_path / "apps",
        tmp_path / "opt",
        "2.9.4",
        "26.10.0",
        dry_run,
        with_codex=with_codex,
    )
    assert installed == (
        [installer.codex_target("aarch64")] if with_codex and not dry_run else []
    )


@pytest.mark.parametrize(
    "architecture,expected", [("arm64", "aarch64"), ("amd64", "x86_64")]
)
def test_platform_aliases_match_official_archive(installer, architecture, expected):
    name, size, digest = installer.codex_target(architecture)
    assert name == f"codex-package-{expected}-unknown-linux-musl"
    assert size > 0 and len(digest) == 64


@pytest.mark.parametrize("npm", [False, True])
def test_reuse_checks_the_existing_cli_companion(installer, tmp_path, monkeypatch, npm):
    launcher = tmp_path / ("bin/codex.js" if npm else "runtime/bin/codex")
    launcher.parent.mkdir(parents=True)
    launcher.touch(mode=0o755)
    link = tmp_path / "codex"
    link.symlink_to(launcher)
    native = tmp_path / "codex-linux-arm64"
    native_bin = native / "vendor/aarch64-unknown-linux-musl/bin"
    directory = native_bin if npm else launcher.parent
    directory.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(installer.platform, "machine", lambda: "aarch64")

    def inspect(command, **kwargs):
        if command[0] == "node":
            assert npm and command[-2] == "@openai/codex-linux-arm64/package.json"
            return str(native / "package.json")
        assert command == [link, "--version"]
        return "codex-cli 0.171.0-alpha.13.2"

    launched = []
    monkeypatch.setattr(installer.subprocess, "check_output", inspect)
    monkeypatch.setattr(
        installer.subprocess, "run", lambda command, **kw: launched.append(command)
    )
    with pytest.raises(ValueError, match="codex-code-mode-host"):
        installer.probe_codex(link)
    assert launched == []

    (directory / "codex-code-mode-host").touch(mode=0o755)
    assert installer.probe_codex(link) == "codex-cli 0.171.0-alpha.13.2"
    assert launched == [[link, "app-server", "--stdio", "--help"]]


@pytest.mark.parametrize("problem", [None, "digest", "missing-host"])
def test_archive_hash_and_reuse(installer, tmp_path, monkeypatch, problem):
    filename, *_ = installer.codex_target("aarch64")
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        binary = f'#!/bin/sh\nif [ "$1" = --version ]; then echo "codex-cli {installer.CODEX_RELEASE}"; fi\n'.encode()
        for name in (
            "bin/codex",
            "bin/codex-code-mode-host",
            "bin/codex-exec-server",
            "codex-path/rg",
            "codex-resources/bwrap",
        ):
            if problem == "missing-host" and name.endswith("code-mode-host"):
                continue
            member = tarfile.TarInfo(name)
            member.mode = 0o755
            member.size = len(binary)
            archive.addfile(member, io.BytesIO(binary))
    payload = data.getvalue()
    digest = hashlib.sha256(
        b"different" if problem == "digest" else payload
    ).hexdigest()
    dest = tmp_path / ".local/bin/codex"
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(
        installer, "resolve_codex", lambda: str(dest) if dest.is_file() else ""
    )
    urls = []

    def download(url, **kwargs):
        urls.append(url)
        return io.BytesIO(payload)

    monkeypatch.setattr(installer, "urlopen", download)
    if problem is None:
        installer.install_codex((filename, len(payload), digest))
        assert dest.read_bytes() == binary and dest.is_symlink()
        assert dest.resolve().with_name("codex-code-mode-host").read_bytes() == binary
        installer.install_codex((filename, len(payload), digest))
    else:
        with pytest.raises(
            ValueError, match="SHA-256" if problem == "digest" else "code-mode-host"
        ):
            installer.install_codex((filename, len(payload), digest))
        assert not dest.exists()
    assert urls == [
        f"https://github.com/openai/codex/releases/download/rust-v{installer.CODEX_RELEASE}/{filename}.tar.gz"
    ]


def test_existing_unusable_cli_is_preserved(installer, tmp_path, monkeypatch):
    dest = tmp_path / ".local/bin/codex"
    dest.parent.mkdir(parents=True)
    dest.write_text("existing")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(installer, "resolve_codex", lambda: "")
    with pytest.raises(FileExistsError, match="existing Codex path"):
        installer.install_codex(installer.codex_target("aarch64"))
    assert dest.read_text() == "existing"
