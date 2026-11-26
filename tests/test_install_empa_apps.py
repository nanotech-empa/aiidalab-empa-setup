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
    assert name == f"codex-{expected}-unknown-linux-musl"
    assert size > 0 and len(digest) == 64


@pytest.mark.parametrize("valid", [False, True])
def test_archive_hash_and_reuse(installer, tmp_path, monkeypatch, valid):
    filename, *_ = installer.codex_target("aarch64")
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        binary = f'#!/bin/sh\nif [ "$1" = --version ]; then echo "codex-cli {installer.CODEX_RELEASE}"; fi\n'.encode()
        member = tarfile.TarInfo(filename)
        member.size = len(binary)
        archive.addfile(member, io.BytesIO(binary))
    payload = data.getvalue()
    digest = hashlib.sha256(payload if valid else b"different").hexdigest()
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
    if valid:
        installer.install_codex((filename, len(payload), digest))
        assert dest.read_bytes() == binary
        installer.install_codex((filename, len(payload), digest))
    else:
        with pytest.raises(ValueError, match="SHA-256"):
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
