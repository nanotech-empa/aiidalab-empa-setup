#!/usr/bin/env python3
"""Install the EMPA apps in a fresh AiiDAlab container using its notebook Python."""

# Install Quantum ESPRESSO (aiidalab-qe) and CSCS MFA (aiidalab-mfa-cscs)
# separately from the AiiDAlab App Store. Those two apps are manual steps.
# The EMPA BandUPpy frontend is included. The optional QE-PP property can be
# enabled separately in Quantum ESPRESSO if needed.
# Works with the container's Python on Intel/AMD x86_64 and ARM64 Linux.
# Native Localhost runtime recipes in the EMPA catalog currently target ARM64.
# Run as the notebook user. For private repos, configure Git HTTPS access first:
#   gh auth login --hostname github.com --git-protocol https
#   gh auth setup-git

import argparse
import hashlib
import os
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
from contextlib import nullcontext
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from urllib.request import urlopen

# (AiiDAlab launcher directory, GitHub repository, branch)
APPS = [
    ("aiidalab-ai-assistant", "nanotech-empa/aiidalab-ai-assistant", "main"),
    ("aiidalab-eln", "aiidalab/aiidalab-eln", "main"),
    ("aiidalab-openbis", "nanotech-empa/aiidalab-openbis", "master"),
    ("pythtb", "nanotech-empa/aiidalab-tb", "main"),
    ("model-hamiltonians", "nanotech-empa/aiidalab-itensor-spectra", "main"),
    ("orca", "nanotech-empa/aiidalab-orca", "main"),
    ("surfaces", "nanotech-empa/aiidalab-empa-surfaces", "master"),
    ("nanoribbons", "nanotech-empa/aiidalab-empa-nanoribbons", "main"),
    ("qe-postprocess", "nanotech-empa/aiidalab-empa-qe-postprocess", "main"),
    ("empa-setup", "nanotech-empa/aiidalab-empa-setup", "main"),
]

CODEX_RELEASE = "0.171.0-alpha.13"
# Linux packages include the companion executables as well as the CLI.
CODEX_ASSETS = {
    "aarch64": (
        199314530,
        "a1cff9df90784974bed7fe468b83d8ffefd91f6f030efe324a6c8dfb2b5ac0b5",
    ),
    "x86_64": (
        197779171,
        "1887b68a11238013ed357e0c458ae7370c3a406338226fc24810a90e598a1b65",
    ),
}


def codex_target(machine):
    machine = machine.lower()
    machine = {"arm64": "aarch64", "amd64": "x86_64"}.get(machine, machine)
    if machine not in CODEX_ASSETS:
        raise ValueError(
            f"The optional Codex installer supports Linux ARM64 and AMD64, found {machine}."
        )
    return f"codex-package-{machine}-unknown-linux-musl", *CODEX_ASSETS[machine]


def codex_binary_directory(executable):
    resolved = Path(executable).resolve()
    if resolved.name != "codex.js":
        return resolved.parent

    # Follow the official npm launcher's native-package lookup on Linux.
    archive, *_ = codex_target(platform.machine())
    target = archive.removeprefix("codex-package-")
    arch = "arm64" if target.startswith("aarch64-") else "x64"
    try:
        manifest = subprocess.check_output(
            [
                "node",
                "-p",
                "require.resolve(process.argv[1], {paths: [process.argv[2]]})",
                f"@openai/codex-linux-{arch}/package.json",
                str(resolved.parent),
            ],
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=10,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        vendor = resolved.parent.parent / "vendor"
    else:
        vendor = Path(manifest).parent / "vendor"
    return vendor / target / "bin"


def probe_codex(executable):
    description = subprocess.check_output(
        [executable, "--version"], text=True, timeout=10
    ).strip()
    version = re.fullmatch(r"codex-cli (\d+)\.(\d+)\.(\d+)[^\s]*", description)
    if version is None:
        raise ValueError(f"Unexpected CLI at {executable}.")
    if tuple(map(int, version.groups())) >= (0, 171, 0):
        host = codex_binary_directory(executable) / "codex-code-mode-host"
        if not host.is_file() or not os.access(host, os.X_OK):
            raise ValueError(
                f"Missing executable {host.name} in {host.parent}. "
                "Install the matching full official Codex package."
            )
    subprocess.run(
        [executable, "app-server", "--stdio", "--help"],
        stdout=subprocess.DEVNULL,
        check=True,
        timeout=10,
    )
    return description


def resolve_codex():
    # Pip can add a user site or a .pth file. Use a fresh notebook interpreter.
    return subprocess.check_output(
        [
            sys.executable,
            "-c",
            """
from aiidalab_ai_assistant.codex_client import AIServiceError, find_codex

try:
    executable = find_codex()
except AIServiceError as error:
    if "Codex is not installed" not in str(error):
        raise SystemExit(str(error)) from None
else:
    print(executable)
""",
        ],
        text=True,
        timeout=30,
    ).strip()


def install_codex(target):
    if existing := resolve_codex():
        print(f"Reusing {probe_codex(existing)} at {existing}.")
        return

    filename, size, digest = target
    destination = Path.home() / ".local/bin/codex"
    bundle = Path.home() / ".local/share/aiidalab/codex" / f"{filename}-{CODEX_RELEASE}"
    for path in (destination, bundle):
        if path.exists() or path.is_symlink():
            raise FileExistsError(
                f"Review the existing Codex path before installing: {path}"
            )
    destination.parent.mkdir(parents=True, exist_ok=True)
    bundle.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/openai/codex/releases/download/rust-v{CODEX_RELEASE}/{filename}.tar.gz"
    with tempfile.TemporaryDirectory(prefix=".codex-stage-", dir=bundle.parent) as tmp:
        archive = Path(tmp) / "release.tar.gz"
        actual = hashlib.sha256()
        received = 0
        with urlopen(url, timeout=45) as response, archive.open("wb") as output:
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                received += len(chunk)
                if received > size:
                    raise ValueError("Codex archive exceeds the official asset size.")
                actual.update(chunk)
                output.write(chunk)
        if received != size or actual.hexdigest() != digest:
            raise ValueError(
                "Codex archive size or SHA-256 differs from the official release."
            )

        staged = Path(tmp) / "package"
        with tarfile.open(archive, "r:gz") as release:
            release.extractall(staged, filter="data")
        for relative in (
            "bin/codex",
            "bin/codex-code-mode-host",
            "bin/codex-exec-server",
            "codex-path/rg",
            "codex-resources/bwrap",
        ):
            path = staged / relative
            if not path.is_file() or not os.access(path, os.X_OK):
                raise ValueError(
                    f"Official Codex package is missing an executable: {relative}"
                )
        version = probe_codex(staged / "bin/codex")
        if version != f"codex-cli {CODEX_RELEASE}":
            raise ValueError(
                f"Unexpected version in the verified Codex archive: {version}"
            )
        staged.rename(bundle)
        try:
            destination.symlink_to(bundle / "bin/codex")
        except OSError:
            bundle.rename(staged)
            raise
    print(f"Installed {version} at {destination} with its runtime in {bundle}.")


def checkout(path, repo, branch):
    url = f"https://github.com/{repo}.git"
    if path.exists():

        def git_output(*args):
            return subprocess.check_output(
                ["git", "-C", path, *args], text=True
            ).strip()

        if not (path / ".git").exists() or git_output(
            "remote", "get-url", "origin"
        ).removesuffix(".git") != url.removesuffix(".git"):
            sys.exit(f"Existing directory has a different origin: {path}")
        if git_output("branch", "--show-current") != branch or git_output(
            "status", "--porcelain", "--untracked-files=no"
        ):
            sys.exit(f"Expected a clean checkout of {branch} at {path}.")
        subprocess.run(
            ["git", "-C", path, "fetch", "origin", f"refs/heads/{branch}"], check=True
        )
        if subprocess.run(
            ["git", "-C", path, "merge-base", "--is-ancestor", "HEAD", "FETCH_HEAD"]
        ).returncode:
            sys.exit(f"Local commits at {path} need review before updating.")
        subprocess.run(
            ["git", "-C", path, "merge", "--ff-only", "FETCH_HEAD"], check=True
        )
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", "--single-branch", "--branch", branch, url, path],
            check=True,
        )
    sha = subprocess.check_output(
        ["git", "-C", path, "rev-parse", "HEAD"], text=True
    ).strip()
    print(f"{repo}  {branch}  {sha}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="clone in a temporary directory and ask pip to resolve without installing",
    )
    parser.add_argument(
        "--with-codex",
        action="store_true",
        help="reuse or install the full official Codex package for optional ChatGPT sign-in",
    )
    args = parser.parse_args()
    if sys.version_info < (3, 12) or platform.system() != "Linux":
        sys.exit("Run inside the Linux AiiDAlab container with Python 3.12 or newer.")
    try:
        core, lab = version("aiida-core"), version("aiidalab")
    except PackageNotFoundError:
        sys.exit("Use the notebook Python of an existing AiiDAlab full-stack image.")
    from aiidalab.config import AIIDALAB_APPS
    from packaging.version import Version

    if not (Version("2.9.4") <= Version(core) < Version("3")) or Version(lab) < Version(
        "26.9.0"
    ):
        sys.exit(
            f"Need AiiDA >=2.9.4,<3 and AiiDAlab >=26.9.0; found {core} and {lab}."
        )
    staging = (
        tempfile.TemporaryDirectory(prefix="empa-app-plan-")
        if args.dry_run
        else nullcontext()
    )
    with staging as tmp:
        apps = Path(tmp) / "apps" if tmp else Path(AIIDALAB_APPS).expanduser().resolve()
        support = Path(tmp) / "opt" if tmp else Path.home() / "opt"
        install(apps, support, core, lab, args.dry_run, with_codex=args.with_codex)


def install(apps, support, core, lab, dry_run, *, with_codex=False):
    arch = platform.machine()
    action = "Planning" if dry_run else "Installing"
    print(f"{action} apps in {apps} with {sys.executable} on Linux {arch}", flush=True)
    target = codex_target(arch) if with_codex else None
    if target:
        print(
            f"ChatGPT / Codex selected. Reuse a compatible CLI or install {target[0]} from {CODEX_RELEASE}."
        )

    # All top-level checkouts track their upstream main/master. Shared libraries
    # use the versions/commits declared by those apps.
    checkout(support / "aiida-aiidalab-dev", "nanotech-empa/aiida-aiidalab-dev", "main")
    model_backend = support / "aiida-itensor-spin"
    checkout(model_backend, "nanotech-empa/aiida-itensor-spin", "main")
    packages = [str(model_backend)]
    for name, repo, branch in APPS:
        path = apps / name
        checkout(path, repo, branch)
        if name == "orca":
            packages.append(str(path / "backend"))
        packages.append(
            str(path) + ("[unfolding-preview]" if name == "surfaces" else "")
        )

    # Resolve the frontends and backends together, keeping the base platform.
    # pip selects wheels for the running interpreter and CPU (Intel/AMD or ARM).
    pip = [sys.executable, "-m", "pip"]
    flags = ["--dry-run"] if dry_run else []
    subprocess.run(
        [*pip, "install", *flags, f"aiida-core=={core}", f"aiidalab=={lab}", *packages],
        check=True,
    )
    if dry_run:
        print(
            "Dependency resolution completed. Run again without --dry-run to install."
        )
        return
    subprocess.run([*pip, "check"], check=True)
    if target:
        install_codex(target)
        print(
            "In the assistant, choose ChatGPT / Codex and complete Sign in with ChatGPT with your own account."
        )
    print(
        "OpenAI API, Anthropic API and CSCS Inference use their own API keys in the assistant's Connection settings."
    )

    print(
        "Reload the app tabs. If the AiiDA daemon is running, restart it with: verdi daemon restart"
    )
    print(
        "In AiiDAlab Config App, Inspect updates on main fetches nanotech-empa/aiidalab-alps-files."
    )
    if arch.lower() in {"aarch64", "arm64"}:
        print(
            "For local codes, review the Linux ARM64 plan before Apply. ORCA needs your licensed archive."
        )
    else:
        print(
            f"On {arch}, use existing compatible Codes or a configured remote computer."
        )
        print(
            "The catalog's native Localhost recipes target ARM64; leave those recipes unapplied."
        )


if __name__ == "__main__":
    main()
