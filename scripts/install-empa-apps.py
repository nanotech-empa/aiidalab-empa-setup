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
import platform
import shutil
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
# Pin the official asset digest and byte count before downloading or extracting.
CODEX_ASSETS = {
    "aarch64": (
        124423726,
        "6a1c5450cda493f3d316e7ffa6de98d24e9f03992efa6fd8884c057c0b13a4f0",
    ),
    "x86_64": (
        122702168,
        "2cb3e32221868bcb8a422d28a3a06f1c60a40023d6e90e1c2c8d97346818bf3a",
    ),
}


def codex_target(machine):
    machine = machine.lower()
    machine = {"arm64": "aarch64", "amd64": "x86_64"}.get(machine, machine)
    if machine not in CODEX_ASSETS:
        raise ValueError(
            f"The optional Codex installer supports Linux ARM64 and AMD64, found {machine}."
        )
    return f"codex-{machine}-unknown-linux-musl", *CODEX_ASSETS[machine]


def probe_codex(executable):
    description = subprocess.check_output(
        [executable, "--version"], text=True, timeout=10
    ).strip()
    if not description.startswith("codex-cli "):
        raise ValueError(f"Unexpected CLI at {executable}.")
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
    print(find_codex())
except AIServiceError:
    pass
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
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(
            f"Review the existing Codex path before installing: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://github.com/openai/codex/releases/download/rust-v{CODEX_RELEASE}/{filename}.tar.gz"
    with tempfile.TemporaryDirectory(
        prefix=".codex-stage-", dir=destination.parent
    ) as tmp:
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
        staged = Path(tmp) / "codex"
        with tarfile.open(archive, "r:gz") as release:
            member = release.getmember(filename)
            if not member.isfile():
                raise ValueError(
                    "Expected a regular Codex executable in the release archive."
                )
            with release.extractfile(member) as source, staged.open("wb") as output:
                shutil.copyfileobj(source, output)
        staged.chmod(0o755)
        version = probe_codex(staged)
        if version != f"codex-cli {CODEX_RELEASE}":
            raise ValueError(
                f"Unexpected version in the verified Codex archive: {version}"
            )
        destination.hardlink_to(staged)
    print(f"Installed {version} at {destination}.")


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
        help="reuse or install the official Codex CLI for optional ChatGPT sign-in",
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
