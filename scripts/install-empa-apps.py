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
import platform
import subprocess
import sys
import tempfile
from contextlib import nullcontext
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

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
        install(apps, support, core, lab, args.dry_run)


def install(apps, support, core, lab, dry_run):
    arch = platform.machine()
    action = "Planning" if dry_run else "Installing"
    print(f"{action} apps in {apps} with {sys.executable} on Linux {arch}", flush=True)

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
