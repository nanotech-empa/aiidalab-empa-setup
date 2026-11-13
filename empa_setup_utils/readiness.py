"""Runtime preflight checks for selected codes; never repair remote permissions."""

import json
import platform
import re
import shlex
import struct
from pathlib import Path

from .aiida_and_ssh_utils import run_command

UENV_GUIDE = "https://docs.cscs.ch/software/uenv/management/#migration-to-ritom"


def remote_command(host, arguments):
    # OpenSSH joins its remaining arguments into a shell command on the remote.
    return run_command(["ssh", host, shlex.join(arguments)])


def code_runtimes(config, code_keys):
    """Return only plain runtime requirements, deduplicated by host/image/view/path."""
    requirements = []
    for key in sorted(code_keys):
        code = config["codes"][key]
        computer = config["computers"][code["computer"]]
        text = code.get("prepend_text", "")
        image = re.search(r"^\s*#SBATCH\s+--uenv(?:=|\s+)(\S+)", text, re.MULTILINE)
        view = re.search(r"^\s*#SBATCH\s+--view(?:=|\s+)(\S+)", text, re.MULTILINE)
        requirement = {
            "host": computer.get("ssh_host", computer["setup"]["hostname"]),
            "transport": computer["setup"].get("transport", "core.ssh"),
            "image": image.group(1) if image else None,
            "view": view.group(1) if view else None,
            "executable": code["filepath_executable"],
            "prepend": text,
            "checks": code.get("runtime_checks", []),
            "manifest": code.get("manifest"),
            "code_key": key,
            "code_config": code,
        }
        requirements.append(requirement)
    return requirements


def report_uenv_failure(host, image, output):
    """Give actionable diagnostics without changing files, ownership or modes."""
    print(f"❌ Runtime check failed on {host} for {image}:\n{output}")
    lower = output.lower()
    if "permission denied" in lower and ("squashfs" in lower or "/ritom/" in lower):
        print(
            "The image is listed but cannot be mounted. On Ritom, an obsolete "
            "group on a parent directory can cause root-squash access failures. "
            "CSCS documents correcting ownership to your current primary group; "
            "uenv 10.2.0 includes a fix. Ask CSCS to check ownership if needed. "
            "The app does not make your scratch directory world-accessible."
        )
    elif "quota" in lower or "no space left" in lower:
        print(
            "The repository may be full or over quota. Free space or contact CSCS, then inspect again."
        )
    elif "squashfs" in lower or "not found" in lower or "inconsistent" in lower:
        print(
            "Check for a missing/corrupt image or incomplete repository migration. "
            "Resume an interrupted migration using the same source and destination. "
            "A work-directory update does not migrate uenv images or old calculations."
        )
    else:
        print(
            "Check the selected uenv version, view, repository and remote executable."
        )
    print(f"CSCS guidance: {UENV_GUIDE}")
    for args in (["uenv", "--version"], ["id"], ["uenv", "repo", "status"]):
        detail, _ = remote_command(host, args)
        print(f"$ {shlex.join(args)}\n{detail}")
    match = re.search(r"(/[^\s:]+/store\.squashfs)", output)
    if match:
        detail, _ = remote_command(host, ["namei", "-l", match.group(1)])
        print(f"Path access diagnostics:\n{detail}")


def manage_uenv_images(requirements):
    """Pull missing selected images and test the mount/view even when already listed.

    A login-node mount check cannot certify Slurm/compute-node launch readiness.
    Accept historical (host, image) pairs as well as full runtime requirements.
    """
    available = {}
    mounted = set()
    for item in requirements:
        if isinstance(item, dict):
            host, image, view, executable = (
                item[name] for name in ("host", "image", "view", "executable")
            )
            local = item["transport"] == "core.local"
            prepend = item.get("prepend", "")
        else:
            host, image = item[:2]
            view, executable = item[2:] if len(item) == 4 else (None, None)
            local, prepend = False, ""

        if local and executable:
            try:
                with Path(executable).open("rb") as stream:
                    header = stream.read(20)
                if header[:4] in {
                    b"\xfe\xed\xfa\xce",
                    b"\xce\xfa\xed\xfe",
                    b"\xfe\xed\xfa\xcf",
                    b"\xcf\xfa\xed\xfe",
                    b"\xca\xfe\xba\xbe",
                }:
                    print(
                        f"❌ {executable} is a macOS executable. Select a native Linux executable inside Docker."
                    )
                    return False
                if header[:4] == b"\x7fELF":
                    endian = "<" if header[5] == 1 else ">"
                    machine = struct.unpack(endian + "H", header[18:20])[0]
                    expected = {"aarch64": 183, "x86_64": 62}.get(platform.machine())
                    if machine != expected:
                        print(
                            f"❌ {executable} has the wrong native architecture for {platform.machine()}."
                        )
                        return False
            except (OSError, IndexError, struct.error) as error:
                print(f"❌ Local executable is unavailable: {executable}: {error}")
                return False
        if image:
            if host not in available:
                status, ok = remote_command(host, ["uenv", "repo", "status"])
                print(f"uenv repository on {host}:\n{status}")
                missing_repo = any(
                    term in status.lower()
                    for term in (
                        "no repository",
                        "not found",
                        "does not exist",
                    )
                )
                if not ok and not missing_repo:
                    report_uenv_failure(host, image, status)
                    return False
                listed, ok = remote_command(host, ["uenv", "image", "ls"])
                if not ok and not missing_repo:
                    report_uenv_failure(host, image, listed)
                    return False
                available[host] = {
                    line.split()[0] for line in listed.splitlines() if line.strip()
                }
            if image not in available[host]:
                # Do not require registry access for an image already installed.
                output, ok = remote_command(host, ["uenv", "image", "pull", image])
                if not ok and not image.startswith("service::"):
                    listing, found = remote_command(
                        host, ["uenv", "image", "find", "service::"]
                    )
                    names = {
                        line.split()[0].removeprefix("service::")
                        for line in listing.splitlines()
                        if line.strip()
                    }
                    if found and image in names:
                        output, ok = remote_command(
                            host, ["uenv", "image", "pull", f"service::{image}"]
                        )
                if not ok:
                    report_uenv_failure(host, image, output)
                    return False
                available[host].add(image)
            launch = ["uenv", "run"]
            if view:
                launch.append(f"--view={view}")
            launch += [image, "--"]
            if (host, image, view) not in mounted:
                output, ok = remote_command(host, [*launch, "/bin/true"])
                if not ok:
                    report_uenv_failure(host, image, output)
                    return False
                mounted.add((host, image, view))
        else:
            launch = []

        def probe(arguments, host=host, local=local, prepend=prepend, launch=launch):
            # Slurm activates the uenv/view before running the Code prepend text.
            body = prepend + "\n" + shlex.join(arguments)
            args = [*launch, "/bin/bash", "-ec", body]
            return run_command(args) if local else remote_command(host, args)

        if executable:
            output, ok = probe(["test", "-x", executable])
            if not ok:
                print(
                    f"❌ Not executable or environment unavailable: {executable} on {host}. {output}"
                )
                if isinstance(item, dict):
                    print(
                        item["code_config"].get(
                            "install_help",
                            "Install the selected runtime, then inspect again.",
                        )
                    )
                return False
        if isinstance(item, dict):
            checks = list(item.get("checks", []))
            manifest_spec = item.get("manifest")
            if manifest_spec:
                checks.append({"arguments": [executable, "manifest"], "manifest": True})
            for check in checks:
                output, ok = probe(check["arguments"])
                if (
                    not ok
                    or check.get("contains", "") not in output
                    or (check.get("not_contains") and check["not_contains"] in output)
                ):
                    print(
                        f"❌ Dependency/capability check failed for {item['code_key']}: {output}"
                    )
                    return False
                if check.get("manifest"):
                    try:
                        manifest = json.loads(output)
                    except (ValueError, TypeError):
                        print("❌ Worker did not emit a valid JSON manifest.")
                        return False
                    if (
                        manifest.get("source_sha256") != manifest_spec["source_sha256"]
                        or manifest.get("protocol") != "quantum-energy/v1"
                    ):
                        print(
                            "❌ Worker source identity/protocol differs from the selected deployment."
                        )
                        return False
                    item["code_config"]["_runtime_manifest"] = manifest
    print(
        "✅ Selected runtime checks passed on login hosts. Compute-node validation remains separate."
    )
    return True
