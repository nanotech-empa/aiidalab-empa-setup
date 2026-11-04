"""Export a recreation inventory; optionally archive app/package source, never AiiDA data.

Run in the instance's normal Python. Do not point a different AiiDA at its profile.
The output is a private operator artifact, not a file to commit or publish.
"""

import argparse
import hashlib
import importlib.metadata as metadata
import io
import json
import platform
import re
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit


def git(root, *arguments):
    return subprocess.check_output(
        ["git", "-C", str(root), *arguments], text=True
    ).strip()


def safe_url(value):
    parsed = urlsplit(value)
    scp = re.fullmatch(r"[^/@:]+@([^:]+):(.+)", value)
    if scp:
        return f"ssh://{scp.group(1)}/{scp.group(2)}"
    if parsed.netloc and parsed.scheme != "file":
        return urlunsplit(
            (parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, "", "")
        )
    return value


def source_snapshot(root, directory):
    """Capture tracked working files plus untracked source, with a content manifest."""
    before = git(root, "diff", "HEAD", "--binary")
    names = git(root, "ls-files", "-z").split("\0")
    names += [
        name
        for name in git(root, "ls-files", "--others", "--exclude-standard", "-z").split(
            "\0"
        )
        if Path(name).suffix in {".py", ".md", ".yml", ".yaml", ".toml", ".cfg"}
    ]
    files = {}
    content = io.BytesIO()
    with tarfile.open(fileobj=content, mode="w:gz") as archive:
        for name in sorted(set(names)):
            if re.search(
                r"(?:^|/)(?:\.env(?:\..*)?|credentials?(?:\..*)?|secrets?(?:\..*)?|id_rsa|id_ed25519)$",
                name,
                re.IGNORECASE,
            ):
                continue
            if not name or re.search(
                r"(?:^|/)(?:\.ipynb_checkpoints|__pycache__)(?:/|$)|-\d+\.ipynb$", name
            ):
                continue
            path = root / name
            if not path.is_file():
                continue
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                raise ValueError(f"Review source symlink before exporting: {path}")
            stat = path.stat()
            blob = path.read_bytes()
            if (stat.st_size, stat.st_mtime_ns) != (
                path.stat().st_size,
                path.stat().st_mtime_ns,
            ):
                raise RuntimeError(f"Source changed during export: {path}")
            files[name] = hashlib.sha256(blob).hexdigest()
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(blob), stat.st_mode & 0o777
            archive.addfile(info, io.BytesIO(blob))
    if git(root, "diff", "HEAD", "--binary") != before:
        raise RuntimeError(f"Source changed during export: {root}")
    for name, digest in files.items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise RuntimeError(f"Source changed during export: {root / name}")
    blob = content.getvalue()
    digest = hashlib.sha256(blob).hexdigest()
    name = f"{root.name}-{digest[:12]}.tar.gz"
    (directory / name).write_bytes(blob)
    return {"archive": name, "sha256": digest, "files": files}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, required=True, help="New private output directory"
    )
    parser.add_argument("--apps-root", type=Path, default=Path("/home/jovyan/apps"))
    parser.add_argument("--source-archives", action="store_true")
    parser.add_argument(
        "--with-codes",
        action="store_true",
        help="Read the currently selected live profile with this interpreter",
    )
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError("Use a new or empty private output directory.")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    args.output.chmod(0o700)
    report = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "executable": sys.executable,
        "architecture": platform.machine(),
        "packages": [],
        "sources": [],
        "excludes": [
            "credentials",
            "AiiDA profile/database/repository",
            "broker state",
            "remote scratch",
            "licensed executables",
            "native runtime prefixes",
        ],
    }
    roots = {path.resolve() for path in args.apps_root.iterdir() if path.is_dir()}
    for distribution in metadata.distributions():
        item = {"name": distribution.metadata["Name"], "version": distribution.version}
        raw = distribution.read_text("direct_url.json")
        if raw:
            direct = json.loads(raw)
            item["source_url"] = safe_url(direct["url"])
            item["vcs_info"] = direct.get("vcs_info", {})
            if (
                direct.get("dir_info", {}).get("editable")
                and urlsplit(direct["url"]).scheme == "file"
            ):
                roots.add(Path(unquote(urlsplit(direct["url"]).path)).resolve())
        report["packages"].append(item)
    for root in sorted(roots):
        if not (root / ".git").exists():
            continue
        remotes = git(root, "remote").splitlines()
        origin = (
            safe_url(git(root, "remote", "get-url", "origin"))
            if "origin" in remotes
            else None
        )
        item = {
            "path": str(root),
            "head": git(root, "rev-parse", "HEAD"),
            "status": git(root, "status", "--porcelain"),
            "origin": origin,
        }
        if args.source_archives:
            item["working_source"] = source_snapshot(root, args.output)
        report["sources"].append(item)
    if args.with_codes:
        from aiida import load_profile, orm

        load_profile()
        report["computers"], report["codes"] = [], []
        for (identifier,) in (
            orm.QueryBuilder().append(orm.Computer, project=["id"]).all()
        ):
            computer = orm.load_computer(identifier)
            report["computers"].append(
                {
                    "label": computer.label,
                    "uuid": str(computer.uuid),
                    "hostname": computer.hostname,
                    "transport": computer.transport_type,
                    "scheduler": computer.scheduler_type,
                    "workdir": computer.get_workdir(),
                }
            )
        for (identifier,) in orm.QueryBuilder().append(orm.Code, project=["id"]).all():
            code = orm.load_node(identifier)
            prepend = code.prepend_text
            if re.search(
                r"token|secret|password|credential|api.?key", prepend, re.IGNORECASE
            ):
                prepend = "REDACTED: review the original runtime environment privately"
            report["codes"].append(
                {
                    "label": code.label,
                    "uuid": str(code.uuid),
                    "computer": code.computer.label if code.computer else None,
                    "executable": str(getattr(code, "filepath_executable", "")),
                    "plugin": code.default_calc_job_plugin,
                    "hidden": code.is_hidden,
                    "prepend": prepend,
                    "with_mpi": getattr(code, "with_mpi", None),
                }
            )
    target = args.output / "instance.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    target.chmod(0o600)
    print(target)


if __name__ == "__main__":
    main()
