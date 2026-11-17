"""Local app discovery and catalog-owned code requirements; no app imports or jobs."""

import configparser
import hashlib
import tomllib
from importlib import metadata
from pathlib import Path

from packaging.utils import canonicalize_name


def discover_applications(config, apps_path=None):
    """Recognize GUI distributions/plugins or actual installed app entry points.

    Backend/plugin entry points alone never imply that a frontend is installed.
    No registry fetch, app import, profile access or executable probe occurs here.
    """
    if apps_path is None:
        from aiidalab.config import AIIDALAB_APPS

        apps_path = AIIDALAB_APPS
    root = Path(apps_path).expanduser()
    candidates = []
    if root.is_dir():
        for path in root.iterdir():
            if path.name.startswith(".") or not any(
                (path / entry).is_file() for entry in ["start.py", "start.ipynb"]
            ):
                continue
            name = None
            try:
                if (path / "pyproject.toml").is_file():
                    name = (
                        tomllib.loads((path / "pyproject.toml").read_text())
                        .get("project", {})
                        .get("name")
                    )
                if name is None and (path / "setup.cfg").is_file():
                    parser = configparser.ConfigParser(interpolation=None)
                    parser.read(path / "setup.cfg")
                    name = parser.get("metadata", "name", fallback=None)
            except (OSError, ValueError, configparser.Error):
                continue
            candidates.append((path, canonicalize_name(name) if name else None))
    installed = {}
    for key, spec in config.get("applications", {}).items():
        matches = {}
        for name in spec.get("distributions", []):
            try:
                matches[canonicalize_name(name)] = metadata.version(name)
            except metadata.PackageNotFoundError:
                pass
        names = {canonicalize_name(name) for name in spec.get("distributions", [])}
        paths = [
            str(path.resolve())
            for path, name in candidates
            if path.name in spec.get("app_ids", []) or name in names
        ]
        if paths or (matches and spec.get("kind") == "plugin"):
            installed[key] = {
                "title": spec["title"],
                "distributions": matches,
                "paths": paths,
            }
    for key in tuple(installed):
        if (
            not set(config["applications"][key].get("requires_applications", []))
            <= installed.keys()
        ):
            installed.pop(key)
    return installed


def codes_for_applications(config, applications, computers):
    """Choose explicit reviewed Code keys, never guesses based on labels/plugins."""
    result = set()
    unknown = set(applications) - config.get("applications", {}).keys()
    if unknown:
        raise ValueError(
            "Unknown application requirements: " + ", ".join(sorted(unknown))
        )
    for app in applications:
        for key in config["applications"][app].get("codes", []):
            if key not in config.get("codes", {}):
                raise ValueError(f"Application {app} refers to unknown code {key}.")
            if config["codes"][key]["computer"] in computers:
                result.add(key)
    return result


def prepare_applications(config, applications=None):
    """Bind a plain discovery snapshot to a rendered automatic setup plan."""
    if not config.get("applications"):
        raise ValueError(
            "This catalog does not define app requirements. Select the matching updated catalog before inspecting."
        )
    config["_installed_applications"] = (
        discover_applications(config) if applications is None else applications
    )
    config["_automatic_codes"] = True
    return config


def find_licensed_orca_archive(config, work_path=None):
    """Find only the exact reviewed vendor archive in the user's Files/Work area."""
    variables = config.get("variables", {})
    if "orca" not in config.get("_installed_applications", {}) or variables.get(
        "local_orca_archive"
    ):
        return
    work = Path(work_path) if work_path is not None else Path.home() / "work"
    expected = variables.get("local_orca_archive_sha256")
    if not expected or not work.is_dir():
        return
    for path in work.iterdir():
        if (
            path.is_symlink()
            or not path.is_file()
            or "orca" not in path.name.lower()
            or not path.name.endswith((".tar.xz", ".tar.bz2", ".tar.gz"))
        ):
            continue
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() == expected:
            variables["local_orca_archive"] = str(path)
            return
