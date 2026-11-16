"""Verify installer receipts before attaching plain metadata to a new Code."""

import hashlib
import json
import shlex
from copy import deepcopy
from pathlib import Path


def verified_code_metadata(config):
    spec = config.get("runtime_receipt")
    if not spec:
        return {}
    path = Path(spec["path"])
    if path.is_symlink():
        raise ValueError("The runtime receipt is a symlink.")
    receipt = json.loads(path.read_text())
    if (
        receipt.get("status") != "INSTALLED_AND_CHECKED"
        or receipt.get("recipe_sha256") != spec["recipe_sha256"]
        or Path(receipt.get("prefix", "")).resolve() != path.parent.resolve()
    ):
        raise ValueError(
            "Runtime receipt does not match the selected completed recipe."
        )
    executable = Path(config["filepath_executable"]).resolve()
    relative = executable.relative_to(path.parent.resolve()).as_posix()
    expected = receipt.get("verified_files", {}).get(relative)
    with executable.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if not expected or actual != expected:
        raise ValueError("The installed runtime launcher differs from its receipt.")
    extras = receipt.get("code_extras", {})
    for key, fields in spec["extras"].items():
        if not isinstance(fields, dict):
            if type(extras.get(key)) is not type(fields) or extras.get(key) != fields:
                raise ValueError("Runtime capability metadata differs from its recipe.")
            continue
        if any(
            extras.get(key, {}).get(name) != value for name, value in fields.items()
        ):
            raise ValueError(
                "Runtime metadata differs from the selected release artifact."
            )
        if extras[key].get("launcher_sha256") != actual:
            raise ValueError("Runtime metadata has a different launcher hash.")
    return {key: deepcopy(extras[key]) for key in spec["extras"]}


def verified_remote_provider(config, runner):
    """Check an exact remote module receipt again before Code lifecycle writes."""
    spec = config.get("runtime_provider")
    if not spec:
        return {}
    probe = (
        "import hashlib,pathlib,runpy,sys; "
        "p=pathlib.Path(sys.argv.pop(1)); h=sys.argv.pop(1); "
        "assert hashlib.sha256(p.read_bytes()).hexdigest()==h, 'Module checker changed'; "
        "runpy.run_path(str(p),run_name='__main__')"
    )
    arguments = [
        "python3",
        "-c",
        probe,
        spec["checker"],
        spec["checker_sha256"],
        "--root",
        spec["root"],
        "--module-root",
        spec["module_root"],
        "--check-family",
        spec["family"],
    ]
    output, ok = runner(
        [
            "ssh",
            "-oBatchMode=yes",
            "-oStrictHostKeyChecking=yes",
            spec["host"],
            shlex.join(arguments),
        ]
    )
    if not ok:
        raise ValueError(f"Selected remote module is unavailable or changed: {output}")
    # run_command appends SSH stderr after stdout. The established ELA proxy
    # reports its short-lived key activation there even on a successful check.
    # Decode the checker's stdout object; identity fields remain strict below.
    receipt, _ = json.JSONDecoder().raw_decode(output.lstrip())
    if (
        any(
            receipt.get(key) != spec[key]
            for key in [
                "family",
                "prefix",
                "module",
                "module_root",
                "recipe_sha256",
            ]
        )
        or receipt.get("status") != "MODULE_LOAD_VERIFIED"
    ):
        raise ValueError("Remote module receipt differs from the inspected provider.")
    executable = Path(config["filepath_executable"])
    relative = executable.relative_to(Path(spec["prefix"])).as_posix()
    if relative not in receipt.get("executables", {}):
        raise ValueError(
            "The selected executable is not verified by the module receipt."
        )
    extras = receipt.get("code_extras", {})
    for key, expected in spec.get("extras", {}).items():
        actual = extras.get(key)
        if isinstance(expected, dict):
            if (
                not isinstance(actual, dict)
                or any(actual.get(k) != v for k, v in expected.items())
                or not isinstance(actual.get("launcher_sha256"), str)
                or len(actual["launcher_sha256"]) != 64
            ):
                raise ValueError(
                    "Remote native launcher metadata is incomplete or changed."
                )
        elif type(actual) is not type(expected) or actual != expected:
            raise ValueError("Remote runtime capability metadata changed.")
    result = deepcopy(extras)
    result["empa_module_provider"] = {
        key: receipt[key]
        for key in [
            "family",
            "module",
            "prefix",
            "module_root",
            "recipe_sha256",
            "module_sha256",
        ]
    }
    return result
