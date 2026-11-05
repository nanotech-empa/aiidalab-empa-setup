"""Load a versioned configuration bundle without executing its contents."""

from pathlib import Path

import yaml


def load_config_bundle(file_path):
    path = Path(file_path).resolve()
    data = yaml.safe_load(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("The configuration must be a YAML mapping.")
    for relative in data.pop("includes", []):
        included = (path.parent / relative).resolve()
        if not included.is_relative_to(path.parent):
            raise ValueError(
                "Configuration includes must stay inside the configuration repository."
            )
        extra = yaml.safe_load(included.read_text())
        if not isinstance(extra, dict) or "includes" in extra:
            raise ValueError(
                "Included configuration must be a mapping without nested includes."
            )
        for section, values in extra.items():
            if not isinstance(values, dict) or not isinstance(
                data.setdefault(section, {}), dict
            ):
                raise ValueError(f"Included section {section} must be a mapping.")
            duplicate = data[section].keys() & values.keys()
            if duplicate:
                raise ValueError(
                    f"Duplicate configuration keys in {section}: {sorted(duplicate)}"
                )
            data[section].update(values)
    return data
