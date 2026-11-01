# aiidalab-empa-setup

> **Legacy Python 3.9 / AiiDA 2.6.3:** use [v2025.0214](https://github.com/nanotech-empa/aiidalab-empa-setup/releases/tag/v2025.0214). See the [Empa legacy compatibility guide](https://github.com/nanotech-empa/aiida-nanotech-empa/blob/master/docs/legacy-python39-aiida263.md) for the matching package versions and installation constraints. Use a tagged version to preserve this runtime.

Application to configure computers and codes for Empa nanotech@surfaces lab

## Installation

This Jupyter-based app is intended to be run with [AiiDAlab](https://www.materialscloud.org/aiidalab).

Assuming that the app was registered, you can install it directly via the app store in AiiDAlab or on the command line with:
```
aiidalab install aiidalab-empa-setup
```
Otherwise, you can also install it directly from the repository:
```
aiidalab install aiidalab-empa-setup@git+https://github.com/nanotech-empa/aiidalab-empa-setup
```

## Development notes

The app reads user-facing computer, code, SSH, uenv, and custom-command
configuration from `/home/jovyan/opt/aiidalab-alps-files/config.yml`.
The setup app is responsible for:

- pulling the latest configuration repository;
- resolving widget and variable placeholders from the YAML file;
- checking that the optional YAML `schema_version` is supported;
- comparing the selected configuration with the current AiiDA profile;
- showing the user the required changes before applying them.

Every configuration change is identified automatically by the Git commit of
`aiidalab-alps-files`; routine edits such as adding a user do not require a
manual config version bump.

The app defaults to the configured branch of `aiidalab-alps-files` (`main`), but
developers can select another local or remote config branch from the app UI to
test unreleased configuration changes.

Most decision logic lives in `empa_setup_utils/control.py`; direct AiiDA,
SSH, and shell command helpers live in `empa_setup_utils/aiida_and_ssh_utils.py`.

## License

MIT

## Contact
