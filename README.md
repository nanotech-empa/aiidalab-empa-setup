# aiidalab-empa-setup

> **Legacy Python 3.9 / AiiDA 2.6.3:** use [v2025.0214](https://github.com/nanotech-empa/aiidalab-empa-setup/releases/tag/v2025.0214). See the [Empa legacy compatibility guide](https://github.com/nanotech-empa/aiida-nanotech-empa/blob/master/docs/legacy-python39-aiida263.md) for the matching package versions and installation constraints. Use a tagged version to preserve this runtime.

Application to configure computers and codes for Empa nanotech@surfaces lab

## Installation

The modern EMPA application line requires Python >=3.12 and AiiDAlab >=26.9.0.
AiiDA >=2.8,<3 is the explicitly approved migration window; newer application
work targets AiiDA 2.9.2. These are support-policy choices, not claims that
older versions fail. The setup commands create codes non-interactively so that
unattended registration does not stop at CLI prompts.

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

- synchronizing the configuration repository when the user clicks **Inspect updates**;
- resolving widget and variable placeholders from the YAML file;
- checking that the optional YAML `schema_version` is supported;
- comparing the selected configuration with the current AiiDA profile;
- showing the user the required changes before applying them.

Opening the app reads the cached configuration without contacting GitHub. There
is no background configuration polling. **Inspect updates** first synchronizes the
selected ALPS branch, preserves compatible choices and builds a plan from that
revision. A failed sync stops inspection. **Apply updates** uses the inspected
revision without fetching; if another session changes the local Git revision,
reinspection is required. On a new instance, the first Inspect click downloads
the catalog, after which the user chooses resources and inspects again.

Each selected remote computer has an expandable SSH-help section. ALPS explains
MFA CSCS installation and certificate renewal. Tigu explains how the existing
route loads an Empa key from ELA through the forwarded agent, so its private-key
file need not be copied into a new container. For new access, **Prepare new Tigu
key on ELA** creates or reuses a dedicated unencrypted Ed25519 key there, preserving
existing default keys. It displays only the public part in a request to copy and
send to `aliaksandr.yakutovich@empa.ch`. The request explicitly asks to add the key
on both the jump host and Tigu, and to confirm when both are ready. The app sends no
email and cannot grant first access on Tigu. Adding the key there needs an existing
working login or its administrator. After confirmation, inspect/apply and test.
The editable Empa/Tigu username defaults to the CSCS username. The ELA key setting
defaults to blank (existing default-key route); preparation selects the dedicated
path. Any setting change invalidates the inspected plan.
For an existing AiiDA Computer, replacing its saved route still requires explicit
computer updates and review; keeping the existing definition remains the default.
The relevant guide is repeated in the log when that computer's SSH setup fails.

Every configuration change is identified by the Git commit of
`aiidalab-alps-files`; routine edits such as adding a user do not require a
manual config version bump.

The app defaults to the configured branch of `aiidalab-alps-files` (`main`), but
developers can select another local or remote config branch from the app UI to
test unreleased configuration changes.

Most decision logic lives in `empa_setup_utils/control.py`; direct AiiDA,
SSH, and shell command helpers live in `empa_setup_utils/aiida_and_ssh_utils.py`.

## Selective setup and deployment profiles

Choose **CSCS Kubernetes** for remote compute codes, or **Personal Docker** for
optional codes inside the Linux container on a personal computer. The detected
container architecture is displayed; ARM64 native runtimes are offered only on
ARM64. `mac-localhost` is the catalog key for AiiDA's `localhost` Computer.
PythTB remains an app Python dependency and needs no executable Code.

Select computers, their individual accounts, and any subset of codes before
inspection. Unselected resources are kept. Codes/computers absent from the YAML
appear in a keep/deactivate list with **Keep** as the default. Updating an existing
Computer definition is also opt-in; replacement is blocked when it would affect
kept codes. No scientific nodes are deleted.

Schema 2 adds configuration includes and `setup_tasks` scoped to computers/codes.
Partial setup refuses ambiguous legacy command bundles. Apply checks selected
SSH/uenv/runtime dependencies before registering codes, retains unrelated SSH
configuration, and reports failed mounts even for already listed uenv images.
Remote permissions and uenv repository locations are never repaired automatically.

See the [personal Docker recreation runbook](docs/recreate-personal-docker.md),
[clean-instance acceptance plan](docs/clean-instance-acceptance.md), and the
companion [runtime catalog and prerequisites](https://github.com/nanotech-empa/aiidalab-alps-files/blob/feature/setup-readiness-checks/docs/runtime-installation.md).
Quantum-worker updates/new deployments are deferred; preserve their existing
versioned Codes and runtime prefixes.

## License

MIT

## Contact
