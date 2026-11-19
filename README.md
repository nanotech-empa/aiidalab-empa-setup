# aiidalab-empa-setup

> **Legacy Python 3.9 / AiiDA 2.6.3:** use [v2025.0214](https://github.com/nanotech-empa/aiidalab-empa-setup/releases/tag/v2025.0214). See the [Empa legacy compatibility guide](https://github.com/nanotech-empa/aiida-nanotech-empa/blob/master/docs/legacy-python39-aiida263.md) for the matching package versions and installation constraints. Use a tagged version to preserve this runtime.

Application to configure computers and codes for Empa nanotech@surfaces lab

## Installation

The modern EMPA application line requires Python >=3.12 and AiiDAlab >=26.9.0.
AiiDA >=2.9.4,<3 is the approved modern support floor. These are support-policy choices, not claims that
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

## App-aware setup

Install the scientific apps you want to use, then open this setup app. It detects
actual app entry points and installed QE property plugins; backend packages alone
do not select an app. Choose **Localhost**, **Tigu** and/or **Daint**, and the needed
accounts. **Inspect updates** shows a plan for those apps; **Apply updates** runs
only their supporting recipes, verifies them and registers their required Codes.
There is no manual Code selector or runtime-path form. Docker/Kubernetes is
detected automatically. Existing complete runtimes are checked and reused.

Tigu appears once, using the maintained module route. Shared native modules and
the existing personal-module fallback use the same reviewed recipes. The old
Tigu and Eiger definitions remain retained in the catalog/profile for historical
calculations, but are not additional targets in the normal interface.

The catalog explicitly maps each supported app to its Code keys. Shared helpers
are deduplicated. QE-PP retains its PythonJob worker; PythTB has a Tigu native
route and the same source-matching native Code on localhost, including orbitals
and Wannier. The official QE app
owns local QE installation/registration. Existing local QE Codes are preserved;
missing ones produce optional QE-app setup guidance, never a second QE installer.
No ORCA executable, Gaussian or PythTB is installed on Daint; Gaussian is Tigu-only.

Unexpected registrations are listed separately with **Keep** as the default.
Check a Code to hide it; renaming is done only when its label must be freed for
a required replacement. Incompatible existing registrations cannot be retired
without that explicit choice. UUIDs, provenance links and scientific records are
retained. Computer replacement remains opt-in and is blocked if it would affect
kept Codes. A changed app set or catalog invalidates the reviewed plan.

For a new local ORCA installation, supply your licensed reviewed vendor archive
through **Files → Work**, then inspect again. The app finds the exact archive by
checksum; it never downloads or distributes ORCA. Technical recipe paths remain
catalog-owned. CPU/memory limits and the meaningful Tigu username remain user
settings. **Configuration source** is available in a collapsed section for preview
review; opening the app uses the cached branch without fetching.

Schema5 introduces this app-owned requirement mapping and single-target UI.
Older setup consumers refuse schema5 before Apply. Install the matching setup
and catalog together. The lower-level explicit selection API remains available
for maintainers and isolated tests; it is not a normal user-facing Code selector.

Schema 2 adds configuration includes and `setup_tasks` scoped to computers/codes.
Partial setup refuses ambiguous legacy command bundles. Apply checks selected
SSH/uenv/runtime dependencies before registering codes, retains unrelated SSH
configuration, and reports failed mounts even for already listed uenv images.
Remote permissions and uenv repository locations are never repaired automatically.

See the [personal Docker recreation runbook](docs/recreate-personal-docker.md),
[clean-instance acceptance plan](docs/clean-instance-acceptance.md), and the
companion [runtime catalog and prerequisites](https://github.com/nanotech-empa/aiidalab-alps-files/blob/main/docs/runtime-installation.md).
The selected modern quantum worker remains included for the ORCA app. Older
experimental/research-worker changes remain deferred; preserve their original
versioned Codes, checkpoints and runtime prefixes.

## License

MIT

## Contact

## Software attribution and scientific acknowledgements

Please credit [aiidalab-empa-setup](https://github.com/nanotech-empa/aiidalab-empa-setup) and its
[contributors](https://github.com/nanotech-empa/aiidalab-empa-setup/graphs/contributors),
and identify the release or immutable commit used. Existing author,
contributor and funding acknowledgements remain authoritative.

See [CITATION.cff](CITATION.cff) for the software citation and identify the
exact release or commit used.

Preserve the copyright and permission notices in [LICENSE](LICENSE)
when copying or distributing copies or substantial portions of MIT-licensed
code. Separately licensed components retain their own notices. A name or
repository link does not replace the applicable licence notices.

For research carried out using this environment or the resulting workflows,
acknowledge [AiiDA](https://github.com/nanotech-empa/.github/blob/main/CITATION_GUIDANCE.md#aiida) and [AiiDAlab](https://github.com/nanotech-empa/.github/blob/main/CITATION_GUIDANCE.md#aiidalab)
when each platform was actually used, following their respective citation
guidance. Cite the scientific software and methods actually used as well.

Credit the configuration app and any setup scripts or external tools reused; acknowledge the platforms used for the resulting research separately.

The [shared credit guide](https://github.com/nanotech-empa/.github/blob/main/CITATION_GUIDANCE.md) keeps software attribution, licence
compliance and scientific citation requests separate. Scientific citation
requests are not additional MIT licence conditions.
