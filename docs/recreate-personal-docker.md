# Set up a new personal AiiDAlab instance

Install the scientific apps using the script below or AiiDAlab's app manager, then open
**AiiDAlab Config App**. The setup detects their frontend installations and QE
property plugins. Choose Localhost, Tigu and/or Daint and the required accounts;
there is no Code or runtime-path selector. The native Localhost recipes target
Linux ARM64 inside Docker. On ARM64, start with Localhost, set its CPU/memory
budget, then Inspect and review before Apply. On Intel/AMD Linux, use compatible
existing Codes or a configured remote computer.

Only the installed apps' required Codes and scoped supporting recipes are planned.
Install another app and inspect again to add its requirements. Existing matching
programs are verified and reused. Unexpected registrations default to Keep; hide
only explicit choices, with renaming only to free a required replacement label.
All scientific records and provenance remain. A changed app set, catalog revision
or affected active calculation invalidates Apply.

Local QE programs belong to the official Quantum ESPRESSO app. Install that app
and complete its localhost code setup if wanted. EMPA setup preserves those Codes
and does not patch the official app. QE-PP still requires PythonJob/Python Codes.
For ORCA, upload the licensed vendor archive using Files → Work before inspection;
the exact expected archive is recognized by checksum. ORCA is never downloaded
or redistributed. Tigu is the single module-based target, including PythTB;
Daint uses the maintained uenvs and excludes ORCA executables, Gaussian and PythTB.

Use the merged default branches of this app and its paired catalog for the
fresh-instance trial. Source integration and fresh-instance qualification are
separate: preserve native/registration/scientific evidence independently. The maintained deployment/native recipes and administrator
module instructions remain in the paired catalog; this UI does not change their
scientific settings, versions, ABI identities or licensing requirements.


Use a new official full-stack container with its own home, AiiDA profile,
database, repository and broker. `localhost` means Linux inside that container,
not macOS. The native recipes in this candidate target Linux ARM64 only.
Keep existing containers, scientific results and research runtimes intact.

## Select and inspect the base

Select the current supported official
[AiiDAlab full-stack release](https://github.com/aiidalab/aiidalab-docker-stack),
record its immutable image digest and native architecture, and inspect its
Python, AiiDA and AiiDAlab versions before installing apps. The modern setup
requires Python >=3.12, AiiDA >=2.9.4,<3 and AiiDAlab >=26.9.0,
following the approved modern support policy. Use the image's compatible interpreter and services.
Do not upgrade an old shared profile through a different interpreter.

On the Docker host, choose unused container/volume names and a local port.
Set CPU/RAM/shared-memory limits from the available host budget. Use a new home
volume; mount no old home, profile, database, broker, secrets or package cache.
An official image is the base: this procedure does not require building another
image or committing the existing container.

In the new instance record:

```console
uname -m
python --version
python -m pip check
verdi status
verdi profile list
```

Verify the new profile UUID and independent storage/services before proceeding.
Install GitHub CLI and authenticate normally if selected private repositories
need it; never copy a token or SSH directory from another instance.

## Install the full app set from GitHub

Run the [app installer](../scripts/install-empa-apps.py) as the notebook user in the
new container's AiiDAlab terminal. It tracks each app's upstream `main` or `master`,
prints the selected commits, and preserves the installed AiiDA and AiiDAlab versions.
Configure Git access to the private EMPA repositories first.

```bash
gh auth login --hostname github.com --git-protocol https
gh auth setup-git
curl -fL -o install-empa-apps.py https://raw.githubusercontent.com/nanotech-empa/aiidalab-empa-setup/main/scripts/install-empa-apps.py
python install-empa-apps.py --dry-run
```

The optional dry run checks dependency resolution using temporary checkouts.
Choose the installation command for the intended assistant provider.

| Provider | Install command | First use |
| --- | --- | --- |
| OpenAI API, Anthropic API or CSCS Inference | `python install-empa-apps.py` | Select the provider, enter its own API key in **Connection**, and click **Refresh models**. |
| ChatGPT / Codex | `python install-empa-apps.py --with-codex` | Select **ChatGPT / Codex**, open **Connection** and click **Sign in with ChatGPT** to use your own account. |

`--with-codex` checks the CLI found by the assistant. On a new Linux ARM64 or
AMD64 instance, it downloads the matching asset from the
[official Codex release](https://github.com/openai/codex/releases/tag/rust-v0.171.0-alpha.13),
verifies its size and SHA‑256, and installs the executable under `~/.local/bin`.
The assistant finds it there even when the kernel `PATH` omits that directory.
`--dry-run --with-codex` plans the optional installation without downloading the
CLI. On first use the assistant selects an API provider and opens **Connection**
when the CLI is absent. It preserves saved provider choices.

Install **Quantum ESPRESSO** and **CSCS MFA** separately from the AiiDAlab App Store.
The script includes the EMPA BandUPpy app, shared assistant and Config App.
After installation, reopen the app tabs and use **Inspect updates** in the Config App
to fetch the runtime catalog.

## Install selected applications manually

Use AiiDAlab's app manager to install the exact reviewed app/backend revisions.
Record repository, full source commit, installed distribution metadata and
actual import origin separately. Use current canonical app sources; do not
restore old working files, numbered notebooks or private runtime patches.

The supported application set includes ORCA, Surfaces, Nanoribbons, Empa QE
post-processing, PythTB and Model Hamiltonians, with the shared widgets and
backend selections required by those apps. ELN/openBIS have separate modern
sources and immutable legacy releases. Install the appropriate scientific
plugins before registering their Codes. PythTB models, orbitals and Wannier
now use the native `empa_tb.calculate` Code on Localhost or Tigu. The catalog
builds the matching worker from the selected merged app source and checks its
model/postprocessing identities and complete capability set. Do not patch or replace the official QE app to install the
separate Empa QE post-processing app.

Official third-party releases are the normal dependency source. A reviewed
unreleased qualification candidate must be selected explicitly at an immutable
SHA and retain its missing-release item. In particular, unchanged version
strings do not prove that the published SPM or CubeHandler package contains
all selected source changes. Widgets-base remains the selected official release;
review-held upstream patches are not hidden installation prerequisites.

## Install the paired setup and shared assistant

Use `main` from both `nanotech-empa/aiidalab-empa-setup` and
`nanotech-empa/aiidalab-alps-files`; record the complete selected commits.
The merged integration is tracked in
[nanotech-empa/aiidalab-empa-setup#14](https://github.com/nanotech-empa/aiidalab-empa-setup/pull/14)
and [nanotech-empa/aiidalab-alps-files#13](https://github.com/nanotech-empa/aiidalab-alps-files/pull/13).
The catalog is schema 5: older consumers reject it before Apply. In the new
instance terminal, install the setup directly from its merged branch:

```console
aiidalab install aiidalab-empa-setup@git+https://github.com/nanotech-empa/aiidalab-empa-setup@main
```

Open **AiiDAlab Config App**, keep Configuration source on `main`, and use
**Inspect updates** to download the paired catalog. No private working-directory
snapshot is needed.

For the shared chat, authenticate the new instance's GitHub CLI normally, then
install the shared component before the app bindings. Do not copy credentials
from an old container:

```console
gh auth login --hostname github.com --git-protocol https
gh auth setup-git
python -m pip install 'git+https://github.com/nanotech-empa/aiidalab-ai-assistant.git@main'
```

For a manual installation, follow the assistant's
[provider setup](https://github.com/nanotech-empa/aiidalab-ai-assistant#provider-setup-on-a-fresh-instance).

Open the chat in a scientific app to edit its draft, or use the standalone
AiiDAlab AI Support launcher. From either chat, ask to rerun a stored AiiDA
workflow using another stored molecule and give both PKs or UUIDs. Review the
displayed input changes, then confirm to receive the new process identifier.
To continue a CP2K NEB from its saved band, give the parent PK or UUID in Home.
Review the optimized replicas, replica grouping and scheduler allocation before
confirming each continuation. For an editable draft, ask to open the Surfaces NEB form.
In On-surface chemistry the chat reports the effective CP2K boundary settings
and can apply the proposal on request. For Daint resource advice, specify the
configured CP2K Code so the chat can inspect the site's launch settings. The private repository requires authorized
account access. Install the scientific apps through the ordinary app manager,
using their default branches and recording resolved commits. ORCA and PythTB
already include the matching editor bindings; Surfaces/Nanoribbons use the same
shared structure-manager tools. Native PythTB's unified-route merge is
`902d3b00023f6d21a09c261cccc65bd72aaaff39`; its native openBIS export is on master
at `4e3b0c57cae6cf4d6294a53cc303230ac0bf890f`.
For a Localhost PythTB installation, open the app and select it in Setup.
Inspect and Apply build the worker from the tracked app source. A numbered
notebook copy can stay open during installation. After registration, select
the PythTB Code in the app, review and submit a model, then open its result UUID.
Orbitals and Wannier calculations can use that saved model.
XPS research branches are outside this installation.

1. Install the intended scientific app through the ordinary AiiDAlab app manager.
2. Open setup, select the reviewed catalog under Configuration source, and keep
   Localhost selected. Deployment and code requirements are determined automatically.
3. Inspect. Only that app's required programs/registrations may be planned; missing
   backend plugins are reported, not replaced with fake Codes. No SSH is used locally.
4. Set the local CPU/memory budget and review the plan. Existing computer replacement
   is opt-in; unknown Codes default to Keep. Apply only after deliberate review.
5. Verify real scoped installation, native checks and registration independently.
   Add further apps and repeat Inspect; shared helpers must not be duplicated.
6. For ORCA, upload your licensed matching Linux ARM64 archive using Files → Work.
   Its checksum is verified before use; the archive is never downloaded/redistributed.

The [runtime installation guide](https://github.com/nanotech-empa/aiidalab-alps-files/blob/main/docs/runtime-installation.md)
lists exact recipes, internal paths, prerequisites and verification limits. Paths
are catalog-owned; existing provenance-linked Codes are never silently retargeted.

Installation output is streamed and retained in owned logs. Failed/interrupted
prefixes remain for diagnosis and are not automatically upgraded or deleted.
Review and diagnose a failure before retrying; never overwrite an incomplete or
unowned prefix. Existing compatible runtimes still require executable, dependency
and applicable receipt checks before registration.
Unselected resources and custom registrations are kept by default.

## Qualify the installation

Run the [localhost-only acceptance checklist](clean-instance-acceptance.md).
Record installation, registration and real tiny execution as separate outcomes.
A path/version check or a prepopulated prefix is not an installation PASS.

Julia uses the backend's committed Project/Manifest and a private depot. The
Model app's supported runtime configuration and fallback depot binding are
prepared for fresh GUI kernels as well as Code wrappers. Block2 uses standard
prefix packaging and the nested metadata required by the ORCA selector.

The localhost-only phase does not configure remote computers or submit hardware
QPU jobs. The selected modern `quantum-energy/v1` worker has its own explicit
installation task and source-manifest check; it does not restore retired or
paused research workers. Tigu and Daint installation/qualification are separate
phases. Preserve the current modern app implementations and old Codes.

After approved small GUI jobs, verify retrieval, parsing, reopening, package
consistency and startup of this new instance. Record fresh-instance acceptance
before claiming production/release qualification; source merges and source tests
alone do not qualify the newly installed native execution path.
