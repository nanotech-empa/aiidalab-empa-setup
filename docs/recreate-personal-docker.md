# Set up a new personal AiiDAlab instance

Use a new official full-stack container with its own home, AiiDA profile,
database, repository and broker. `localhost` means Linux inside that container,
not macOS. The native recipes in this candidate target Linux ARM64 only.
Keep existing containers, scientific results and research runtimes intact.

## Select and inspect the base

Select the current supported official
[AiiDAlab full-stack release](https://github.com/aiidalab/aiidalab-docker-stack),
record its immutable image digest and native architecture, and inspect its
Python, AiiDA and AiiDAlab versions before installing apps. The modern setup
requires Python >=3.12 and AiiDAlab >=26.9.0; its approved AiiDA migration
window remains >=2.8,<3. Use the image's compatible interpreter and services.
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

## Install the selected applications

Use AiiDAlab's app manager to install the exact reviewed app/backend revisions.
Record repository, full source commit, installed distribution metadata and
actual import origin separately. Use current canonical app sources; do not
restore old working files, numbered notebooks or private runtime patches.

The supported application set includes ORCA, Surfaces, Nanoribbons, Empa QE
post-processing, PythTB and Model Hamiltonians, with the shared widgets and
backend selections required by those apps. ELN/openBIS have separate modern
sources and immutable legacy releases. Install the appropriate scientific
plugins before registering their Codes. PythTB is an app dependency, not an
executable Code. Do not patch or replace the official QE app to install the
separate Empa QE post-processing app.

Official third-party releases are the normal dependency source. A reviewed
unreleased qualification candidate must be selected explicitly at an immutable
SHA and retain its missing-release item. In particular, unchanged version
strings do not prove that the published SPM or CubeHandler package contains
all selected source changes. Widgets-base remains the selected official release;
review-held upstream patches are not hidden installation prerequisites.

## Install the paired setup candidate

Until acceptance is complete, use the existing [nanotech-empa/aiidalab-empa-setup#14](https://github.com/nanotech-empa/aiidalab-empa-setup/pull/14) and configuration
PR13 candidates together and record both complete commit hashes. The extended
catalog is schema 4: older consumers reject it before Apply.

1. Open the setup app and click **Inspect updates** to obtain the configuration.
2. Select the reviewed configuration branch, inspect again, then choose
   **Personal Docker** and only **mac-localhost**. Leave remote accounts and
   certificates unset.
3. Begin with **local_pythonjob**. Inspect and Apply register the local Computer
   and Code without SSH. The PythonJob plugin must already be installed.
4. Set **Local MPI tasks/machine** and **Local memory [KiB]** to the container's
   actual allocation. Existing computer replacement is opt-in; review the plan.
5. Keep **Install selected localhost runtimes** enabled to test real installation.
   Add BandUPpy, CP2K/SPM, QE, rendering, Block2 and Model runtimes separately.
   Each family runs only its own selected tasks and uses a new versioned prefix.
6. For ORCA, supply the licensed Linux ARM64 archive matching the catalog's
   recorded checksum. The selected task validates it before installing MPI,
   then extracts it and checks numeric-type support. No program archive is
   downloaded or redistributed by the setup app.

The [runtime installation guide](https://github.com/nanotech-empa/aiidalab-alps-files/blob/feature/setup-readiness-checks/docs/runtime-installation.md)
lists exact recipes, paths, prerequisites and verification limits. Paths are
editable; the defaults are new-instance choices, not instructions to retarget
existing provenance-linked Codes.

Installation output is streamed and retained in owned logs. Failed/interrupted
prefixes remain for diagnosis and are not automatically upgraded or deleted.
Review the failure, choose a new prefix where necessary, then inspect again.
To use an already installed compatible runtime, uncheck runtime installation;
executable, dependency and applicable receipt checks still precede registration.
Unselected resources and custom registrations are kept by default.

## Qualify the installation

Run the [localhost-only acceptance checklist](clean-instance-acceptance.md).
Record installation, registration and real tiny execution as separate outcomes.
A path/version check or a prepopulated prefix is not an installation PASS.

Julia uses the backend's committed Project/Manifest and a private depot. The
Model app's supported runtime configuration and fallback depot binding are
prepared for fresh GUI kernels as well as Code wrappers. Block2 uses standard
prefix packaging and the nested metadata required by the ORCA selector.

No remote computers, SSH onboarding, hardware-QPU submission or old research
worker revival belongs to this first empty-instance test. Existing quantum
worker deployment holds remain separate; do not advertise those paths as
installed by these recipes. Preserve the current modern app implementations.

After approved small GUI jobs, verify retrieval, parsing, reopening, package
consistency and startup of this new instance. Merge/release preparation follows
review of the actual fresh-instance evidence; source tests alone do not qualify
native execution or a production release.
