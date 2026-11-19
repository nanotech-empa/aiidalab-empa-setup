# Clean-instance acceptance

Use the matching schema5 setup/catalog pair. First test with no scientific app:
no Code/runtime installer should be planned. Install one supported app, inspect
again, and compare the automatically selected requirements to applications.yml.
Then combine apps and verify shared helpers are selected once. A backend alone
must not count as a frontend; a QE property plugin needs its installed QE host.

Normal UI: Localhost/Tigu/Daint only, one Tigu account, no deployment, Code or
runtime-path selector. Localhost is the default. Selecting a remote target reveals
its account/SSH help; there is no SSH requirement for Localhost. Verify each app's
supported target routes, including Tigu PythTB, QE-PP PythonJob and Daint exclusions.

Unknown Codes default to Keep. Explicit hiding must retain records and links;
label-conflicting replacements additionally require explicit hide/rename consent.
Verify that adding/removing an app before or during Apply stops stale registration.
Opening/selector changes never fetch or install. Inspect synchronizes only the
chosen catalog; Apply uses exactly that reviewed revision.


Test the setup/configuration candidate pair in a new independent Linux ARM64
AiiDAlab container. Use the integrated default branches and retain this
fresh-instance acceptance as a separate installation/scientific qualification.
Use the [new-instance runbook](recreate-personal-docker.md); do not copy a live
home, profile, database, repository, broker, secrets or old dirty source tree.

## Baseline and registration

1. Record official image digest, CPU/RAM/shared-memory allocation, architecture,
   Python/AiiDA/AiiDAlab, new profile UUID and independent storage/broker identity.
   Check packages and startup with the image's intended interpreter.
2. Install the selected app/plugin sources with the normal app manager and
   record canonical commits, declarations, installed metadata and import origins.
   Install [nanotech-empa/aiidalab-empa-setup#14](https://github.com/nanotech-empa/aiidalab-empa-setup/pull/14) and [nanotech-empa/aiidalab-alps-files#13](https://github.com/nanotech-empa/aiidalab-alps-files/pull/13) from their integrated default branches and record exact commits. Schema 5
   must be displayed; old consumers must refuse it before any Apply.
3. Opening setup reads the cache only. **Inspect updates** may obtain/synchronize
   the selected configuration; changing the branch alone must not fetch.
4. Install the QE app and QE-PP property plugin, then choose **Localhost**. The PythonJob requirement is detected automatically.
   Leave CSCS credentials unset. Inspect/Apply must use core.local/core.direct
   without SSH or remote installation tasks. Record the new Code/Computer UUIDs.
5. Set the local MPI/memory values to the actual container budget. Verify the
   planned values and stored Computer, and exercise deliberate opt-in replacement.
   Keep a disposable custom Code/Computer by default; test explicit deactivation
   separately without deleting data. Affected unfinished work blocks changes.

## Real installation, one family at a time

Install the corresponding app before Inspect. A preinstalled program
cannot stand in for this part. Record each recipe ID/hash, official artifact
identities, prefix, logs and completed receipt. No task for another family or
remote computer may run.

| Selection | Required observation |
| --- | --- |
| BandUPpy | New isolated worker, exact wheel closure, imports and pip check; register the actual interpreter. |
| CP2K/SPM | Official ARM64 CP2K2026.2, matching MPI, complete basis/potential/D3 data and selected SPM source. Current newer CP2K needs its own native qualification. |
| QE | Install and register QE7.5 through the official QE app, then verify/reuse its native programs and Codes. EMPA setup must not run another localhost QE installer or overwrite those registrations. Preserve the official app source. |
| QE-PP Python | Separate Python3.12 worker with cloudpickle, NumPy/SciPy, pymatgen and scikit-image; verify imports before registering the PythonJob Code. |
| CubeHandler | Current official graphics libraries and reviewed CubeHandler source; CPU Mesa/EGL startup, then a real GUI render. No legacy library-path copying. |
| Block2 | Official0.5.4 cp312 ARM64 wheel, native import and owned launcher; Code extras contain nested block2 version/artifact/launcher hashes and the real GUI selector accepts it. |
| Model | Official Julia1.12.6; unchanged committed Project/Manifest; private instantiated depot; app-owned runtime export, fresh-GUI binding and both local wrapper Codes. |
| ORCA | User-supplied licensed archive SHA/architecture, exact Fortran-enabled MPI4.1.8, clean linked libraries and native numeric-type probe. |
| Native support | Official Bader/Wannier90/SKEAF and source-built Critic2/PPAFM with data/native-library checks. Keep actual native build/parser failures explicit. |
| Quantum | Source-locked modern quantum-energy/v1 SDK worker, manifest/source identity and import checks; no provider credentials, hardware calls or retired research workers. |

PythTB uses the same native CalcJob route for models, selected orbitals and
Wannier on Localhost and Tigu. Install the app first; Localhost Inspect must
select `local_pythtb` and its matching worker task. Verify the worker manifest
reports solve/orbital/wannier capabilities, matching model/postprocessing hashes
and PythTB 2.0.2 before registering the Code. Use the real Review/Submit/Open
route for a small model and saved-model postprocessing without another SCF.
Tigu retains its independent remote installation/acceptance status.
Phonopy and app-owned Python helpers require their supplying plugins/apps and
console commands; an absent command must be diagnosed before registration.

The official QE v26.11.0 CLI currently needs explicit `--computer localhost`
when invoked for local installation/registration; without it the CLI passes
`None`, despite the underlying setup function's localhost default. Use that
supported explicit option. No official QE patch or external PR is required for
this setup path. Do not add a duplicate local QE task to the EMPA catalog.

In **Inspect updates**, the QE app's local Codes must appear as existing
registrations that are kept, with no deactivation checkboxes. With no local QE
Codes, show the optional instruction to install the Quantum ESPRESSO app and
complete its code setup; other EMPA runtime selections remain usable. Check a
partially registered set too. Hidden Codes/disabled Computers must not be
reported as available. Existing QE-PP `python3`/PythonJob and `critic2` helpers
are recognized separately from the QE executables. Inspection alone does not
verify executable versions or authorize any calculation.

## Failure, reuse and preservation

- Try an absent executable/plugin, wrong architecture, corrupted artifact and
  interrupted prefix with disposable fixtures. Require a useful diagnosis and
  no new/replaced Code. Preserve the failed owned prefix/log for review.
- After a reviewed correction/new prefix, Inspect/Apply may retry. A matching
  completed prefix is checked and reused; an unknown prefix is never adopted or
  overwritten silently. Use existing-program mode separately from install proof.
- Change a path, selection, local allocation or installation toggle after Inspect:
  Apply must be invalidated. A changed configuration revision or newly started
  affected work during installation must stop before registration.
- Refresh configuration explicitly. Keep compatible choices; newly discovered
  resources must not become selected automatically. Apply must not fetch.
- Verify existing codes, custom resources, unrelated SSH entries and scientific
  nodes remain intact. No automatic daemon repair or resume of paused work.

## Actual tiny execution and reopening

After exact-source/import/metadata preflight passes, exercise the maintained app
manual cases, one native workflow at a time with the agreed CPU/thread budget.
Start with tiny CP2K and QE cases, a BandUPpy path with meaningful bands/reference,
Block2's analytic fixture, tiny Model ED/DMRG and one shared render. Licensed
ORCA additionally needs serial/parallel PBE0/RIJCOSX and parallel CASCI checks;
a version string or tiny HF alone missed an earlier MPI defect.

Use actual GUI controls, native outputs, resources, retrieval/parsing, downloads
and fresh reopening. Record new fixture UUIDs and original evidence/source
identities. A different fingerprint alone does not require repeating science;
a new native runtime or installation does need its relevant execution evidence.
Keep known numerical/coverage limitations and required future dependency releases.

Finish with package/daemon/startup checks on this new instance. No remote CSCS/
Tigu acceptance, real QPU job or old research continuation is included. The
separate remote checklists remain useful follow-up work and must not be counted
as localhost PASS. Setup and catalog source tests do not replace this test.
