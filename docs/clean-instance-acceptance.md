# Clean-instance localhost acceptance

Test the setup/configuration candidate pair in a new independent Linux ARM64
AiiDAlab container. Keep both PRs unmerged until this acceptance is assessed.
Use the [new-instance runbook](recreate-personal-docker.md); do not copy a live
home, profile, database, repository, broker, secrets or old dirty source tree.

## Baseline and registration

1. Record official image digest, CPU/RAM/shared-memory allocation, architecture,
   Python/AiiDA/AiiDAlab, new profile UUID and independent storage/broker identity.
   Check packages and startup with the image's intended interpreter.
2. Install the selected app/plugin sources with the normal app manager and
   record canonical commits, declarations, installed metadata and import origins.
   Install [nanotech-empa/aiidalab-empa-setup#14](https://github.com/nanotech-empa/aiidalab-empa-setup/pull/14) and [nanotech-empa/aiidalab-alps-files#13](https://github.com/nanotech-empa/aiidalab-alps-files/pull/13) at the reviewed exact heads. Schema 4
   must be displayed; old consumers must refuse it before any Apply.
3. Opening setup reads the cache only. **Inspect updates** may obtain/synchronize
   the selected configuration; changing the branch alone must not fetch.
4. Choose **Personal Docker**, only **mac-localhost**, and **local_pythonjob**.
   Leave CSCS credentials unset. Inspect/Apply must use core.local/core.direct
   without SSH or remote installation tasks. Record the new Code/Computer UUIDs.
5. Set the local MPI/memory values to the actual container budget. Verify the
   planned values and stored Computer, and exercise deliberate opt-in replacement.
   Keep a disposable custom Code/Computer by default; test explicit deactivation
   separately without deleting data. Affected unfinished work blocks changes.

## Real installation, one family at a time

Leave **Install selected localhost runtimes** enabled. A preinstalled program
cannot stand in for this part. Record each recipe ID/hash, official artifact
identities, prefix, logs and completed receipt. No task for another family or
remote computer may run.

| Selection | Required observation |
| --- | --- |
| BandUPpy | New isolated worker, exact wheel closure, imports and pip check; register the actual interpreter. |
| CP2K/SPM | Official ARM64 CP2K2026.2, matching MPI, complete basis/potential/D3 data and selected SPM source. Current newer CP2K needs its own native qualification. |
| QE | Complete selected QE7.5 closure and matching MPI, including pw/pp/projwfc/dos/pw2wannier90; preserve the official QE app source. |
| CubeHandler | Current official graphics libraries and reviewed CubeHandler source; CPU Mesa/EGL startup, then a real GUI render. No legacy library-path copying. |
| Block2 | Official0.5.4 cp312 ARM64 wheel, native import and owned launcher; Code extras contain nested block2 version/artifact/launcher hashes and the real GUI selector accepts it. |
| Model | Official Julia1.12.6; unchanged committed Project/Manifest; private instantiated depot; app-owned runtime export, fresh-GUI binding and both local wrapper Codes. |
| ORCA | User-supplied licensed archive SHA/architecture, exact Fortran-enabled MPI4.1.8, clean linked libraries and native numeric-type probe. |
| Native support | Official Bader/Wannier90/SKEAF and source-built Critic2/PPAFM with data/native-library checks. Keep actual native build/parser failures explicit. |

PythTB uses its app Python dependency, not a fabricated executable Code.
Phonopy and app-owned Python helpers require their supplying plugins/apps and
console commands; an absent command must be diagnosed before registration.
Quantum worker new deployments remain separately deferred.

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
