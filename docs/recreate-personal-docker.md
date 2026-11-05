# Recreate an Empa AiiDAlab instance on a personal computer

This is an agent runbook for a **new** Docker instance. On a Mac, `localhost`
means Linux inside the container. Native ARM64 executables belong to an ARM64
container; macOS executables and x86-64 Linux installations are different runtimes.
Use the **Personal Docker** setup profile. The **CSCS Kubernetes** profile keeps
compute codes remote. PythTB is an application Python dependency in both profiles.

## Record the source instance

Use its normal interpreter, before changing any packages:

```bash
python scripts/snapshot_instance.py \
  --output /home/jovyan/recreation-bundle --source-archives --with-codes
python -m pip check
```

Run the script from this setup checkout. Export source from **each** source
instance if consolidating the ORCA and ITensor instances. The report records
packages, editable sources, Git heads, dirty state, Computer/Code identities and
working-source archive hashes. A dirty checkout's HEAD alone cannot reproduce
its application. Numbered notebook copies are excluded. Keep the bundle private
and review it before sharing: source and runtime paths can contain private project
information. The script does not export the AiiDA database, binary repository,
broker, credentials, licensed programs or native runtime prefixes.

On the Mac, record the actual source container and image separately:

```bash
docker inspect --format='{{.Name}} {{.Image}}' EXISTING_CONTAINER
docker image inspect --format='{{.Id}} {{.Os}}/{{.Architecture}} {{json .RepoDigests}}' EXISTING_IMAGE_ID
docker cp EXISTING_CONTAINER:/home/jovyan/recreation-bundle ./recreation-bundle
```

Substitute the inspected names/IDs. Do not copy the source instance's `.aiida`,
PostgreSQL or RabbitMQ directories into a fresh profile. A scientific-data
migration needs a separate database/repository backup and restore procedure.

## Start a new persistent instance

Use an [official full-stack image](https://github.com/aiidalab/aiidalab-docker-stack#quickstart),
or the already qualified image, by **digest**, with the native architecture.
[AiiDAlab Launch](https://github.com/aiidalab/aiidalab-launch) is the upstream
production launcher; direct Docker is also suitable for a controlled recreation.
Inspect the candidate image without mounting any old home directory and check
its Python/AiiDA/AiiDAlab versions before choosing it. The modern target is
Python >=3.12, AiiDAlab >=26.9.0 and AiiDA >=2.9.2; the Empa CP2K/setup migration
retains its approved `aiida-core>=2.8,<3` window. The source ORCA instance observed
on November 4 has AiiDAlab 26.6.0 and is **not** evidence for the newer platform.

Example on an Apple Silicon Mac after setting `EMPA_IMAGE` to that inspected
`aiidalab/full-stack@sha256:...` image:

```bash
docker volume create empa_personal_new_home
docker run -d --name empa-personal-new \
  --platform linux/arm64 --cpus=8 --memory=32g --shm-size=2g \
  -p 127.0.0.1:8889:8888 \
  --mount type=volume,src=empa_personal_new_home,dst=/home/jovyan \
  "$EMPA_IMAGE"
```

Choose new container/volume names and available CPU/RAM/port budgets. On Intel,
use `linux/amd64` and rebuild native runtimes for that architecture. Keep the
existing container/volume intact. Do not mount the same AiiDA home concurrently
in two independent instances. Obtain the new login URL from Docker's logs and
keep its authentication token private.

In the new instance:

```bash
uname -m
python --version
python -m pip check
verdi status
verdi profile list
```

Confirm that image startup and notebook/daemon interpreters agree on AiiDA and
configuration-schema compatibility. An import succeeding in one kernel does not
establish restart compatibility. Use a fresh image-created profile; do not
upgrade an old image's AiiDA blindly through user-site packages.

## Restore applications and agent context

1. Install `gh` using the image's package manager if absent. Have the user run
   `gh auth login` (GitHub.com/HTTPS), then require `gh auth status`. Never print
   or transfer its token. Private ITensor and living-memory access depends on it.
2. Read the current `aiida-aiidalab-dev` skill and live-memory handoffs. The
   November 4 Empa merges supersede the older Surfaces alpha pins. ORCA's current
   electronic/quantum GUI remains a preview assembled from canonical drafts;
   current `main` alone does not contain all visible features.
3. Copy the private source bundle into the new instance. Verify each archive's
   SHA-256 against `instance.json`. Extract each into its own directory under
   `/home/jovyan/opt`, using Python's `tarfile` data filter or equivalent safe
   extraction. Preserve licences and source manifests. A working-source snapshot
   is an installation artifact; it does not create or merge an integration branch.
4. For clean upstream installations, use the exact repository commits from the
   verified handoff/report. For a dirty preview, install its verified working-source
   archive. Never replace it with an old HEAD and assume the features survived.
5. Preview the dependency resolution for the chosen source stack before installing.
   Use the real instance environment, then `python -m pip check`. Keep package
   isolation for external workers separate from the AiiDA app environment. Every
   alternate interpreter/test must receive its own command-scoped `AIIDA_PATH`.

Install the relevant app sources through `aiidalab install` using their immutable
Git revisions, or install the exported sources and link only their canonical app
roots under `/home/jovyan/apps`. Keep runtime prefixes, logs, inventories and build
directories under `opt`/a private work directory, outside app discovery.

The application set to reconcile is Surfaces, Nanoribbons, Empa QE post-processing
(independent of the main QE app), PythTB, ORCA, ITensor/model Hamiltonians, shared
widgets and CubeHandler, plus the existing ELN/openBIS apps. ELN/openBIS
modernization remains held for Fabio; source recovery does not lift that hold.

PythTB installs its `pythtb>=2.0` dependency with the app. It needs no AiiDA
Computer/Code or Slurm worker. A source-pinned installation looks like:

```bash
aiidalab install --yes 'pythtb@git+https://github.com/nanotech-empa/aiidalab-tb.git@VERIFIED_FULL_COMMIT'
```

Resolve that commit from the current maintained handoff; `9cf3e4c106885e07e7f63277643017736edd215b`
was the November 4 inventory's reference, not a moving `latest` selection.

## Install executables, then configure the selected codes

Use the companion [runtime installation guide](https://github.com/nanotech-empa/aiidalab-alps-files/blob/feature/setup-readiness-checks/docs/runtime-installation.md).
It covers ORCA/MPI, native local support codes, BandUPpy and SPM. A Code
registration does not install its executable. Supply licensed ORCA files through
the user's licensed distribution; they are not in Git or the source bundle.

Open the setup app, choose **Personal Docker**, select only the intended computers
and codes, and choose each remote account independently. Set the runtime paths to
the actual installations. `mac-localhost` is the catalog key; the AiiDA Computer
label is `localhost`, with `core.local`/`core.direct`. Its conservative one-process
default is editable to the container's tested allocation. Hosted setup offers no
local scientific-code catalog. Existing computers are preserved unless **Update
existing computer definitions** is explicitly enabled.

Inspect the plan. Unlisted resources default to **Keep**. Deactivation is optional
and never deletes scientific nodes. Replacing a Computer is blocked if it would
implicitly retire kept codes. New choices require reinspection. Apply prepares
selected SSH hosts, checks/pulls/mounts selected uenvs, runs only scoped support
tasks, verifies executables/dependencies and finally registers the selected codes.
Installation is not a transaction: a failed task can leave its new files in place;
reinspect and review that partial state before retrying.

For Daint/Tigu, renew the user's own MFA credentials and use the normal configured
SSH route. Daint host-key bootstrap precedes batch SSH checks. A passphrase-protected
key still needs the user's normal agent unlock; the app never answers password
prompts. In the existing Tigu route, the Docker SSH agent is forwarded to ELA,
where `ssh-add -t 5` temporarily loads ELA's Empa key for the jump-host path.
That private-key file can stay on ELA; the container's own `id_rsa` may be a
different identity. Reuse the proxy configuration with fresh CSCS credentials
and a working agent in the new instance. Do not copy the old instance's agent
environment/socket paths. Reusing the same registered key/account needs no new
SCI-IT registration.

For new users, choose the actual **Empa/Tigu username**, then click **Prepare new
Tigu key on ELA**. This creates/reuses `~/.ssh/aiidalab-tigu/id_ed25519` on ELA
without a passphrase for the non-interactive proxy. It preserves default keys,
checks the dedicated pair and displays only the public part. Send the displayed
request to `aliaksandr.yakutovich@empa.ch`, explicitly asking to add the public key
on both the jump host and Tigu and confirm when both are ready. If Tigu needs the
public key added separately, use an already working login or ask its administrator.
After confirmation, inspect/apply and check both hops. The action selects the
dedicated ELA path in the form; it does not write the live proxy until Apply.

For first use, obtain trusted host-key entries for `jump1.empa.ch` and Tigu from
SCI-IT and install them in the container's `known_hosts` using their instructions.
The custom proxy uses strict batch SSH; a missing/changed host key stops setup.
Unrelated SSH entries are preserved and backed up.

Quantum-worker **updates and new deployment work are deferred** for this setup
series. Preserve existing versioned worker prefixes/Codes. The known standard
localhost worker is `0.2.0.dev1`, source SHA-256
`43c77c2badba6db0eeb6678e8037f340ccf5e83f823e0524a795f7ac8b740a36`;
it is separate from the research `quantum-tools` tree. Its old hardware ledgers
and SDK prefix belong to their recorded deployments. A fresh instance must not
claim quantum readiness until that exact worker/SDK and manifest have been
restored and checked in the later worker task. No automatic IBM/QPU submission
belongs in configuration.

For ITensor, recover the app/backend revisions and use the app's committed model
runtime exporter with its checksum manifest. Keep the local Julia graph/Hubbard
preview distinct from the production Daint chain/BLAS runtime. General-model
production adapters and modern-platform qualification remain open; do not invent
generic production Codes or reuse obsolete scratch paths to make a selector green.

## Acceptance record

Record package versions/import paths, image digest/architecture, app-source hashes,
profile/daemon health, Computer/Code UUIDs and runtime manifests. Then:

- Run `verdi computer test` and `verdi code test` for every chosen registration.
- Load the required AiiDA/AiiDAlab entry points and open each canonical app notebook.
- Check PythTB imports/model calculation. Check the complete QE/BandUPpy and
  sparse-overlap/unfolding paths with their actual required support codes.
- For ORCA, run the app-owned MPI probe and serial/parallel PBE0/RIJCOSX validation,
  then a representative parallel CASCI check. See the runtime guide. A tiny HF
  test alone missed the earlier missing-Fortran-MPI problem.
- After approved small jobs, check retrieval/parsing and the app result view.
  A login-host uenv mount or `verdi code test` cannot certify a compute-node launch.
- Check daemon health and package consistency. Schedule a restart of this **new**
  instance after its jobs finish, then recheck startup/daemon health before calling
  the image reusable. Do not restart other active instances for this validation.

Keep merges, releases, scientific acceptance and deployment acceptance separate.
This runbook and its setup/configuration PRs remain unmerged pending review.
