# Clean-instance acceptance for the setup app

Use the new personal Docker instance to test this draft setup/configuration pair.
Keep production Kubernetes and existing personal instances intact. Source setup
and configuration changes stay unmerged until this test is reviewed.

1. Follow [the recreation runbook](recreate-personal-docker.md) through creation of
   a new image/volume, version checks, GitHub authentication and installation of
   the selected app/plugin sources. Record image digest and CPU/RAM/architecture.
2. Install setup branch `feature/selective-safe-setup` and choose configuration
   branch `feature/setup-readiness-checks`. Record **both full commit hashes**;
   do not mix the old setup app with schema 2. The displayed configuration
   revision must match the intended checkout.
3. Choose **Personal Docker** and only `mac-localhost`, with only PythonJob first.
   Leave CSCS credentials unset. Verify that Inspect/Apply configure the local
   Computer and Code without an SSH attempt. Run their AiiDA tests.
4. Add the other local families in small groups: BandUPpy, native CP2K/SPM and
   sparse-overlap/unfolding, QE/support tools, then licensed ORCA and its helpers.
   PythTB is installed with its app and does not appear as a fake executable Code.
   Record the actual installation action and runtime path for each group.
5. BandUPpy has a selected isolated-worker installer. Daint supporting wrappers,
   Bader/data files also have scoped tasks. Other native personal runtimes and
   licensed ORCA are currently agent-prepared prerequisites documented in the
   runtime guide. Treat missing one-click installers as an explicit acceptance
   gap; do not manually preinstall everything and then claim the app installed it.
6. Try an unavailable executable, missing plugin/dependency and wrong-architecture
   binary. Expect a useful diagnosis and no new/replaced AiiDA registration.
   Install the missing prerequisite, inspect again, and verify the retry path.
7. Configure Tigu using its normal SSH route, selecting only ORCA/helper codes
   initially. Check that no Daint-only tasks or localhost changes run. Then test
   the separate Tigu BandUPpy worker if requested.
8. Configure Daint and Eiger independently with their own project accounts.
   Verify that only selected code tasks run, including Eiger's own CP2K data
   installation. Check that the image's missing SSH config is created safely and
   Daint's first normal connection is non-interactive after host-key bootstrap.
9. With a missing/expired certificate or locked key, expect MFA/agent guidance,
   no repeated empty-password prompts and no partially changed registrations.
   Custom Tigu multi-hop first-login prerequisites must be reported explicitly.
10. Test a uenv already listed but unmountable, and a pull/migration/quota failure.
    Mount failure must stop setup with version/repository/group/path diagnostics.
    After a separately reviewed fix, repeat Inspect/Apply. The app must not force
    Capstor, broaden scratch permissions or claim compute-node launch success.
11. Create a disposable custom Code/Computer. Inspect must propose **Keep**.
    Applying another selected code must retain them. Test explicit deactivation
    separately and verify retained scientific data and unchanged unrelated labels.
12. Confirm selected-resource running/paused jobs block affected reconfiguration
    even when old, while unrelated computers do not block the plan. Change any
    account, code, deployment or path after inspection and verify Apply is invalidated.
13. Switch to **CSCS Kubernetes**: local scientific codes disappear from choices;
    existing local/custom resources are retained. Check PythTB in the app environment.
14. Run computer/code tests and approved small real jobs, including retrieval,
    parsing and GUI results. Test ORCA's numeric-type/parallel RIJCOSX/CASCI gates,
    complete QE/BandUPpy execution and the SPM sparse-overlap/unfolding path.
15. Check package/daemon health and the new instance's startup after its jobs finish.
    Record passed, failed, prerequisite-only and deferred items separately.

Quantum-worker updates and new deployments remain deferred. Preserve existing
worker identities/ledgers; do not submit QPU work as a configuration check.
ITensor local preview/production integration retains its separate qualification
limits in the living memory. Do not register fictional production model Codes.

The automated suite verifies planner safety, SSH command/config handling, uenv
failure paths and real localhost registration in disposable AiiDA profiles. It
does not replace this empty-image installation or remote compute acceptance.
