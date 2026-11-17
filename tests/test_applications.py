"""Automatic app requirements and the normal setup UI; no scientific execution."""

import hashlib
import os
from copy import deepcopy
from importlib import metadata
from pathlib import Path

import pytest
from packaging.utils import canonicalize_name

import start
from empa_setup_utils import applications, control
from empa_setup_utils.configuration import load_config_bundle
from empa_setup_utils.selection import installation_tasks, select_resources

CATALOG = os.environ.get("EMPA_SETUP_CATALOG")
pytestmark = pytest.mark.skipif(not CATALOG, reason="Supply the paired catalog")


@pytest.fixture
def discovery(tmp_path, monkeypatch):
    config = load_config_bundle(CATALOG)
    root = tmp_path / "apps"
    root.mkdir()
    monkeypatch.setattr("aiidalab.config.AIIDALAB_APPS", str(root))
    packages = {}

    def version(name):
        if canonicalize_name(name) not in packages:
            raise metadata.PackageNotFoundError(name)
        return packages[canonicalize_name(name)]

    monkeypatch.setattr(applications.metadata, "version", version)

    def install(app, *, name=None, with_metadata=False):
        spec = config["applications"][app]
        path = root / (name or spec["app_ids"][0])
        path.mkdir()
        (path / "start.py").write_text(
            "raise AssertionError('Do not import app code')\n"
        )
        if with_metadata:
            (path / "setup.cfg").write_text(
                "[metadata]\nname=" + spec["distributions"][0] + "\n"
            )
        return path

    return config, root, packages, install


def test_no_apps_or_backend_only_never_selects_all_codes(discovery):
    config, root, packages, _ = discovery
    packages.update(
        {
            "aiida-empa-orca": "0.1",
            "aiida-cp2k": "2.2",
            "aiidalab-orca": "0.1",
            "aiidalab-qe": "26.11",
        }
    )
    assert applications.discover_applications(config, root) == {}
    applications.prepare_applications(config)
    config["_deployment"] = "personal"
    computers, codes = select_resources(config, None, ["mac-localhost"], None)
    assert codes == set() and installation_tasks(config, computers, codes) == []
    assert (
        installation_tasks(
            config, {"mac-localhost", "tigu-modules", "daint.alps"}, set()
        )
        == []
    )


@pytest.mark.parametrize(
    "app",
    [
        "orca",
        "surfaces",
        "nanoribbons",
        "qe",
        "qe_postprocess",
        "model",
        "pythtb",
        "gaussian",
    ],
)
def test_each_frontend_has_explicit_machine_scoped_requirements(discovery, app):
    config, root, _, install = discovery
    install(app)
    found = applications.discover_applications(config, root)
    assert set(found) == {app}
    for computer in ["mac-localhost", "tigu-modules", "daint.alps"]:
        keys = applications.codes_for_applications(config, found, [computer])
        assert all(config["codes"][key]["computer"] == computer for key in keys)
        assert keys == {
            key
            for key in config["applications"][app]["codes"]
            if config["codes"][key]["computer"] == computer
        }
    assert not applications.codes_for_applications(config, found, ["tigu"])


def test_custom_app_directory_uses_its_package_identity_not_its_name(discovery):
    config, root, _, install = discovery
    path = install("surfaces", name="my-surface-app", with_metadata=True)
    assert applications.discover_applications(config, root)["surfaces"]["paths"] == [
        str(path)
    ]
    (root / "orca").mkdir()  # An empty clone/path is not an installed app.
    assert set(applications.discover_applications(config, root)) == {"surfaces"}


def test_qe_property_packages_require_the_actual_qe_frontend(discovery):
    config, root, packages, install = discovery
    packages.update({"aiidalab-qe-pp": "2.5", "aiidalab-qe-vibroscopy": "1.3"})
    assert applications.discover_applications(config, root) == {}
    install("qe")
    found = applications.discover_applications(config, root)
    assert set(found) == {"qe", "qe_pp", "qe_vibroscopy"}
    for computer in ["mac-localhost", "tigu-modules", "daint.alps"]:
        keys = applications.codes_for_applications(config, found, [computer])
        assert any(
            config["codes"][key]["default_calc_job_plugin"] == "pythonjob.pythonjob"
            for key in keys
        )


def test_shared_requirements_are_deduplicated_and_scoped_to_apps(discovery):
    config, root, _, install = discovery
    install("surfaces")
    install("nanoribbons")
    found = applications.discover_applications(config, root)
    keys = applications.codes_for_applications(config, found, ["mac-localhost"])
    assert keys == set(config["applications"]["surfaces"]["codes"]) & {
        key
        for key, value in config["codes"].items()
        if value["computer"] == "mac-localhost"
    }
    tasks = installation_tasks(config, {"mac-localhost"}, keys)
    assert {task["name"] for task in tasks} == {
        "local_cp2k_spm_runtime",
        "local_native_support",
        "local_cube_render_runtime",
    }
    assert not any(
        "orca" in key or "itensor" in key or "quantum" in key for key in keys
    )


def test_model_does_not_install_orca_and_pythtb_remains_tigu_capable(discovery):
    config, root, _, install = discovery
    install("model")
    keys = applications.codes_for_applications(
        config, applications.discover_applications(config, root), ["mac-localhost"]
    )
    assert keys == {"local_itensor_gs", "local_itensor_segment"}
    assert {
        task["name"] for task in installation_tasks(config, {"mac-localhost"}, keys)
    } == {"local_julia_runtime", "local_itensor_project"}
    install("pythtb")
    found = applications.discover_applications(config, root)
    assert "tigu_module_pythtb" in applications.codes_for_applications(
        config, found, ["tigu-modules"]
    )
    assert not any(
        "pythtb" in key
        for key in applications.codes_for_applications(config, found, ["daint.alps"])
    )


def test_automatic_selection_rejects_manual_widening_and_legacy_tigu(
    discovery, monkeypatch
):
    config, _, _, install = discovery
    install("model")
    applications.prepare_applications(config)
    config["_deployment"] = "personal"
    monkeypatch.setattr("platform.machine", lambda: "aarch64")
    with pytest.raises(ValueError, match="requirements changed"):
        select_resources(config, None, ["mac-localhost"], ["local_orca"])
    with pytest.raises(ValueError, match="available"):
        select_resources(config, None, ["tigu"], [])


def test_catalog_mapping_rejects_missing_code_keys(discovery):
    config, _, _, _ = discovery
    config["applications"]["model"]["codes"].append("missing-native-code")
    with pytest.raises(ValueError, match="unknown code"):
        applications.codes_for_applications(config, ["model"], ["mac-localhost"])


def test_license_archive_discovery_accepts_only_exact_user_supplied_bytes(
    discovery, tmp_path
):
    config, _, _, _ = discovery
    config["_installed_applications"] = {"orca": {}}
    config["variables"]["local_orca_archive"] = ""
    config["variables"]["local_orca_archive_sha256"] = hashlib.sha256(
        b"licensed-test-fixture"
    ).hexdigest()
    work = tmp_path / "work"
    work.mkdir()
    (work / "orca-wrong.tar.xz").write_bytes(b"wrong vendor artifact")
    original = deepcopy(config)
    applications.find_licensed_orca_archive(config, work)
    assert config == original
    archive = work / "orca-reviewed.tar.xz"
    archive.write_bytes(b"licensed-test-fixture")
    applications.find_licensed_orca_archive(config, work)
    assert config["variables"]["local_orca_archive"] == str(archive)
    assert archive.read_bytes() == b"licensed-test-fixture"


@pytest.fixture
def automatic_widget(discovery, monkeypatch):
    config, _, _, _ = discovery
    # Exercise the explicitly supported Linux ARM64 personal-Docker form on
    # hosted x86 CI too; no native executable is started by these widget tests.
    monkeypatch.setattr("platform.machine", lambda: "aarch64")
    monkeypatch.setattr(start, "available_config_branches", lambda **kw: ["main"])
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp, "check_paused_workchains", lambda self: (False, "")
    )
    monkeypatch.setattr(
        start, "check_repository", lambda **kw: pytest.fail("Opening must not fetch")
    )
    original = start.ConfigAiiDAlabApp.widgets_from_yaml
    monkeypatch.setattr(
        start.ConfigAiiDAlabApp,
        "widgets_from_yaml",
        lambda self, **kw: original(self, **{**kw, "file_path": Path(CATALOG)}),
    )
    widget = start.ConfigAiiDAlabApp()
    yield widget
    widget.close()


def descendants(widget):
    yield widget
    for child in getattr(widget, "children", []):
        yield from descendants(child)


def test_normal_ui_has_one_tigu_and_no_deployment_code_or_path_controls(
    automatic_widget,
):
    widget = automatic_widget
    tree = list(descendants(widget))
    assert widget.deployment not in tree and widget.code_selection not in tree
    assert (
        widget.all_codes_button not in tree and widget.all_computers_button not in tree
    )
    assert widget.play_button not in tree
    assert [w.description for w in widget.computer_choices.children] == [
        "Daint",
        "Localhost",
        "Tigu",
    ]
    assert set(widget.account_widgets) == {"daint.alps", "tigu-modules"}
    assert widget.account_widgets["tigu-modules"].description == "Tigu account"
    assert not widget.code_selection.value
    assert set(widget.runtime_setting_box.children) == {
        widget.runtime_settings[k]
        for k in ["tigu_empa_username", "local_default_mpi", "local_memory_kib"]
    }
    assert widget.computer_selection.value == ("mac-localhost",)
    widget.computer_checkboxes["tigu-modules"].value = True
    assert widget.computer_selection.value == ("mac-localhost", "tigu-modules")
    assert len(widget.connection_help.children) == 1


def test_installing_app_updates_automatic_choices_and_invalidates_review(
    automatic_widget, discovery
):
    widget = automatic_widget
    _, _, _, install = discovery
    widget.updates_needed = {"codes": {"old": {"install": True}}}
    widget.start_button.disabled = False
    install("model")
    widget.refresh_applications()
    widget.on_computers_change(None)
    assert widget.code_selection.value == ("local_itensor_gs", "local_itensor_segment")
    assert widget.start_button.disabled and not widget.updates_needed
    assert "Model Hamiltonians" in widget.applications_summary.value


def test_app_changes_before_apply_cannot_reach_installers(
    automatic_widget, discovery, monkeypatch
):
    widget = automatic_widget
    config, _, _, install = discovery
    applications.prepare_applications(config)
    widget.config = config
    install("model")
    monkeypatch.setattr(
        start, "execute_setup_tasks", lambda *a: pytest.fail("No installer may run")
    )
    monkeypatch.setattr(
        start,
        "check_for_updates",
        lambda *a, **kw: pytest.fail("No stale reinspection"),
    )
    widget._apply_configuration()
    assert "Installed apps changed" in widget.update_message.value


def test_cscs_identity_change_keeps_tigu_and_supports_an_optional_empa_override(
    automatic_widget,
):
    widget = automatic_widget
    widget.computer_checkboxes["tigu-modules"].value = True
    widget.config_widgets["cscsuser"].value = "cpi"
    assert set(widget.computer_selection.value) == {"mac-localhost", "tigu-modules"}
    assert widget.tigu_username() == "cpi"
    assert widget.runtime_settings["tigu_empa_username"].value == ""
    assert "tigu_empa_username" not in widget.config_arguments()["settings"]
    assert (
        widget.account_widgets["tigu-modules"] not in widget.account_selectors.children
    )
    widget.runtime_settings["tigu_empa_username"].value = "empa-user"
    widget.config_widgets["cscsuser"].value = "adroux"
    assert widget.tigu_username() == "empa-user"
    assert "tigu-modules" in widget.computer_selection.value


def test_hidden_empty_archive_setting_does_not_override_a_verified_work_upload(
    discovery, tmp_path, monkeypatch
):
    config, _, _, install = discovery
    install("orca")
    work = tmp_path / "work"
    work.mkdir()
    archive = work / "orca-6.1.1.tar.xz"
    archive.write_bytes(b"licensed-fixture")
    config["variables"]["local_orca_archive"] = ""
    config["variables"]["local_orca_archive_sha256"] = hashlib.sha256(
        archive.read_bytes()
    ).hexdigest()
    monkeypatch.setattr(control, "load_config_bundle", lambda *a: deepcopy(config))
    monkeypatch.setattr(
        control,
        "find_licensed_orca_archive",
        lambda data: applications.find_licensed_orca_archive(data, work),
    )
    ok, message, rendered = control.get_config(
        file_path=CATALOG,
        deployment="personal",
        selected_computers=["mac-localhost"],
        automatic_codes=True,
        settings={"local_orca_archive": ""},
    )
    assert ok, message
    assert rendered["variables"]["local_orca_archive"] == str(archive)
    command = rendered["setup_tasks"]["local_orca_runtime"]["commands"][0]["arguments"]
    assert command[command.index("--archive") + 1] == str(archive)
