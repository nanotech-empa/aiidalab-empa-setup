from empa_setup_utils import readiness


def remote_responses(monkeypatch, responses):
    calls = []

    def command(host, arguments):
        calls.append(arguments)
        return responses.get(tuple(arguments), ("", True))

    monkeypatch.setattr(readiness, "remote_command", command)
    return calls


def test_listed_image_that_cannot_mount_fails_with_path_diagnostics(
    monkeypatch, capsys
):
    image = "cp2k/2024.3:v2"
    calls = remote_responses(
        monkeypatch,
        {
            ("uenv", "repo", "status"): (
                "repository /ritom/scratch/cscs/user/.uenv-images is readwrite",
                True,
            ),
            ("uenv", "image", "ls"): ("uenv arch\ncp2k/2024.3:v2 gh200", True),
            ("uenv", "run", image, "--", "/bin/true"): (
                "invalid squashfs mount /ritom/scratch/cscs/user/.uenv-images/images/hash/store.squashfs: Permission denied",
                False,
            ),
        },
    )
    assert not readiness.manage_uenv_images([("daint", image)])
    output = capsys.readouterr().out
    assert "root-squash" in output and "10.2.0" in output
    assert any(args[0] == "namei" for args in calls)
    assert not any(
        "chmod" in args or "chown" in args or "pull" in args for args in calls
    )


def test_newly_pulled_image_is_mount_checked(monkeypatch):
    calls = remote_responses(
        monkeypatch, {("uenv", "image", "ls"): ("uenv arch", True)}
    )
    assert readiness.manage_uenv_images([("daint", "cp2k/2024.3:v2")])
    assert ["uenv", "image", "pull", "cp2k/2024.3:v2"] in calls
    assert ["uenv", "run", "cp2k/2024.3:v2", "--", "/bin/true"] in calls


def test_failed_pull_never_reports_success(monkeypatch, capsys):
    calls = remote_responses(
        monkeypatch,
        {("uenv", "image", "pull", "cp2k/2024.3:v2"): ("Disk quota exceeded", False)},
    )
    assert not readiness.manage_uenv_images([("daint", "cp2k/2024.3:v2")])
    assert "quota" in capsys.readouterr().out
    assert not any(args[:2] == ["uenv", "run"] for args in calls)


def test_installed_image_does_not_require_registry_access(monkeypatch):
    calls = remote_responses(
        monkeypatch,
        {
            ("uenv", "image", "ls"): (
                "repo default\nuenv arch\ncp2k/2024.3:v2 gh200",
                True,
            )
        },
    )
    assert readiness.manage_uenv_images([("daint", "cp2k/2024.3:v2")])
    assert not any("find" in args or "pull" in args for args in calls)


def test_code_environment_is_applied_inside_the_uenv_view(monkeypatch):
    calls = remote_responses(
        monkeypatch,
        {("uenv", "image", "ls"): ("uenv arch\ncp2k/2024.3:v2 gh200", True)},
    )
    requirement = {
        "host": "daint",
        "transport": "core.ssh",
        "image": "cp2k/2024.3:v2",
        "view": "cp2k",
        "executable": "/user-environment/cp2k",
        "prepend": "export CP2K_DATA_DIR=/users/test/custom-data",
        "code_key": "cp2k",
        "code_config": {},
    }
    assert readiness.manage_uenv_images([requirement])
    command = next(args for args in calls if "/bin/bash" in args)
    assert command[:5] == ["uenv", "run", "--view=cp2k", "cp2k/2024.3:v2", "--"]
    assert command[5:7] == ["/bin/bash", "-ec"]
    assert command[7].startswith("export CP2K_DATA_DIR=/users/test/custom-data\n")


def test_wrong_architecture_local_binary_is_rejected_without_execution(
    tmp_path, monkeypatch
):
    path = tmp_path / "orca"
    header = bytearray(20)
    header[:4] = b"\x7fELF"
    header[5] = 1
    header[18] = 62  # x86_64
    path.write_bytes(header)
    monkeypatch.setattr(readiness.platform, "machine", lambda: "aarch64")

    def forbidden(*args):
        raise AssertionError("wrong-architecture binary must not execute")

    monkeypatch.setattr(readiness, "run_command", forbidden)
    assert not readiness.manage_uenv_images(
        [
            {
                "host": "localhost",
                "transport": "core.local",
                "image": None,
                "view": None,
                "executable": str(path),
            }
        ]
    )
