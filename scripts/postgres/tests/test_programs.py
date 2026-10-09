"""Executable lookup does not implicitly trust a project working directory."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import programs
from state_io import ServiceError


class ProgramTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.cwd = self.root / "project"
        self.trusted = self.root / "tools"
        self.cwd.mkdir()
        self.trusted.mkdir()
        for directory in (self.cwd, self.trusted):
            (directory / "openssl.exe").write_bytes(b"lookup fixture, never executed")

    def test_bare_name_excludes_cwd_and_relative_path_entries(self):
        self.assertEqual(
            programs._windows_program(
                "openssl", [str(self.cwd), ".", "", str(self.trusted)], self.cwd
            ),
            str(self.trusted / "openssl.exe"),
        )

    def test_missing_path_tool_does_not_fall_back_to_cwd(self):
        with self.assertRaises(ServiceError):
            programs._windows_program("openssl", [str(self.cwd), ".", ""], self.cwd)

    def test_inaccessible_path_entry_does_not_hide_later_executable(self):
        original = Path.resolve

        def resolve(path):
            if path == self.root / "unavailable":
                raise OSError("fixture path unavailable")
            return original(path)

        with patch.object(Path, "resolve", resolve):
            self.assertEqual(
                programs._windows_program(
                    "openssl",
                    [str(self.root / "unavailable"), str(self.trusted)],
                    self.cwd,
                ),
                str(self.trusted / "openssl.exe"),
            )

    def test_explicit_operator_path_is_retained(self):
        explicit = self.cwd / "openssl.exe"
        self.assertEqual(
            programs._windows_program(str(explicit), [], self.cwd), str(explicit)
        )

    def test_explicit_extensionless_path_does_not_select_sibling_exe(self):
        explicit = self.cwd / "docker"
        explicit.write_bytes(b"explicit wrapper fixture, never executed")
        explicit.with_suffix(".exe").write_bytes(b"sibling fixture, never executed")
        for requested in (str(explicit), "./docker"):
            with self.subTest(requested=requested):
                self.assertEqual(
                    programs._windows_program(requested, [], self.cwd), str(explicit)
                )

    def test_missing_explicit_extensionless_path_does_not_select_sibling_exe(self):
        explicit = self.cwd / "docker"
        explicit.with_suffix(".exe").write_bytes(b"sibling fixture, never executed")
        for requested in (str(explicit), "./docker"):
            with self.subTest(requested=requested):
                with self.assertRaises(ServiceError):
                    programs._windows_program(requested, [], self.cwd)

    def test_extension_and_quoted_absolute_path_entries_work(self):
        self.assertEqual(
            programs._windows_program(
                "openssl.exe", ['"' + str(self.trusted) + '"'], self.cwd
            ),
            str(self.trusted / "openssl.exe"),
        )

    def test_normal_resolution_produces_an_absolute_executable(self):
        self.assertEqual(
            Path(programs.resolve_program(sys.executable)),
            Path(sys.executable).resolve(),
        )

    def test_windows_name_prefers_exe_over_extensionless_wrapper(self):
        (self.trusted / "docker").write_bytes(b"shell wrapper")
        (self.trusted / "docker.exe").write_bytes(b"executable fixture")
        self.assertEqual(
            programs._windows_program("docker", [str(self.trusted)], self.cwd),
            str(self.trusted / "docker.exe"),
        )
