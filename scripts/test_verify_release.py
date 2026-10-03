#!/usr/bin/env python3
"""Unit tests for the synthetic release verifier."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("verify-release.py")
SPEC = importlib.util.spec_from_file_location("verify_release", SCRIPT)
assert SPEC and SPEC.loader
verify_release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_release)


class VerifyReleaseTests(unittest.TestCase):
    def test_main_resolves_relative_binary_before_temporary_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            binary = Path(temp_name) / ("asg.exe" if os.name == "nt" else "asg")
            binary.write_text("synthetic binary", encoding="utf-8")
            expected = str(binary.resolve())
            seen: list[str] = []

            def passed(binary_arg: str, *unused: object) -> bool:
                seen.append(binary_arg)
                return True

            checks = [
                "verify_build",
                "verify_sync",
                "verify_search",
                "verify_get",
                "verify_context",
                "verify_resume",
                "verify_handoff",
                "verify_semantic",
                "verify_hook",
                "verify_providers",
            ]
            patches = [mock.patch.object(verify_release, name, passed) for name in checks]
            for patcher in patches:
                patcher.start()
            try:
                with mock.patch("sys.argv", [str(SCRIPT), "--asg", str(binary)]):
                    self.assertEqual(verify_release.main(), 0)
            finally:
                for patcher in reversed(patches):
                    patcher.stop()
            self.assertEqual(seen, [expected] * len(checks))

    def test_semantic_check_requires_bigram_hash_and_effective_modes(self) -> None:
        frames = iter(
            [
                {"data": {"model_id": "bigram-hash-v1", "dimension": 384, "indexed": 2}},
                {"retrieval_mode": "semantic", "data": {"hits": [{"id": "msg_v1_s"}]}},
                {"retrieval_mode": "hybrid", "data": {"hits": [{"id": "msg_v1_h"}]}},
            ]
        )
        with mock.patch.object(verify_release, "run_asg", side_effect=lambda *args, **kwargs: next(frames)):
            self.assertTrue(verify_release.verify_semantic("synthetic-binary", "synthetic-root"))

    def test_handoff_requires_schema_10_and_deterministic_pack(self) -> None:
        frames = iter(
            [
                {"data": {"schema_version": "1.0", "pack_id": "pack_v1_abc", "evidence": [{"message_id": "msg_v1_a"}], "inference": []}},
                {"data": {"schema_version": "1.0", "pack_id": "pack_v1_abc", "evidence": [{"message_id": "msg_v1_a"}], "inference": []}},
            ]
        )
        with mock.patch.object(verify_release, "run_asg", side_effect=lambda *args, **kwargs: next(frames)):
            self.assertTrue(verify_release.verify_handoff("synthetic-binary", "synthetic-root"))

        frames = iter(
            [
                {"data": {"schema_version": "handoff-pack/v1", "pack_id": "pack_v1_abc", "evidence": [{"message_id": "msg_v1_a"}], "inference": []}},
                {"data": {"schema_version": "handoff-pack/v1", "pack_id": "pack_v1_abc", "evidence": [{"message_id": "msg_v1_a"}], "inference": []}},
            ]
        )
        with mock.patch.object(verify_release, "run_asg", side_effect=lambda *args, **kwargs: next(frames)):
            self.assertFalse(verify_release.verify_handoff("synthetic-binary", "synthetic-root"))

    def test_subprocess_isolates_platform_paths_without_changing_parent(self) -> None:
        keys = (
            "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
            "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
        )
        ambient = {key: "synthetic-ambient-" + key for key in keys}
        result = subprocess.CompletedProcess([], 0, '{"data":{}}', "")
        with tempfile.TemporaryDirectory() as root:
            with mock.patch.dict(os.environ, ambient):
                parent = dict(os.environ)
                with mock.patch.object(verify_release.subprocess, "run", return_value=result) as run:
                    verify_release.run_asg("synthetic-binary", root, ["index", "embeddings"])
                child = run.call_args.kwargs["env"]
                for key in keys:
                    self.assertTrue(Path(child[key]).is_relative_to(Path(root)), key)
                    self.assertNotEqual(child[key], ambient[key])
                self.assertEqual(dict(os.environ), parent)
                self.assertEqual(run.call_args.args[0][1:3], ["--db", str(Path(root) / "asg.db")])

    def test_hook_default_accepts_empty_stdout_and_diagnostic_stderr(self) -> None:
        result = subprocess.CompletedProcess(
            [], 0, "", "hook: event=UserPromptSubmit enabled=false offline=false hits=0 injected=false\n"
        )
        with mock.patch.object(verify_release.subprocess, "run", return_value=result):
            self.assertTrue(verify_release.verify_hook("synthetic-binary", "synthetic-root"))

    def test_hook_default_rejects_any_stdout(self) -> None:
        for output in (" \n", "unexpected history", '{"data":{"enabled":false}}'):
            with self.subTest(output=output):
                result = subprocess.CompletedProcess([], 0, output, "")
                with mock.patch.object(verify_release.subprocess, "run", return_value=result):
                    self.assertFalse(verify_release.verify_hook("synthetic-binary", "synthetic-root"))

    def test_hook_default_rejects_nonzero_exit_even_with_empty_stdout(self) -> None:
        result = subprocess.CompletedProcess([], 2, "", "synthetic failure")
        with mock.patch.object(verify_release.subprocess, "run", return_value=result):
            with self.assertRaises(verify_release.VerificationError):
                verify_release.verify_hook("synthetic-binary", "synthetic-root")

    def test_parse_first_json_line_ignores_blank_lines(self) -> None:
        self.assertEqual(
            verify_release.parse_first_json_line('\n{"data":{"value":1}}\n'),
            {"data": {"value": 1}},
        )

    def test_status_marks_print_on_a_non_utf8_code_page(self) -> None:
        """The verdict must survive a non-UTF-8 stdout encoding.

        A captured CI step's stdout is a pipe, so on a Windows runner Python
        encodes it with the ANSI code page, where the ✓/✗ status marks are
        unencodable. ``PYTHONIOENCODING`` reproduces that on every platform.
        """
        program = (
            "import importlib.util, sys;"
            f"spec = importlib.util.spec_from_file_location('verify_release', {str(SCRIPT)!r});"
            "module = importlib.util.module_from_spec(spec);"
            "sys.modules['verify_release'] = module;"
            "spec.loader.exec_module(module);"
            "module.step('encoding', True, 'pass mark');"
            "module.step('encoding', False, 'fail mark')"
        )
        result = subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=dict(os.environ, PYTHONIOENCODING="cp1252"),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("UnicodeEncodeError", result.stderr)
        self.assertIn("✓ encoding: pass mark", result.stdout)
        self.assertIn("✗ encoding: fail mark", result.stdout)


if __name__ == "__main__":
    unittest.main()
