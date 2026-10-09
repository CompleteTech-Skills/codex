"""Synthetic physical-copy and direct-lineage checks for snapshot previews."""

import contextlib
import io
import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from .rollout_capture_preview import _compressed_headers, preview
from .rollout_capture_cli import main


class RolloutCapturePreviewTest(unittest.TestCase):
    def test_recursive_helper_json_is_reported_as_protocol_failure(self):
        depth = max(10_000, sys.getrecursionlimit() * 2)
        response = b"[" * depth + b"0" + b"]" * depth + b"\n"

        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch(
                "storage_contract.rollout_capture_preview._capture_helper_output",
                return_value=(0, response, None),
            ):
                result = _compressed_headers(
                    Path(temporary),
                    Path(sys.executable).resolve(),
                    [("a.zst", "id", "id")],
                )
        self.assertEqual(result, (None, "helper_protocol"))

    def test_copies_and_direct_ancestors_are_reported_without_selecting_a_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            active = home / "sessions" / "2026" / "09" / "28"
            archive = home / "archived_sessions"
            active.mkdir(parents=True)
            archive.mkdir()
            parent = str(uuid.uuid4())
            child = str(uuid.uuid4())
            orphan = str(uuid.uuid4())
            compressed_child = str(uuid.uuid4())
            parent_name = f"rollout-2026-09-28T12-00-00-{parent}.jsonl"
            child_name = f"rollout-2026-09-28T12-00-01-{child}.jsonl"
            compressed_name = (
                f"rollout-2026-09-28T12-00-02-{compressed_child}.jsonl.zst"
            )
            (active / parent_name).write_text(self._meta(parent), encoding="utf-8")
            (archive / parent_name).write_text(self._meta(parent), encoding="utf-8")
            (active / f"{parent_name}.zst").write_bytes(b"synthetic compressed copy")
            (active / child_name).write_text(
                self._meta(child, parent), encoding="utf-8"
            )
            orphan_child = str(uuid.uuid4())
            (archive / f"rollout-2026-09-28T12-00-03-{orphan_child}.jsonl").write_text(
                self._meta(orphan_child, orphan), encoding="utf-8"
            )
            (active / compressed_name).write_bytes(b"synthetic compressed child")

            result = preview(home)

            self.assertEqual(result["duplicate_rollout_ids"]["examples"], [parent])
            self.assertEqual(result["plain_compressed_siblings"]["examples"], [parent])
            self.assertEqual(result["active_archive_copies"]["examples"], [parent])
            self.assertEqual(
                result["ambiguous_direct_ancestors"]["examples"], [f"{child}:{parent}"]
            )
            self.assertEqual(
                result["missing_direct_ancestors"]["examples"],
                [f"{orphan_child}:{orphan}"],
            )
            self.assertEqual(result["compressed_headers_unknown"], 2)
            self.assertFalse(result["capture_complete"])
            self.assertFalse(result["activation_permitted"])
            self.assertNotIn(str(home), json.dumps(result))
            self.assertNotIn("synthetic compressed", json.dumps(result))

    def test_symlink_is_not_followed_or_counted_as_canonical(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            active = home / "sessions" / "2026" / "09" / "28"
            active.mkdir(parents=True)
            outside = home / "outside"
            outside.mkdir()
            thread_id = str(uuid.uuid4())
            name = f"rollout-2026-09-28T12-00-00-{thread_id}.jsonl"
            (outside / name).write_text(self._meta(thread_id), encoding="utf-8")
            try:
                (active / name).symlink_to(outside / name)
            except (NotImplementedError, OSError):
                self.skipTest("symlink creation unavailable")

            result = preview(home)

            self.assertEqual(result["canonical_files"], 0)
            self.assertEqual(result["unknown_entries"], 1)

    def test_native_compressed_batch_adds_lineage_only_after_full_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            archive = home / "archived_sessions"
            archive.mkdir()
            parent, child, missing = (str(uuid.uuid4()) for _ in range(3))
            for thread_id in (parent, child):
                (
                    archive / f"rollout-2026-09-28T12-00-00-{thread_id}.jsonl.zst"
                ).write_bytes(b"zstd")
            (archive / f"rollout-2026-09-28T12-00-00-{parent}.jsonl").write_text(
                self._meta(parent), encoding="utf-8"
            )

            def respond(command, requests):
                self.assertEqual(command[1:], ["--snapshot-home", str(home.absolute())])
                paths = [
                    json.loads(line)["relative_path"] for line in requests.splitlines()
                ]
                self.assertEqual(len(paths), 2)
                responses = []
                for path in paths:
                    thread_id = parent if parent in path else child
                    ancestor = missing if thread_id == child else None
                    responses.append(
                        json.dumps(
                            {
                                "status": "ok",
                                "thread_id": thread_id,
                                "ancestor_rollout_id": ancestor,
                            }
                        ).encode()
                    )
                return 0, b"\n".join(responses) + b"\n", None

            with mock.patch(
                "storage_contract.rollout_capture_preview._capture_helper_output",
                side_effect=respond,
            ):
                result = preview(
                    home, compressed_header_helper=Path(sys.executable).resolve()
                )
            self.assertEqual(result["compressed_header_status"], "processed")
            self.assertEqual(result["compressed_headers_unknown"], 0)
            self.assertEqual(
                result["missing_direct_ancestors"]["examples"], [f"{child}:{missing}"]
            )
            self.assertEqual(result["duplicate_rollout_ids"]["examples"], [parent])
            self.assertFalse(result["capture_complete"])
            self.assertFalse(result["activation_permitted"])
            self.assertNotIn(str(home), json.dumps(result))

    def test_invalid_late_native_response_discards_all_header_edges(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            archive = home / "archived_sessions"
            archive.mkdir()
            ids = [str(uuid.uuid4()) for _ in range(2)]
            for thread_id in ids:
                (
                    archive / f"rollout-2026-09-28T12-00-00-{thread_id}.jsonl.zst"
                ).write_bytes(b"zstd")

            def respond(command, requests):
                first = json.dumps(
                    {
                        "status": "ok",
                        "thread_id": ids[0],
                        "ancestor_rollout_id": str(uuid.uuid4()),
                    }
                )
                second = (
                    f'{{"status":"ok","thread_id":"{ids[1]}",'
                    f'"thread_id":"{ids[1]}","ancestor_rollout_id":null}}'
                )
                return 0, f"{first}\n{second}\n".encode(), None

            with mock.patch(
                "storage_contract.rollout_capture_preview._capture_helper_output",
                side_effect=respond,
            ):
                result = preview(
                    home, compressed_header_helper=Path(sys.executable).resolve()
                )
            self.assertEqual(result["compressed_header_status"], "unavailable")
            self.assertEqual(result["compressed_header_code"], "helper_protocol")
            self.assertEqual(result["compressed_headers_unknown"], 2)
            self.assertEqual(result["missing_direct_ancestors"]["count"], 0)

    def test_timeout_and_request_limit_leave_compressed_headers_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            archive = home / "archived_sessions"
            archive.mkdir()
            thread_id = str(uuid.uuid4())
            (
                archive / f"rollout-2026-09-28T12-00-00-{thread_id}.jsonl.zst"
            ).write_bytes(b"zstd")
            with mock.patch(
                "storage_contract.rollout_capture_preview._capture_helper_output",
                return_value=(None, None, "helper_timeout"),
            ):
                result = preview(
                    home, compressed_header_helper=Path(sys.executable).resolve()
                )
            self.assertEqual(
                (
                    result["compressed_header_code"],
                    result["compressed_headers_unknown"],
                ),
                ("helper_timeout", 1),
            )
            candidates = [("archived_sessions/x", thread_id, thread_id)] * 1025
            self.assertEqual(
                _compressed_headers(home, Path(sys.executable).resolve(), candidates),
                (None, "request_limit"),
            )

    def test_oversized_helper_output_is_discarded(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            archive = home / "archived_sessions"
            archive.mkdir()
            thread_id = str(uuid.uuid4())
            (
                archive / f"rollout-2026-09-28T12-00-00-{thread_id}.jsonl.zst"
            ).write_bytes(b"zstd")

            with mock.patch(
                "storage_contract.rollout_capture_preview._capture_helper_output",
                return_value=(None, None, "helper_output_limit"),
            ):
                result = preview(
                    home, compressed_header_helper=Path(sys.executable).resolve()
                )
            self.assertEqual(
                (
                    result["compressed_header_code"],
                    result["compressed_headers_unknown"],
                ),
                ("helper_output_limit", 1),
            )

    def test_cli_rejects_relative_helper_without_echoing_it(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = main(
                [
                    "--snapshot-home",
                    "unused",
                    "--compressed-header-helper",
                    "secret/helper",
                ]
            )
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue())["code"], "invalid_helper_path")
        self.assertNotIn("secret/helper", output.getvalue())

    @staticmethod
    def _meta(thread_id, ancestor=None):
        payload = {"id": thread_id}
        if ancestor is not None:
            payload["history_base"] = {"thread_id": ancestor}
        return json.dumps({"type": "session_meta", "payload": payload}) + "\n"
