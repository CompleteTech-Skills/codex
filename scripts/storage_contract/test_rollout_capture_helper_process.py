"""Real subprocess checks for bounded compressed-header helper execution."""

import json
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock

from .rollout_capture_preview import _capture_helper_output, _compressed_headers


class RolloutCaptureHelperProcessTest(unittest.TestCase):
    def test_posix_without_waitid_nowait_fails_before_spawning(self):
        if sys.platform == "win32":
            self.skipTest("Windows uses the owned Job Object handle")
        with (
            mock.patch(
                "storage_contract.rollout_capture_preview._posix_waitid_supported",
                return_value=False,
            ),
            mock.patch(
                "storage_contract.rollout_capture_preview._start_helper_process"
            ) as start,
        ):
            result = _capture_helper_output([sys.executable], b"")
        self.assertEqual(result, (None, None, "helper_unavailable"))
        start.assert_not_called()

    def test_real_helper_protocol_accepts_a_complete_batch(self):
        thread_id = str(uuid.uuid4())
        script = (
            "import json,sys\n"
            "for line in sys.stdin:\n"
            " json.loads(line)\n"
            f" print(json.dumps({{'status':'ok','thread_id':{thread_id!r},"
            "'ancestor_rollout_id':None}))\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            result = _compressed_headers(
                Path(temporary),
                Path(sys.executable).resolve(),
                [("archived_sessions/x.zst", thread_id, thread_id)],
                _helper_args=("-c", script),
            )
        self.assertEqual(result, ([(thread_id, None)], None))

    def test_real_helper_protocol_failure_discards_earlier_rows(self):
        ids = [str(uuid.uuid4()) for _ in range(2)]
        first = {
            "status": "ok",
            "thread_id": ids[0],
            "ancestor_rollout_id": None,
        }
        duplicate_key = (
            f'{{"status":"ok","thread_id":"{ids[1]}",'
            f'"thread_id":"{ids[1]}","ancestor_rollout_id":null}}'
        )
        script = (
            "import json\n"
            f"print({json.dumps(json.dumps(first))})\n"
            f"print({duplicate_key!r})\n"
        )
        with tempfile.TemporaryDirectory() as temporary:
            result = _compressed_headers(
                Path(temporary),
                Path(sys.executable).resolve(),
                [
                    ("archived_sessions/first.zst", ids[0], ids[0]),
                    ("archived_sessions/second.zst", ids[1], ids[1]),
                ],
                _helper_args=("-c", script),
            )
        self.assertEqual(result, (None, "helper_protocol"))

    def test_real_helper_over_limit_is_killed_during_output(self):
        script = "import os; os.write(1, b'x' * 4097)"
        with mock.patch(
            "storage_contract.rollout_capture_preview.MAX_HELPER_OUTPUT", 4096
        ):
            result = _capture_helper_output([sys.executable, "-c", script], b"")
        self.assertEqual(result, (None, None, "helper_output_limit"))

    def test_real_helper_flood_is_stopped_at_the_bound(self):
        script = "import os\nwhile True: os.write(1, b'x' * 4096)"
        started = time.monotonic()
        with mock.patch(
            "storage_contract.rollout_capture_preview.MAX_HELPER_OUTPUT", 32 * 1024
        ):
            result = _capture_helper_output([sys.executable, "-c", script], b"")
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(result, (None, None, "helper_output_limit"))

    def test_real_hanging_helper_is_terminated_at_deadline(self):
        started = time.monotonic()
        with mock.patch(
            "storage_contract.rollout_capture_preview.HELPER_TIMEOUT_SECONDS", 0.25
        ):
            result = _capture_helper_output(
                [sys.executable, "-c", "import time; time.sleep(30)"], b""
            )
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(result, (None, None, "helper_timeout"))

    def test_descendant_inheriting_stdout_is_stopped_without_waiting_for_pipe_eof(self):
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "descendant-survived"
            child = (
                "import time; time.sleep(1); "
                f"open({str(marker)!r}, 'w', encoding='utf-8').write('alive')"
            )
            script = (
                "import subprocess,sys\n"
                f"subprocess.Popen([sys.executable, '-c', {child!r}])\n"
                "print('started', flush=True)\n"
            )
            started = time.monotonic()
            with mock.patch(
                "storage_contract.rollout_capture_preview.HELPER_TIMEOUT_SECONDS",
                0.25,
            ):
                result = _capture_helper_output([sys.executable, "-c", script], b"")
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(result, (None, None, "helper_timeout"))
            time.sleep(1.1)
            self.assertFalse(marker.exists())

    def test_successful_helper_stops_descendant_that_redirected_stdout(self):
        thread_id = str(uuid.uuid4())
        response = json.dumps(
            {
                "status": "ok",
                "thread_id": thread_id,
                "ancestor_rollout_id": None,
            }
        )
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "descendant-survived"
            child = (
                "import time; time.sleep(1); "
                f"open({str(marker)!r}, 'w', encoding='utf-8').write('alive')"
            )
            script = (
                "import subprocess,sys\n"
                f"subprocess.Popen([sys.executable, '-c', {child!r}], "
                "stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
                f"print({response!r})\n"
            )
            result = _compressed_headers(
                Path(temporary),
                Path(sys.executable).resolve(),
                [("archived_sessions/x.zst", thread_id, thread_id)],
                _helper_args=("-c", script),
            )
            self.assertEqual(result, ([(thread_id, None)], None))
            time.sleep(1.1)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
