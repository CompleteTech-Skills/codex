"""Pure shared-protocol controls; no sockets, TLS, SQL or fixture invocation."""

import importlib.util
from pathlib import Path
import struct
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "commit_fault_source", Path(__file__).with_name("commit_ack_fault_proxy.py")
)
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


class BytesOnly:
    def __init__(self, data, chunk=1):
        self.data = data
        self.chunk = chunk

    def recv(self, count):
        take = min(count, self.chunk, len(self.data))
        value, self.data = self.data[:take], self.data[take:]
        return value


class ProtocolControls(unittest.TestCase):
    def test_actual_shared_request_fault_never_forwards_commit(self):
        class Stream(BytesOnly):
            def __init__(self, data=b"", fail=False):
                super().__init__(data)
                self.sent = bytearray()
                self.stopped = False
                self.fail = fail

            def sendall(self, value):
                self.sent.extend(value)

            def shutdown(self, _how):
                self.stopped = True
                if self.fail:
                    raise OSError("original shutdown failure")

        startup = struct.pack("!II", 8, 196608)
        for kind, payload in ((b"Q", b"COMMIT\0"), (b"P", b"\0COMMIT\0\0\0")):
            for fail in (False, True):
                wire = kind + struct.pack("!I", len(payload) + 4) + payload
                frontend, backend = Stream(startup + wire, fail), Stream()
                relay = source.OriginalRelay(frontend, backend, "commit_request")
                relay.client()  # SAME production parser/pump, no threads/sockets.
                self.assertEqual(bytes(backend.sent), startup)
                self.assertTrue(frontend.stopped and backend.stopped)
                self.assertTrue(relay.commit_request_observed)
                self.assertFalse(relay.commit_request_forwarded or relay.suppressed)
                self.assertEqual(relay.commit_request_kind, kind.decode("ascii"))
                self.assertEqual(
                    relay.commit_request_sha256, source.hashlib.sha256(wire).hexdigest()
                )
                self.assertEqual(
                    any(isinstance(cause, OSError) for cause in relay.causes), fail
                )

    def test_malformed_request_is_not_a_commit_fault_witness(self):
        class Stream(BytesOnly):
            def __init__(self, data=b""):
                super().__init__(data)
                self.sent = bytearray()

            def sendall(self, value):
                self.sent.extend(value)

        startup = struct.pack("!II", 8, 196608)
        for payload in (b"COMMIT", b"COMMIT\0\0"):
            wire = b"Q" + struct.pack("!I", len(payload) + 4) + payload
            frontend, backend = Stream(startup + wire), Stream()
            relay = source.OriginalRelay(frontend, backend, "commit_request")
            relay.client()
            self.assertEqual(bytes(backend.sent), startup + wire)
            self.assertFalse(
                relay.commit_request_observed or relay.commit_request_forwarded
            )
            self.assertTrue(any(isinstance(cause, EOFError) for cause in relay.causes))

    def test_unknown_fault_mode_refuses_before_stream_use(self):
        for mode in (True, "rollback", None):
            with self.assertRaises(ValueError):
                source.OriginalRelay(object(), object(), mode)

    def test_join_and_close_refusals_do_not_skip_other_originals(self):
        originals = []

        class OriginalTask:
            ident = 1

            def __init__(self, fail):
                self.fail = fail
                self.join_attempted = False

            def start(self):
                pass

            def join(self):
                self.join_attempted = True
                if self.fail:
                    raise RuntimeError("original join interrupted")

            def is_alive(self):
                return False  # Cannot substitute for a successful original join.

        class OriginalStream:
            def __init__(self, fail):
                self.fail = fail
                self.close_attempted = False

            def close(self):
                self.close_attempted = True
                if self.fail:
                    raise OSError("original close refused")

        def factory(**_kwargs):
            task = OriginalTask(not originals)
            originals.append(task)
            return task

        frontend, backend = OriginalStream(True), OriginalStream(False)
        relay = source.OriginalRelay(frontend, backend)
        with mock.patch.object(source.threading, "Thread", side_effect=factory):
            with self.assertRaises(source.RelayCleanupRefusal) as observed:
                relay.run()
        refusal = observed.exception
        self.assertIs(refusal.original, relay)
        self.assertEqual(refusal.started, tuple(originals))
        self.assertEqual(refusal.joined, (originals[1],))
        self.assertTrue(all(task.join_attempted for task in originals))
        self.assertTrue(frontend.close_attempted and backend.close_attempted)
        self.assertTrue(any(isinstance(cause, RuntimeError) for cause in relay.causes))
        self.assertTrue(any(isinstance(cause, OSError) for cause in relay.causes))

    def test_partial_start_releases_and_joins_same_started_original(self):
        class OriginalStream(BytesOnly):
            def __init__(self):
                super().__init__(b"")
                self.stopped = False
                self.closed = False

            def shutdown(self, _how):
                self.stopped = True

            def close(self):
                self.closed = True

        class RefusedStart:
            ident = None

            def start(self):
                raise RuntimeError("second original start refused")

        real_thread = source.threading.Thread
        originals = []

        def original_factory(**kwargs):
            original = real_thread(**kwargs) if not originals else RefusedStart()
            originals.append(original)
            return original

        frontend, backend = OriginalStream(), OriginalStream()
        relay = source.OriginalRelay(frontend, backend)
        with mock.patch.object(
            source.threading, "Thread", side_effect=original_factory
        ):
            report = relay.run()
        self.assertIs(relay.tasks[0], originals[0])
        self.assertFalse(originals[0].is_alive())
        self.assertEqual(report["started_original_threads"], 1)
        self.assertTrue(report["original_threads_joined"])
        self.assertFalse(report["all_original_threads_started"])
        self.assertTrue(frontend.stopped and backend.stopped)
        self.assertTrue(frontend.closed and backend.closed)
        self.assertIn("RuntimeError", report["cause_types"])

    def test_real_simple_command_recognition(self):
        self.assertTrue(source.commit_command(b"Q", b"COMMIT\0"))
        self.assertTrue(source.commit_command(b"Q", b" commit ; \0"))
        self.assertFalse(source.commit_command(b"Q", b"ROLLBACK\0"))
        self.assertFalse(source.commit_command(b"Q", b"COMMIT; SELECT 1\0"))
        self.assertFalse(source.commit_command(b"Q", b"COMMIT"))
        self.assertFalse(source.commit_command(b"Q", b"COMMIT\0\0"))
        self.assertFalse(source.commit_command(b"Q", b"COMMIT;;\0"))

    def test_real_extended_parse_recognition(self):
        self.assertTrue(source.commit_command(b"P", b"prepared\0COMMIT\0\0\0"))
        self.assertFalse(source.commit_command(b"P", b"COMMIT\0SELECT 1\0\0\0"))
        self.assertFalse(source.commit_command(b"P", b"unterminated"))
        self.assertFalse(source.commit_command(b"B", b"COMMIT\0"))
        self.assertFalse(source.commit_command(b"P", b"\0COMMIT\0"))
        self.assertFalse(source.commit_command(b"P", b"\0COMMIT\0\0"))
        self.assertFalse(source.commit_command(b"P", b"\0COMMIT\0\0\1"))
        self.assertFalse(source.commit_command(b"P", b"\0COMMIT\0\0\0extra"))

    def test_original_server_tag_frame_survives_fragmentation(self):
        wire = b"C" + struct.pack("!I", 11) + b"COMMIT\0"
        kind, payload, observed = source.message(BytesOnly(wire))
        self.assertEqual((kind, payload, observed), (b"C", b"COMMIT\0", wire))
        rollback = b"C" + struct.pack("!I", 13) + b"ROLLBACK\0"
        self.assertEqual(source.message(BytesOnly(rollback, 3))[1], b"ROLLBACK\0")

    def test_oversized_frame_refuses_before_payload_read(self):
        stream = BytesOnly(b"C" + struct.pack("!I", source.MAX_FRAME + 1) + b"retained")
        with self.assertRaises(ValueError):
            source.message(stream)
        self.assertEqual(stream.data, b"retained")

    def test_truncated_frame_never_mints_command_tag(self):
        with self.assertRaises(EOFError):
            source.message(BytesOnly(b"C" + struct.pack("!I", 11) + b"COMM"))
        with self.assertRaises(ValueError):
            source.message(BytesOnly(b"C" + struct.pack("!I", 3)))


if __name__ == "__main__":
    unittest.main()
