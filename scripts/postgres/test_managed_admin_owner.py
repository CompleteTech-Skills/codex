"""Pure shared-owner controls: fake processes/threads only, no Docker/SQL/I/O."""

import importlib.util
import io
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


class Refusal(Exception):
    pass


dependencies = {
    "programs": types.SimpleNamespace(
        resolve_program=lambda *args, **kwargs: "/fake/docker"
    ),
    "state": types.SimpleNamespace(ServiceError=Refusal, load=lambda path: {}),
}
with patch.dict(sys.modules, dependencies):
    spec = importlib.util.spec_from_file_location(
        "original_managed_admin_under_test",
        Path(__file__).with_name("managed_admin_owner.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


class FakeThread:
    instances = []
    fail_start = None
    fail_join = None

    def __init__(self, target, args=(), daemon=False):
        self.target, self.args = target, args
        self.started = self.joined = self.join_attempted = False
        self.index = len(self.instances)
        self.instances.append(self)

    def start(self):
        if self.index == self.fail_start:
            raise RuntimeError("original start failure")
        self.started = True

    def join(self):
        self.join_attempted = True
        if not self.started:
            raise RuntimeError("unstarted join")
        if self.index == self.fail_join:
            raise RuntimeError("original join failure")
        if not self.joined:
            self.target(*self.args)
            self.joined = True


class Input(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.actual_bytes = bytearray()

    def write(self, value):
        self.actual_bytes.extend(value)
        return super().write(value)


class FakeProcess:
    def __init__(self, stdout=b"native result", stderr=b"", read_failure=False):
        self.stdin = Input()
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self.waited = False
        if read_failure:
            ordinary = self.stdout.read

            def read(size):
                value = ordinary(size)
                if not value:
                    raise OSError("original read failure")
                return value

            self.stdout.read = read

    def wait(self):
        self.waited = True
        return 0


class OriginalOwnerControls(unittest.TestCase):
    def setUp(self):
        FakeThread.instances = []
        FakeThread.fail_start = FakeThread.fail_join = None
        self.owner = module.ManagedAdministrativeOwner("/not-read")
        self.owner._executable = "/fake/docker"
        self.owner._environment = {}

    def execute(self, process, sql=b"fixed identity query"):
        with (
            patch.object(module.threading, "Thread", FakeThread),
            patch.object(module.subprocess, "Popen", return_value=process),
        ):
            return self.owner._execute(["exec", "owned-container"], sql)

    def test_exact_actual_process_and_both_pumps_reaped(self):
        process = FakeProcess()
        self.assertEqual(self.execute(process), b"native result")
        self.assertIs(self.owner.children[0].process, process)
        self.assertEqual(process.stdin.actual_bytes, b"fixed identity query")
        self.assertTrue(process.waited)
        self.assertTrue(all(task.joined for task in FakeThread.instances))
        self.assertTrue(self.owner.children[0].terminal)

    def test_partial_second_pump_start_never_dispatches_sql(self):
        FakeThread.fail_start = 1
        process = FakeProcess()
        with self.assertRaises(module.AdministrativeRefusal) as caught:
            self.execute(process)
        self.assertIs(caught.exception.original, self.owner)
        self.assertEqual(process.stdin.actual_bytes, b"")
        self.assertTrue(process.waited)
        self.assertTrue(FakeThread.instances[0].joined)
        self.assertFalse(FakeThread.instances[1].join_attempted)
        self.assertTrue(process.stdout.closed and process.stderr.closed)
        self.assertIsInstance(self.owner.children[0].causes["dispatch"], RuntimeError)

    def test_join_failure_still_attempts_second_original(self):
        FakeThread.fail_join = 0
        with self.assertRaises(module.AdministrativeRefusal):
            self.execute(FakeProcess())
        self.assertTrue(all(task.join_attempted for task in FakeThread.instances))
        self.assertTrue(FakeThread.instances[1].joined)
        self.assertFalse(self.owner.children[0].terminal)
        self.assertIsInstance(
            self.owner.children[0].causes["pump_join_0"], RuntimeError
        )

    def test_output_cap_and_later_read_cause_both_survive(self):
        with self.assertRaises(module.AdministrativeRefusal):
            self.execute(FakeProcess(b"x" * 65537, read_failure=True))
        child = self.owner.children[0]
        self.assertEqual(len(child.stdout), 65536)
        self.assertIn("stdout_cap", child.causes)
        self.assertIsInstance(child.causes["stdout_read"], OSError)
        self.assertTrue(child.terminal)

    def test_original_gate_failure_is_raw_cause_before_pipe_close(self):
        class FailedGate:
            def wait(self):
                raise RuntimeError("original gate failure")

        child = module._Child()
        original_stream = io.BytesIO(b"not read")
        self.owner._pump(child, original_stream, child.stdout, "stdout", FailedGate())
        self.assertIsInstance(child.causes["stdout_read"], RuntimeError)
        self.assertTrue(original_stream.closed)
        self.assertEqual(child.stdout, b"")

    def test_original_worker_lock_failure_is_cached_before_body_terminal(self):
        class FailedLock:
            def __enter__(self):
                raise RuntimeError("original worker lock failure")

            def __exit__(self, *args):
                return False

        self.owner._lock = FailedLock()
        self.owner._run()
        self.assertIsInstance(self.owner.causes["worker"], RuntimeError)
        self.assertTrue(self.owner._body_terminal)
        self.assertEqual(self.owner.children, [])
        with self.assertRaises(module.AdministrativeRefusal):
            self.owner.finish_original()


if __name__ == "__main__":
    unittest.main()
