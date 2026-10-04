"""Inactive owned local-peer administration prerequisite; identity queries only.

No CLI, role/grant mutation, HBA deployment, password authentication, or exclusive
permit. Existing state.load validates every protected provider file by reading its
bytes, including secret files; no secret is sent to this local-peer SQL route or
printed. Those existing integrity checks are not bypassed.
The owning host must retain this original frame before start_original(). Generic
remote administration needs a distinct authenticated transport implementation.
"""

import json
import os
from pathlib import Path
import subprocess
import threading

from programs import resolve_program
from state import ServiceError, load


_IDENTITY_SQL = b"""SELECT pg_catalog.json_build_object(
  'session_user', session_user, 'current_user', current_user,
  'superuser', (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = session_user),
  'database', pg_catalog.current_database(),
  'database_oid', (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()),
  'system_identifier', (SELECT system_identifier::text FROM pg_catalog.pg_control_system()),
  'backend_pid', pg_catalog.pg_backend_pid(),
  'instance', (SELECT instance FROM codex_service.bootstrap WHERE format = 1));
"""


class AdministrativeRefusal(ServiceError):
    """Redacted diagnostic retaining the actual original owner and native causes."""

    def __init__(self, original):
        super().__init__("managed_administration_original_refused")
        self.original = original


class _Child:
    def __init__(self):
        self.process = None
        self.pumps = []
        self.joined = []
        self.returncode = None
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.causes = {}
        self.terminal = False


class ManagedAdministrativeOwner:
    """Own one real worker and each actual process/pipe before SQL dispatch.

    Captured JSON and Docker labels never yield authority: identity is additionally
    queried on the actual local-peer PostgreSQL session. Even that observation is
    not proof that old backends, prepared transactions or other writers drained.
    """

    def __init__(self, state_path):
        self.state_path = Path(state_path)
        self._lock = threading.Lock()
        self._join = threading.Lock()
        self._thread = None
        self._hold = None
        self._started = False
        self._joined = False
        self._body_terminal = False
        self._join_failures = [None] * 8
        self._join_attempts = 0
        self.receipt = None
        self.children = []
        self.causes = {}
        self.native_identity = None
        self.container_identity = None
        self.engine_identity = None
        self._environment = None
        self._executable = None

    def start_original(self):
        with self._lock:
            if self._started:
                raise AdministrativeRefusal(self)
            self._started = True
            self._hold = self
            original = threading.Thread(target=self._run, daemon=False)
            self._thread = original
            try:
                original.start()
            except BaseException as error:
                self.causes["worker_start"] = error
                # No caller sees successful dispatch. Preserve the original frame;
                # a failed native thread start is not a synthetic join receipt.
                raise AdministrativeRefusal(self) from None

    def _pump(self, child, stream, target, key, gate):
        try:
            gate.wait()
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    break
                if len(target) + len(chunk) <= 65536:
                    target.extend(chunk)
                elif key + "_cap" not in child.causes:
                    child.causes[key + "_cap"] = ServiceError(
                        "administration_output_limit"
                    )
                # Continue draining the SAME pipe after cap; no unbounded output
                # allocation or inference that a cap means child termination.
        except BaseException as error:
            child.causes[key + "_read"] = error
        finally:
            try:
                stream.close()
            except BaseException as error:
                child.causes[key + "_close"] = error

    def _execute(self, arguments, input_bytes=None):
        child = _Child()
        self.children.append(child)  # Original resource frame exists before launch.
        gate = threading.Event()
        try:
            child.process = subprocess.Popen(
                [self._executable, *arguments],
                stdin=subprocess.PIPE
                if input_bytes is not None
                else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._environment,
            )
            for stream, target, key in (
                (child.process.stdout, child.stdout, "stdout"),
                (child.process.stderr, child.stderr, "stderr"),
            ):
                task = threading.Thread(
                    target=self._pump,
                    args=(child, stream, target, key, gate),
                    daemon=False,
                )
                child.pumps.append(task)
                task.start()
                child.joined.append(False)
            # Both original pumps and process are retained before the first SQL
            # byte. The worker is independent of an interrupted observer join.
            gate.set()
            if input_bytes is not None:
                child.process.stdin.write(input_bytes)
                child.process.stdin.close()
        except BaseException as error:
            child.causes["dispatch"] = error
        finally:
            gate.set()
            if child.process is not None:
                if child.process.stdin is not None:
                    try:
                        child.process.stdin.close()
                    except BaseException as error:
                        child.causes["stdin_close"] = error
                # A partially started reader set must not strand the other pipe.
                # No SQL is sent in that case; close unowned local pipe ends.
                for index, stream in enumerate(
                    (child.process.stdout, child.process.stderr)
                ):
                    if index >= len(child.joined):
                        try:
                            stream.close()
                        except BaseException as error:
                            child.causes["unstarted_pipe_close_" + str(index)] = error
                try:
                    child.returncode = child.process.wait()
                except BaseException as error:
                    child.causes["process_wait"] = error
                for index in range(len(child.joined)):
                    try:
                        child.pumps[index].join()
                        child.joined[index] = True
                    except BaseException as error:
                        child.causes["pump_join_" + str(index)] = error
                child.terminal = child.returncode is not None and all(child.joined)
        if child.causes or not child.terminal or child.returncode != 0:
            raise AdministrativeRefusal(self)
        return bytes(child.stdout)

    def _run(self):
        try:
            # Sender holds this same lock until the original worker handle is saved.
            with self._lock:
                pass
            self.receipt = load(self.state_path)
            receipt = self.receipt
            self._environment = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("COMPOSE_", "CODEX_PG_", "PG"))
            }
            self._executable = resolve_program("docker", environment=self._environment)
            self.engine_identity = (
                self._execute(["info", "--format", "{{.ID}} "]).decode().strip()
            )
            if self.engine_identity != receipt["engine_id"]:
                raise ServiceError("administration_engine_changed")
            image = (
                self._execute(
                    [
                        "image",
                        "inspect",
                        receipt["image_digest"],
                        "--format",
                        "{{.Id}} ",
                    ]
                )
                .decode()
                .strip()
            )
            ids = (
                self._execute(
                    [
                        "ps",
                        "-q",
                        "--filter",
                        "label=com.docker.compose.project=" + receipt["project"],
                        "--filter",
                        "label=com.docker.compose.service=postgres",
                    ]
                )
                .decode()
                .split()
            )
            if len(ids) != 1:
                raise ServiceError("administration_container_ambiguous")
            observed = json.loads(self._execute(["inspect", ids[0]]))
            if not isinstance(observed, list) or len(observed) != 1:
                raise ServiceError("administration_container_ambiguous")
            container = observed[0]
            self.container_identity = container
            labels = container["Config"]["Labels"]
            mounts = container["Mounts"]
            roots = [
                mount
                for mount in mounts
                if mount["Destination"] == "/var/lib/postgresql/data"
            ]
            overlays = [
                mount
                for mount in mounts
                if mount["Destination"].startswith("/var/lib/postgresql/data/")
            ]
            if (
                container["Image"] != image
                or labels.get("com.completedottech.codex.pg.instance")
                != receipt["instance"]
                or labels.get("com.docker.compose.project") != receipt["project"]
                or labels.get("com.docker.compose.service") != "postgres"
                or not container["State"]["Running"]
                or len(roots) != 1
                or overlays
                or roots[0]["Type"] != "volume"
                or roots[0]["Name"] != receipt["volume"]
            ):
                raise ServiceError("administration_container_changed")
            raw = self._execute(
                [
                    "exec",
                    "-i",
                    "--user",
                    "postgres",
                    container["Id"],
                    "psql",
                    "-X",
                    "-qAt",
                    "--no-password",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-d",
                    "codex",
                ],
                _IDENTITY_SQL,
            )
            self.native_identity = json.loads(raw)
            native = self.native_identity
            if (
                native.get("session_user") != "postgres"
                or native.get("current_user") != "postgres"
                or native.get("superuser") is not True
                or native.get("database") != "codex"
                or native.get("instance") != receipt["instance"]
                or type(native.get("database_oid")) is not int
                or native["database_oid"] <= 0
                or type(native.get("backend_pid")) is not int
                or native["backend_pid"] <= 0
                or type(native.get("system_identifier")) is not str
                or not native["system_identifier"].isdecimal()
                or int(native["system_identifier"]) <= 0
            ):
                raise ServiceError("administration_native_identity_refused")
            final_engine = (
                self._execute(["info", "--format", "{{.ID}} "]).decode().strip()
            )
            if final_engine != self.engine_identity:
                raise ServiceError("administration_engine_changed")
        except AdministrativeRefusal:
            # Child frame already owns its actual causes/result. Do not cache an
            # outward error which itself owns this original frame.
            self.causes["worker_refusal"] = ServiceError("administration_child_refused")
        except BaseException as error:
            self.causes["worker"] = error
        finally:
            self._body_terminal = True

    def finish_original(self):
        # No deadlines, termination, replacement or is_alive-only completion. An
        # interrupted observer retains the SAME original worker and every child.
        with self._join:
            original = self._thread
            if original is None or not self._started:
                raise AdministrativeRefusal(self)
            if self._join_attempts == len(self._join_failures):
                raise AdministrativeRefusal(self)
            reserved = self._join_attempts
            self._join_attempts += 1
            try:
                original.join()
                self._joined = True
            except BaseException as error:
                self._join_failures[reserved] = error
                raise AdministrativeRefusal(self) from None
            if not self._body_terminal or any(
                not child.terminal for child in self.children
            ):
                raise AdministrativeRefusal(self)
            if self.causes or any(error is not None for error in self._join_failures):
                raise AdministrativeRefusal(self)
            self._hold = None
            # Intentionally no returned administrative/exclusive authority token.
