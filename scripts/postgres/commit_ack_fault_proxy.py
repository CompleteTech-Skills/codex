"""Single ROOT-owned fixture connection: suppress the real COMMIT reply.

This helper is not an admission authority. ROOT must retain the helper process,
protected provider certificate/key/CA and original operation before invocation.
No SQL, passwords, authentication payloads or exception messages are recorded.
"""

import argparse
import hashlib
import json
import os
import socket
import ssl
import struct
import threading

MAX_FRAME = 1024 * 1024
MAX_STREAM = 8 * 1024 * 1024
SSL_REQUEST = struct.pack("!II", 8, 80877103)


class RelayCleanupRefusal(Exception):
    def __init__(self, original, started, joined):
        super().__init__("fixture relay cleanup unresolved [originals retained]")
        self.original = original
        self.started = tuple(started)
        self.joined = tuple(joined)


def exact(stream, count):
    value = bytearray()
    while len(value) < count:
        chunk = stream.recv(min(16384, count - len(value)))
        if not chunk:
            raise EOFError("fixture transport ended")
        value.extend(chunk)
    return bytes(value)


def message(stream):
    kind = exact(stream, 1)
    header = exact(stream, 4)
    size = struct.unpack("!I", header)[0]
    if size < 4 or size > MAX_FRAME:
        raise ValueError("fixture protocol bound")
    payload = exact(stream, size - 4)
    return kind, payload, kind + header + payload


def commit_command(kind, payload):
    if kind == b"Q":
        if not payload.endswith(b"\0") or b"\0" in payload[:-1]:
            return False
        query = payload[:-1]
    elif kind == b"P":
        _, separator, rest = payload.partition(b"\0")
        if not separator:
            return False
        query, separator, parameters = rest.partition(b"\0")
        if not separator:
            return False
        if len(parameters) < 2:
            return False
        count = struct.unpack("!H", parameters[:2])[0]
        if len(parameters) != 2 + count * 4 or count != 0:
            return False
    else:
        return False
    query = query.strip()
    if query.endswith(b";"):
        query = query[:-1].strip()
    return query.upper() == b"COMMIT"


def create_receipt(path, value):
    with open(path, "x", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


class OriginalRelay:
    def __init__(self, frontend, backend, fault_mode="commit_reply"):
        if type(fault_mode) is not str or fault_mode not in (
            "commit_reply",
            "commit_request",
        ):
            raise ValueError("fixture commit fault mode refused")
        self.fault_mode = fault_mode
        self.commit_request_observed = False
        self.commit_request_forwarded = False
        self.commit_request_kind = None
        self.commit_request_sha256 = None
        self.frontend = frontend
        self.backend = backend
        self.guard = threading.Lock()
        self.armed = False
        self.suppressed = False
        self.server_idle_after_commit = False
        self.command_tag_sha256 = None
        self.commit_tag_forwarded = False
        self.causes = []
        self.tasks = []

    def client(self):
        total = 0
        try:
            # PostgreSQL StartupMessage follows SSL negotiation without a type.
            header = exact(self.frontend, 4)
            size = struct.unpack("!I", header)[0]
            if size < 8 or size > MAX_FRAME:
                raise ValueError("fixture startup bound")
            self.backend.sendall(header + exact(self.frontend, size - 4))
            while True:
                kind, payload, wire = message(self.frontend)
                total += len(wire)
                if total > MAX_STREAM:
                    raise ValueError("fixture stream bound")
                commit = commit_command(kind, payload)
                if commit:
                    with self.guard:
                        self.armed = True
                        self.commit_request_observed = True
                        self.commit_request_kind = kind.decode("ascii")
                        self.commit_request_sha256 = hashlib.sha256(wire).hexdigest()
                    if self.fault_mode == "commit_request":
                        # The original request is NEVER sent upstream. Closing
                        # is only the injected fault; native xid status remains
                        # a separate genuine diagnostic-session observation.
                        for stream in (self.frontend, self.backend):
                            try:
                                stream.shutdown(socket.SHUT_RDWR)
                            except BaseException as original:
                                with self.guard:
                                    self.causes.append(original)
                        return
                self.backend.sendall(wire)
                if commit:
                    with self.guard:
                        self.commit_request_forwarded = True
        except BaseException as original:
            with self.guard:
                self.causes.append(original)

    def server(self):
        total = 0
        try:
            while True:
                kind, payload, wire = message(self.backend)
                total += len(wire)
                if total > MAX_STREAM:
                    raise ValueError("fixture stream bound")
                with self.guard:
                    trap = self.armed and kind == b"C" and payload == b"COMMIT\0"
                    if trap:
                        self.suppressed = True
                        self.command_tag_sha256 = hashlib.sha256(wire).hexdigest()
                    suppressed = self.suppressed
                if trap:
                    # The driver never receives this actual server command tag.
                    self.frontend.shutdown(socket.SHUT_RDWR)
                if suppressed:
                    if kind == b"Z" and payload == b"I":
                        with self.guard:
                            self.server_idle_after_commit = True
                        return
                else:
                    self.frontend.sendall(wire)
                    if kind == b"C" and payload == b"COMMIT\0":
                        with self.guard:
                            self.commit_tag_forwarded = True
        except BaseException as original:
            with self.guard:
                self.causes.append(original)

    def run(self):
        # Actual original thread handles retained before their bodies first run.
        gate = threading.Event()
        self.tasks = [
            threading.Thread(target=lambda body=body: (gate.wait(), body()))
            for body in (self.client, self.server)
        ]
        started = []
        joined = []
        cleanup_refused = False
        try:
            try:
                for task in self.tasks:
                    try:
                        task.start()
                    except BaseException:
                        # A failing start may still have created this original.
                        if task.ident is not None:
                            started.append(task)
                        raise
                    started.append(task)
            except BaseException as original:
                with self.guard:
                    self.causes.append(original)
                for stream in (self.frontend, self.backend):
                    try:
                        stream.shutdown(socket.SHUT_RDWR)
                    except BaseException as cleanup:
                        with self.guard:
                            self.causes.append(cleanup)
            finally:
                gate.set()  # Release EVERY actually started original even on refusal.
            for task in started:
                try:
                    task.join()  # Timeout/idle/driver error is not a physical join.
                    joined.append(task)
                except BaseException as original:
                    cleanup_refused = True
                    with self.guard:
                        self.causes.append(original)
        finally:
            for stream in (self.frontend, self.backend):
                try:
                    stream.close()
                except BaseException as original:
                    cleanup_refused = True
                    with self.guard:
                        self.causes.append(original)
        if cleanup_refused or len(joined) != len(started):
            # No CreateNew terminal report on unresolved join/close. The owning
            # exception retains every actual original and its raw causes.
            raise RelayCleanupRefusal(self, started, joined)
        with self.guard:
            return {
                "kind": (
                    "fixture-commit-reply-suppression"
                    if self.fault_mode == "commit_reply"
                    else "fixture-commit-request-suppression"
                ),
                "fault_mode": self.fault_mode,
                "client_commit_request_observed": self.commit_request_observed,
                "client_commit_request_forwarded": self.commit_request_forwarded,
                "client_commit_request_kind": self.commit_request_kind,
                "client_commit_request_sha256": self.commit_request_sha256,
                "release_authority": False,
                "server_commit_tag_observed": self.suppressed,
                "driver_commit_tag_forwarded": self.commit_tag_forwarded,
                "server_idle_observed": self.server_idle_after_commit,
                "command_tag_frame_sha256": self.command_tag_sha256,
                "started_original_threads": len(started),
                "original_threads_joined": len(joined) == len(started)
                and all(not task.is_alive() for task in joined),
                "all_original_threads_started": len(started) == len(self.tasks),
                "cleanup_refused": cleanup_refused,
                "cause_types": [type(cause).__name__ for cause in self.causes],
            }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", required=True)
    args = parser.parse_args()
    with open(args.inputs, encoding="utf-8") as source:
        inputs = json.load(source)
    fault_mode = inputs.get("fault_mode", "commit_reply")
    if type(fault_mode) is not str or fault_mode not in (
        "commit_reply",
        "commit_request",
    ):
        raise ValueError("fixture commit fault mode refused")
    port = inputs["provider_port"]
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("fixture provider port")
    upstream_context = ssl.create_default_context(cafile=inputs["ca_file"])
    downstream_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    downstream_context.load_cert_chain(
        inputs["server_certificate_file"], inputs["server_key_file"]
    )
    # The SAME protected provider leaf preserves SCRAM tls-server-end-point.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        create_receipt(
            inputs["ready_file"],
            {
                "kind": "fixture-commit-relay-ready",
                "release_authority": False,
                "pid": os.getpid(),
                "port": listener.getsockname()[1],
                "fault_mode": fault_mode,
            },
        )
        accepted, _ = listener.accept()
        with accepted:
            accepted.settimeout(10)
            if exact(accepted, 8) != SSL_REQUEST:
                raise ValueError("fixture requires TLS")
            accepted.sendall(b"S")
            with downstream_context.wrap_socket(accepted, server_side=True) as frontend:
                with socket.create_connection(
                    ("localhost", port), timeout=10
                ) as connected:
                    connected.sendall(SSL_REQUEST)
                    if exact(connected, 1) != b"S":
                        raise ValueError("provider requires TLS")
                    with upstream_context.wrap_socket(
                        connected, server_hostname="localhost"
                    ) as backend:
                        with open(
                            inputs["server_certificate_file"], encoding="ascii"
                        ) as certificate:
                            expected_leaf = ssl.PEM_cert_to_DER_cert(certificate.read())
                        if backend.getpeercert(binary_form=True) != expected_leaf:
                            raise ValueError("provider leaf association refused")
                        backend.settimeout(10)
                        relay = OriginalRelay(frontend, backend, fault_mode)
                        result = relay.run()
                        result.update(
                            {
                                "pid": os.getpid(),
                                "relay_port": listener.getsockname()[1],
                                "provider_port": port,
                                "provider_leaf_sha256": hashlib.sha256(
                                    expected_leaf
                                ).hexdigest(),
                            }
                        )
                        create_receipt(inputs["result_file"], result)


if __name__ == "__main__":
    main()
