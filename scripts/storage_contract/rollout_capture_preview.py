"""Bounded, path-free preview of rollout files in a user-supplied snapshot.

This does not prove that the snapshot was fenced or that all durable files were
captured. Compressed headers can be inspected by an explicitly supplied helper.
"""

import datetime
import json
import os
import re
import select
import signal
import stat
import subprocess
import tempfile
import time
import uuid
from collections import defaultdict
from pathlib import Path

from .records import require

MAX_ENTRIES = 100_000
MAX_DIRECTORIES = 10_000
MAX_HEADER_BYTES = 64 * 1024
MAX_EXAMPLES = 32
MAX_COMPRESSED_REQUESTS = 1024
MAX_HELPER_OUTPUT = 512 * 1024
HELPER_TIMEOUT_SECONDS = 30
UNRESOLVED_CODES = {
    "compressed_limit",
    "decode_error",
    "decoded_limit",
    "invalid_metadata",
    "io_error",
    "missing_metadata",
    "not_regular",
    "symlink",
}
NAME = re.compile(
    r"rollout-(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})-"
    r"([0-9a-f-]{36})(?:_([0-9a-f-]{36}))?\.jsonl(\.zst)?"
)


def _uuid(value):
    if type(value) is not str:
        return None
    try:
        return value if str(uuid.UUID(value)) == value else None
    except ValueError:
        return None


def _name_parts(name):
    match = NAME.fullmatch(name)
    if match is None:
        return None
    try:
        datetime.datetime.strptime(match[1], "%Y-%m-%dT%H-%M-%S")
    except ValueError:
        return None
    thread_id = _uuid(match[2])
    rollout_id = _uuid(match[3]) if match[3] else thread_id
    if thread_id is None or rollout_id is None:
        return None
    return thread_id, rollout_id, "compressed" if match[4] else "plain", match[1][:10]


def _header(path):
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        require(stat.S_ISREG(os.fstat(descriptor).st_mode), "input_not_regular_file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            data = stream.read(MAX_HEADER_BYTES + 1)
    finally:
        os.close(descriptor)
    for line in data[:MAX_HEADER_BYTES].splitlines():
        if not line or len(line) >= MAX_HEADER_BYTES:
            continue
        try:
            record = json.loads(line)
        except (UnicodeDecodeError, ValueError):
            continue
        if type(record) is not dict or record.get("type") != "session_meta":
            continue
        payload = record.get("payload")
        if type(payload) is not dict:
            return None
        thread_id = _uuid(payload.get("id"))
        history_base = payload.get("history_base")
        if history_base is None:
            return thread_id, None
        if type(history_base) is not dict:
            return None
        ancestor = _uuid(history_base.get("thread_id"))
        return (thread_id, ancestor) if ancestor is not None else None
    return None


def _examples(values):
    ordered = sorted(values)
    return {"count": len(ordered), "examples": ordered[:MAX_EXAMPLES]}


class _WindowsJob:
    """Own the helper process tree and kill descendants when its job closes."""

    def __init__(self):
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        self._ctypes = ctypes
        self._wintypes = wintypes
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32 = self._kernel32
        kernel32.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = (
            wintypes.HANDLE,
            wintypes.INT,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = (
            wintypes.HANDLE,
            wintypes.HANDLE,
        )
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL

        self._handle = kernel32.CreateJobObjectW(None, None)
        if not self._handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = ExtendedLimitInformation()
        limits.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
        if not kernel32.SetInformationJobObject(
            self._handle,
            9,  # JobObjectExtendedLimitInformation
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        ):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def assign_and_resume(self, process):
        ctypes = self._ctypes
        wintypes = self._wintypes
        kernel32 = self._kernel32
        if not kernel32.AssignProcessToJobObject(self._handle, process._handle):
            raise ctypes.WinError(ctypes.get_last_error())

        class ThreadEntry32(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ThreadID", wintypes.DWORD),
                ("th32OwnerProcessID", wintypes.DWORD),
                ("tpBasePri", ctypes.c_long),
                ("tpDeltaPri", ctypes.c_long),
                ("dwFlags", wintypes.DWORD),
            ]

        kernel32.CreateToolhelp32Snapshot.argtypes = (wintypes.DWORD, wintypes.DWORD)
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Thread32First.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(ThreadEntry32),
        )
        kernel32.Thread32First.restype = wintypes.BOOL
        kernel32.Thread32Next.argtypes = (
            wintypes.HANDLE,
            ctypes.POINTER(ThreadEntry32),
        )
        kernel32.Thread32Next.restype = wintypes.BOOL
        kernel32.OpenThread.argtypes = (
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.DWORD,
        )
        kernel32.OpenThread.restype = wintypes.HANDLE
        kernel32.ResumeThread.argtypes = (wintypes.HANDLE,)
        kernel32.ResumeThread.restype = wintypes.DWORD

        snapshot = kernel32.CreateToolhelp32Snapshot(0x00000004, 0)
        if snapshot == wintypes.HANDLE(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        thread_handle = None
        try:
            entry = ThreadEntry32()
            entry.dwSize = ctypes.sizeof(entry)
            found = kernel32.Thread32First(snapshot, ctypes.byref(entry))
            while found:
                if entry.th32OwnerProcessID == process.pid:
                    thread_handle = kernel32.OpenThread(
                        0x0002, False, entry.th32ThreadID
                    )
                    if not thread_handle:
                        raise ctypes.WinError(ctypes.get_last_error())
                    if kernel32.ResumeThread(thread_handle) == 0xFFFFFFFF:
                        raise ctypes.WinError(ctypes.get_last_error())
                    return
                entry.dwSize = ctypes.sizeof(entry)
                found = kernel32.Thread32Next(snapshot, ctypes.byref(entry))
            raise OSError("suspended helper thread was not found")
        finally:
            if thread_handle:
                kernel32.CloseHandle(thread_handle)
            kernel32.CloseHandle(snapshot)

    def terminate(self):
        return bool(self._kernel32.TerminateJobObject(self._handle, 1))

    def close(self):
        if self._handle:
            handle, self._handle = self._handle, None
            self._kernel32.CloseHandle(handle)


def _start_helper_process(command, request_stream):
    options = {
        "stdin": request_stream,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.DEVNULL,
        "bufsize": 0,
    }
    if os.name != "nt":
        return subprocess.Popen(command, start_new_session=True, **options), None

    job = _WindowsJob()
    process = None
    try:
        process = subprocess.Popen(
            command,
            creationflags=getattr(subprocess, "CREATE_SUSPENDED", 0x00000004),
            **options,
        )
        job.assign_and_resume(process)
        return process, job
    except BaseException:
        if process is not None:
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        job.close()
        raise


def _terminate_helper_tree(process, owner):
    if owner is not None and owner.terminate():
        return
    if os.name == "nt":
        try:
            process.kill()
        except OSError:
            pass
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except OSError:
        try:
            process.kill()
        except OSError:
            pass


def _windows_stdout_reader(process):
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.PeekNamedPipe.argtypes = (
        wintypes.HANDLE,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
    )
    kernel32.PeekNamedPipe.restype = wintypes.BOOL
    kernel32.ReadFile.argtypes = (
        wintypes.HANDLE,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p,
    )
    kernel32.ReadFile.restype = wintypes.BOOL
    handle = wintypes.HANDLE(msvcrt.get_osfhandle(process.stdout.fileno()))

    def read_available(maximum):
        available = wintypes.DWORD()
        if not kernel32.PeekNamedPipe(
            handle, None, 0, None, ctypes.byref(available), None
        ):
            error = ctypes.get_last_error()
            if error in (109, 232):  # broken pipe or no pipe instance
                return b""
            raise ctypes.WinError(error)
        if available.value == 0:
            return None
        size = min(maximum, available.value)
        buffer = ctypes.create_string_buffer(size)
        received = wintypes.DWORD()
        if not kernel32.ReadFile(handle, buffer, size, ctypes.byref(received), None):
            error = ctypes.get_last_error()
            if error in (109, 232):
                return b""
            raise ctypes.WinError(error)
        return buffer.raw[: received.value] if received.value else None

    return read_available


def _helper_exited(process):
    if os.name == "nt":
        return process.poll() is not None
    try:
        status = os.waitid(
            os.P_PID,
            process.pid,
            os.WEXITED | os.WNOHANG | os.WNOWAIT,
        )
    except InterruptedError:
        return False
    return status is not None and status.si_pid != 0


def _posix_waitid_supported():
    return all(
        hasattr(os, name)
        for name in ("waitid", "P_PID", "WEXITED", "WNOHANG", "WNOWAIT")
    )


def _capture_helper_output(command, requests):
    if os.name != "nt" and not _posix_waitid_supported():
        return None, None, "helper_unavailable"
    with tempfile.TemporaryFile(mode="w+b") as request_stream:
        request_stream.write(requests)
        request_stream.seek(0)
        process, owner = _start_helper_process(command, request_stream)
        output = bytearray()
        deadline = time.monotonic() + HELPER_TIMEOUT_SECONDS
        ownership_lost = False
        try:
            read_available = (
                _windows_stdout_reader(process)
                if os.name == "nt"
                else lambda maximum: (
                    os.read(process.stdout.fileno(), maximum)
                    if select.select([process.stdout], [], [], 0)[0]
                    else None
                )
            )
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _stop_helper_process(process, owner)
                    return None, None, "helper_timeout"
                chunk = read_available(
                    min(64 * 1024, MAX_HELPER_OUTPUT + 1 - len(output))
                )
                if chunk == b"":
                    if _helper_exited(process):
                        _stop_helper_process(process, owner)
                        return process.returncode, bytes(output), None
                elif chunk:
                    output.extend(chunk)
                    if len(output) > MAX_HELPER_OUTPUT:
                        _stop_helper_process(process, owner)
                        return None, None, "helper_output_limit"
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        except ChildProcessError:
            ownership_lost = True
            return None, None, "helper_failed"
        except OSError:
            _stop_helper_process(process, owner)
            return None, None, "helper_failed"
        finally:
            if owner is not None:
                owner.close()
            if not ownership_lost and process.returncode is None:
                _stop_helper_process(process, None)
            process.stdout.close()


def _stop_helper_process(process, owner):
    _terminate_helper_tree(process, owner)
    if owner is not None:
        owner.close()
    process.stdout.close()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except OSError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def _compressed_headers(home, helper, candidates, *, _helper_args=()):
    """Return one validated batch, or a path-free code with no partial results."""
    if len(candidates) > MAX_COMPRESSED_REQUESTS:
        return None, "request_limit"
    executable = Path(helper)
    try:
        if not executable.is_absolute() or not stat.S_ISREG(
            os.lstat(executable).st_mode
        ):
            return None, "helper_unavailable"
        requests = b"".join(
            (json.dumps({"relative_path": relative}) + "\n").encode("utf-8")
            for relative, _, _ in candidates
        )
        command = [
            str(executable),
            *_helper_args,
            "--snapshot-home",
            str(home.absolute()),
        ]
        returncode, data, error = _capture_helper_output(command, requests)
        if error is not None:
            return None, error
        if returncode != 0:
            return None, "helper_failed"
        if data is None:
            return None, "helper_failed"
        if len(data) > MAX_HELPER_OUTPUT:
            return None, "helper_output_limit"
        if data and not data.endswith(b"\n"):
            return None, "helper_protocol"
        lines = data.splitlines()
    except OSError:
        return None, "helper_unavailable"
    if len(lines) != len(candidates):
        return None, "helper_protocol"
    verified = []
    for line, (_, thread_id, rollout_id) in zip(lines, candidates):
        try:
            response = json.loads(line, object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, ValueError, RecursionError):
            return None, "helper_protocol"
        if type(response) is not dict:
            return None, "helper_protocol"
        if response.get("status") == "ok":
            if set(response) != {"status", "thread_id", "ancestor_rollout_id"}:
                return None, "helper_protocol"
            ancestor = response["ancestor_rollout_id"]
            if _uuid(response["thread_id"]) != thread_id or (
                ancestor is not None and _uuid(ancestor) is None
            ):
                return None, "helper_protocol"
            verified.append((rollout_id, ancestor))
        elif response.get("status") == "unresolved":
            if (
                set(response) != {"status", "code"}
                or type(response["code"]) is not str
                or response["code"] not in UNRESOLVED_CODES
            ):
                return None, "helper_protocol"
            verified.append((rollout_id, None, "unresolved"))
        else:
            return None, "helper_protocol"
    return verified, None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def preview(snapshot_home, *, compressed_header_helper=None):
    """Report physical copies and direct ancestry without selecting authority."""
    home = Path(snapshot_home)
    require(stat.S_ISDIR(os.lstat(home).st_mode), "snapshot_not_directory")
    files = defaultdict(list)
    lineage = set()
    invalid_headers = set()
    unknown_entries = 0
    compressed_headers_unknown = 0
    compressed_candidates = []
    scanned_entries = 0
    scanned_directories = 0
    collection_present = {}

    for collection, maximum_depth in (("active", 3), ("archive", 0)):
        root = home / ("sessions" if collection == "active" else "archived_sessions")
        try:
            root_mode = os.lstat(root).st_mode
        except FileNotFoundError:
            collection_present[collection] = False
            continue
        require(stat.S_ISDIR(root_mode), "collection_not_directory")
        collection_present[collection] = True
        stack = [(root, 0)]
        while stack:
            directory, depth = stack.pop()
            require(stat.S_ISDIR(os.lstat(directory).st_mode), "collection_changed")
            scanned_directories += 1
            require(scanned_directories <= MAX_DIRECTORIES, "too_many_directories")
            with os.scandir(directory) as entries:
                for entry in entries:
                    scanned_entries += 1
                    require(scanned_entries <= MAX_ENTRIES, "too_many_entries")
                    mode = entry.stat(follow_symlinks=False).st_mode
                    if stat.S_ISDIR(mode):
                        if depth < maximum_depth:
                            stack.append((Path(entry.path), depth + 1))
                        else:
                            unknown_entries += 1
                        continue
                    if not stat.S_ISREG(mode):
                        unknown_entries += 1
                        continue
                    parts = _name_parts(entry.name)
                    if parts is None or (collection == "active" and depth != 3):
                        unknown_entries += 1
                        continue
                    thread_id, rollout_id, representation, date = parts
                    if collection == "active" and directory.relative_to(
                        root
                    ).parts != tuple(date.split("-")):
                        unknown_entries += 1
                        continue
                    files[rollout_id].append((collection, representation))
                    if representation == "compressed":
                        compressed_candidates.append(
                            (
                                Path(entry.path).relative_to(home).as_posix(),
                                thread_id,
                                rollout_id,
                            )
                        )
                        continue
                    header = _header(entry.path)
                    if header is None or header[0] != thread_id:
                        invalid_headers.add(rollout_id)
                    elif header[1] is not None:
                        lineage.add((rollout_id, header[1]))

    compressed_header_status = "not_requested"
    compressed_header_code = None
    compressed_headers_unknown = len(compressed_candidates)
    if compressed_header_helper is not None and compressed_candidates:
        verified, compressed_header_code = _compressed_headers(
            home, compressed_header_helper, compressed_candidates
        )
        compressed_header_status = (
            "processed" if verified is not None else "unavailable"
        )
        if verified is not None:
            compressed_headers_unknown = 0
            for result in verified:
                if len(result) == 3:
                    compressed_headers_unknown += 1
                elif result[1] is not None:
                    lineage.add(result)

    duplicate_ids = {
        rollout_id for rollout_id, copies in files.items() if len(copies) > 1
    }
    sibling_ids = {
        rollout_id
        for rollout_id, copies in files.items()
        if any(
            (where, "plain") in copies and (where, "compressed") in copies
            for where in ("active", "archive")
        )
    }
    active_archive_ids = {
        rollout_id
        for rollout_id, copies in files.items()
        if {where for where, _ in copies} == {"active", "archive"}
    }
    missing = {
        f"{child}:{ancestor}" for child, ancestor in lineage if ancestor not in files
    }
    ambiguous = {
        f"{child}:{ancestor}"
        for child, ancestor in lineage
        if ancestor in duplicate_ids
    }
    return {
        "status": "partial",
        "capture_complete": False,
        "activation_permitted": False,
        "collection_present": collection_present,
        "scanned_entries": scanned_entries,
        "scanned_directories": scanned_directories,
        "canonical_files": sum(map(len, files.values())),
        "unknown_entries": unknown_entries,
        "invalid_plain_headers": _examples(invalid_headers),
        "compressed_headers_unknown": compressed_headers_unknown,
        "compressed_header_status": compressed_header_status,
        "compressed_header_code": compressed_header_code,
        "duplicate_rollout_ids": _examples(duplicate_ids),
        "plain_compressed_siblings": _examples(sibling_ids),
        "active_archive_copies": _examples(active_archive_ids),
        "missing_direct_ancestors": _examples(missing),
        "ambiguous_direct_ancestors": _examples(ambiguous),
    }
