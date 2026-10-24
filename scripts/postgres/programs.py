"""Resolve helper executables without Windows' implicit working-directory search."""

import os
from pathlib import Path
import shutil

from state_io import ServiceError


def _windows_program(program, directories, cwd):
    requested = Path(program)
    explicit_path = (
        requested.is_absolute()
        or requested.parent != Path(".")
        or program.startswith(".")
    )
    if explicit_path:
        # A path explicitly supplied by the operator is distinct from a bare name.
        candidates = [requested if requested.is_absolute() else cwd / requested]
    else:
        candidates = []
        for entry in directories:
            directory = Path(entry.strip('"'))
            try:
                if directory.is_absolute() and directory.resolve() != cwd.resolve():
                    candidates.append(directory / program)
            except (OSError, RuntimeError):
                continue
    for candidate in candidates:
        names = (
            [candidate]
            if explicit_path or candidate.suffix
            else [candidate.with_suffix(".exe"), candidate]
        )
        for name in names:
            try:
                if name.is_file():
                    return str(name.resolve())
            except (OSError, RuntimeError):
                continue
    raise ServiceError("command_unavailable_or_timed_out")


def resolve_program(program, *, environment=None):
    if not isinstance(program, str) or not program or "\0" in program:
        raise ServiceError("command_unavailable_or_timed_out")
    if os.name == "nt":
        return _windows_program(program, os.get_exec_path(environment), Path.cwd())
    found = shutil.which(program, path=os.pathsep.join(os.get_exec_path(environment)))
    if found is None:
        raise ServiceError("command_unavailable_or_timed_out")
    return str(Path(found).resolve())
