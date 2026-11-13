"""Process-owned, nonblocking leases for a journal's individual sessions.

Lock files deliberately persist: unlinking a locked file would let a second
process lock a new inode while the original owner is still running. Closing the
owning descriptor, including on process death, releases the operating-system
lock. Call acquire/close off the event loop because they touch the filesystem.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl


class SessionBusyError(RuntimeError):
    """Another runtime owns this journal/session pair."""


class SessionLease:
    """An exclusive OS lease; each instance owns only its own descriptor."""

    def __init__(self, path: Path, descriptor: int) -> None:
        self.path = path
        self._descriptor: int | None = descriptor

    @classmethod
    def acquire(cls, journal_path: Path, session_id: str) -> SessionLease:
        canonical = journal_path.resolve()
        identity = json.dumps(
            [os.path.normcase(str(canonical)), session_id], ensure_ascii=True
        ).encode("utf-8")
        digest = hashlib.sha256(identity).hexdigest()
        directory = canonical.parent / ".nervipulsa-locks"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{digest}.lock"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if os.name == "nt":
                # Windows permits a byte-range lock beyond EOF. Keeping the
                # file empty avoids writing through another owner's lock.
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise SessionBusyError(
                    f"session {session_id!r} is already active in {canonical}"
                ) from exc
            raise
        except BaseException:
            os.close(descriptor)
            raise
        return cls(path, descriptor)

    def close(self) -> None:
        descriptor = self._descriptor
        if descriptor is not None:
            self._descriptor = None
            os.close(descriptor)
