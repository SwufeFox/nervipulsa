"""Length-prefixed UTF-8 JSON frames for the worker control socket.

A frame is a 4-byte unsigned big-endian length followed by JSON bytes.
The default limit is 1 MiB; larger execution output belongs in an artifact file.
"""

from __future__ import annotations

import json
import socket
import struct
from typing import Any

MAX_FRAME_BYTES = 1024 * 1024
_HEADER = struct.Struct(">I")


class FrameError(ValueError):
    pass


def encode_frame(payload: dict[str, Any]) -> bytes:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(data) > MAX_FRAME_BYTES:
        raise FrameError("control frame exceeds 1 MiB")
    return _HEADER.pack(len(data)) + data


def write_frame(sock: socket.socket, payload: dict[str, Any]) -> None:
    sock.sendall(encode_frame(payload))


def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
    buffer = bytearray()
    while len(buffer) < size:
        try:
            chunk = sock.recv(size - len(buffer))
        except OSError:
            return None
        if not chunk:
            return None
        buffer.extend(chunk)
    return bytes(buffer)


def read_frame(sock: socket.socket) -> dict[str, Any] | None:
    header = _recv_exact(sock, _HEADER.size)
    if header is None:
        return None
    (size,) = _HEADER.unpack(header)
    if size > MAX_FRAME_BYTES:
        raise FrameError("control frame exceeds 1 MiB")
    body = _recv_exact(sock, size)
    if body is None:
        return None
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FrameError("control frame is not UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise FrameError("control frame must be a JSON object")
    return payload
