"""A small Server-Sent Events parser (the WHATWG event-stream format) over byte chunks."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

__all__ = ["Event", "read_sse"]


@dataclass(frozen=True)
class Event:
    event: str
    data: str
    id: str | None = None


def _lines(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Lines ending in CRLF, LF or CR, with a CRLF split across chunks counted once."""
    buf = b""
    skip_lf = False
    for chunk in chunks:
        if not chunk:
            continue
        if skip_lf:
            skip_lf = False
            if chunk.startswith(b"\n"):
                chunk = chunk[1:]
        buf += chunk
        start = 0
        n = len(buf)
        while start < n:
            cr = buf.find(b"\r", start)
            lf = buf.find(b"\n", start)
            if cr < 0 and lf < 0:
                break
            if cr >= 0 and (lf < 0 or cr < lf):
                yield buf[start:cr]
                if cr + 1 < n:
                    start = cr + 2 if buf[cr + 1 : cr + 2] == b"\n" else cr + 1
                else:
                    start = cr + 1
                    skip_lf = True
            else:
                yield buf[start:lf]
                start = lf + 1
        buf = buf[start:]


def read_sse(chunks: Iterable[bytes], *, on_comment: Callable[[str], None] | None = None) -> Iterator[Event]:
    """Yields each dispatched event. Comments go to `on_comment` (text after the colon), if given;
    a frame without data isn't dispatched, nor is an unterminated frame at the end of the stream."""
    event = ""
    data: list[str] = []
    event_id: str | None = None
    first = True
    for raw in _lines(chunks):
        line = raw.decode("utf-8", errors="replace")
        if first:
            first = False
            line = line.removeprefix("﻿")
        if line == "":
            if data:
                yield Event(event=event or "message", data="\n".join(data), id=event_id)
            event, data, event_id = "", [], None
            continue
        if line.startswith(":"):
            if on_comment is not None:
                on_comment(line[1:].removeprefix(" "))
            continue
        field, sep, value = line.partition(":")
        if sep:
            value = value.removeprefix(" ")
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)
        elif field == "id" and "\0" not in value:
            event_id = value
