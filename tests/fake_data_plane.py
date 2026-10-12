"""A fake EZGH data plane (flags.ezghcloud.com) for the provider tests: the snapshot endpoint and the
server sync stream, on a ThreadingHTTPServer bound to 127.0.0.1."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

SDK_KEY = "ezgh_fs_" + "A1b2C3d4E5" * 4 + "xyz"
SNAPSHOT_PATH = "/v1/sync/snapshot"
STREAM_PATH = "/v1/sync/stream"


def make_snapshot(version: int, flags: dict[str, tuple[bool, int]] | None = None) -> dict[str, Any]:
    """A server snapshot of boolean flags, each `key: (value, flagVersion)`, served statically."""
    flags = {"new-checkout": (True, 1)} if flags is None else flags
    return {
        "$schema": "https://flagd.dev/schema/v0/flags.json",
        "metadata": {
            "environmentId": "0199a3c2-8d31-7f02-a4b5-6c7d8e9f0a1b",
            "snapshotVersion": version,
            "compiledAt": "2026-10-12T09:00:00Z",
        },
        "flags": {
            key: {
                "state": "ENABLED",
                "variants": {"true": True, "false": False},
                "defaultVariant": "true" if value else "false",
                "metadata": {"flagVersion": flag_version, "enabled": True},
            }
            for key, (value, flag_version) in flags.items()
        },
    }


@dataclass
class Recorded:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    at: float = field(default_factory=time.monotonic)

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None


@dataclass
class _Failure:
    status: int
    retry_after: int | None
    paths: tuple[str, ...]
    times: int | None  # None: until cleared


class FakeDataPlane:
    def __init__(self, snapshot: dict[str, Any] | None = None, *, heartbeat: float = 0.2) -> None:
        self.sdk_key = SDK_KEY
        self.heartbeat = heartbeat
        self.requests: list[Recorded] = []
        self._cond = threading.Condition()
        self._snapshot = snapshot if snapshot is not None else make_snapshot(1)
        self._end_gen = 0
        self._end_reason = ""
        self._drop_gen = 0
        self._closed = False
        self._failures: list[_Failure] = []
        self.open_streams = 0
        self.stream_content_type = "text/event-stream; charset=utf-8"
        self.stream_closes_at_once = False  # answer 200, then close without a byte
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                fake._handle(self, "GET")

            def do_POST(self) -> None:
                fake._handle(self, "POST")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"

    # ---- controls

    @property
    def version(self) -> int:
        with self._cond:
            return int(self._snapshot["metadata"]["snapshotVersion"])

    def set_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Serves this snapshot from now on and pushes it to open streams."""
        with self._cond:
            self._snapshot = snapshot
            self._cond.notify_all()

    def end_streams(self, reason: str) -> None:
        """Ends every open stream with `event: end` and this reason."""
        with self._cond:
            self._end_gen += 1
            self._end_reason = reason
            self._cond.notify_all()

    def drop_streams(self) -> None:
        """Closes every open stream without an end event."""
        with self._cond:
            self._drop_gen += 1
            self._cond.notify_all()

    def revoke(self) -> None:
        """The key stops working (401 from now on) and open streams end with `revoked`."""
        with self._cond:
            self.sdk_key = "revoked"
        self.end_streams("revoked")

    def fail(
        self,
        status: int,
        *,
        retry_after: int | None = None,
        paths: tuple[str, ...] = (SNAPSHOT_PATH, STREAM_PATH),
        times: int | None = None,
    ) -> None:
        """Answers `status` on these paths, `times` times or until `recover`."""
        with self._cond:
            self._failures.append(_Failure(status, retry_after, paths, times))

    def recover(self) -> None:
        with self._cond:
            self._failures.clear()

    def requests_to(self, path: str) -> list[Recorded]:
        with self._cond:
            return [r for r in self.requests if r.path == path]

    def wait_for(self, predicate: Callable[[], bool], timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise AssertionError("timed out waiting for the fake data plane")
            time.sleep(0.01)

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        self._server.shutdown()
        self._server.server_close()

    # ---- serving

    def _handle(self, h: BaseHTTPRequestHandler, method: str) -> None:
        length = int(h.headers.get("Content-Length") or 0)
        body = h.rfile.read(length) if length else b""
        with self._cond:
            self.requests.append(Recorded(method, h.path, {k.lower(): v for k, v in h.headers.items()}, body))
            failure = next((f for f in self._failures if h.path in f.paths), None)
            if failure is not None and failure.times is not None:
                failure.times -= 1
                if failure.times == 0:
                    self._failures.remove(failure)
            key = self.sdk_key
        if failure is not None:
            self._error(h, failure.status, "unavailable", failure.retry_after)
            return
        if h.headers.get("Authorization") != f"Bearer {key}":
            self._error(h, 401, "unauthenticated")
            return
        if method == "GET" and h.path == SNAPSHOT_PATH:
            self._serve_snapshot(h)
        elif method == "POST" and h.path == STREAM_PATH:
            self._serve_stream(h, body)
        else:
            self._error(h, 404, "not_found")

    def _error(self, h: BaseHTTPRequestHandler, status: int, code: str, retry_after: int | None = None) -> None:
        payload = json.dumps({"error": {"code": code, "message": code}}).encode()
        h.send_response(status)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(payload)))
        if retry_after is not None:
            h.send_header("Retry-After", str(retry_after))
        h.end_headers()
        h.wfile.write(payload)

    def _serve_snapshot(self, h: BaseHTTPRequestHandler) -> None:
        with self._cond:
            snapshot = self._snapshot
        version = int(snapshot["metadata"]["snapshotVersion"])
        etag = f'"{version}"'
        if h.headers.get("If-None-Match") == etag:
            h.send_response(304)
            h.send_header("ETag", etag)
            h.end_headers()
            return
        payload = json.dumps(snapshot, separators=(",", ":")).encode()
        h.send_response(200)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(payload)))
        h.send_header("ETag", etag)
        h.end_headers()
        h.wfile.write(payload)

    def _serve_stream(self, h: BaseHTTPRequestHandler, body: bytes) -> None:
        known = (json.loads(body) if body else {}).get("version")
        h.send_response(200)
        h.send_header("Content-Type", self.stream_content_type)
        h.send_header("Cache-Control", "no-store")
        h.end_headers()
        if self.stream_closes_at_once:
            return

        def write(frame: str) -> bool:
            try:
                h.wfile.write(frame.encode())
                h.wfile.flush()
                return True
            except OSError:
                return False

        with self._cond:
            self.open_streams += 1
            end_gen, drop_gen = self._end_gen, self._drop_gen
        try:
            if not write(": open\n\n"):
                return
            sent = known
            while True:
                with self._cond:
                    snapshot = self._snapshot
                    version = int(snapshot["metadata"]["snapshotVersion"])
                    if self._closed or self._drop_gen != drop_gen:
                        return
                    if self._end_gen != end_gen:
                        reason = self._end_reason
                        end = True
                    else:
                        end = False
                if end:
                    write(f"event: end\ndata: {json.dumps({'reason': reason})}\n\n")
                    return
                if version != sent:
                    data = json.dumps(snapshot, separators=(",", ":"))
                    if not write(f"event: snapshot\nid: {version}\ndata: {data}\n\n"):
                        return
                    sent = version
                with self._cond:
                    changed = self._cond.wait_for(
                        lambda sent=sent: self._closed  # type: ignore[misc]
                        or self._drop_gen != drop_gen
                        or self._end_gen != end_gen
                        or int(self._snapshot["metadata"]["snapshotVersion"]) != sent,
                        timeout=self.heartbeat,
                    )
                if not changed and not write(": heartbeat\n\n"):
                    return
        finally:
            with self._cond:
                self.open_streams -= 1
