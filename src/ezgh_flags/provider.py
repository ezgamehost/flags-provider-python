"""EzghProvider: an OpenFeature server provider for EZGH Feature Flags.

It loads the environment's snapshot from the data plane (`GET /v1/sync/snapshot`), evaluates
every flag locally with flagd's core, and follows `POST /v1/sync/stream` for new snapshots. When
streaming keeps failing it also polls the snapshot every 30 seconds with `If-None-Match`, while
retrying the stream with exponential backoff and jitter.
"""

from __future__ import annotations

import contextlib
import email.utils
import http.client
import json
import logging
import random
import socket
import ssl
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any, Literal
from urllib.parse import urlsplit

from openfeature.evaluation_context import EvaluationContext
from openfeature.event import ProviderEventDetails
from openfeature.exception import ErrorCode, GeneralError, ProviderFatalError
from openfeature.flag_evaluation import FlagResolutionDetails, FlagValueType, Reason
from openfeature.provider import AbstractProvider, Metadata

from ._version import VERSION
from .evaluator import Evaluator, FlagType, InvalidSnapshotError, changed_keys
from .sse import read_sse

__all__ = ["DEFAULT_ENDPOINT", "EzghProvider"]

DEFAULT_ENDPOINT = "https://flags.ezghcloud.com"
_SNAPSHOT_PATH = "/v1/sync/snapshot"
_STREAM_PATH = "/v1/sync/stream"
_USER_AGENT = f"ezgh-flags-openfeature-python/{VERSION}"
_MAX_RETRY_AFTER = 3600.0

logger = logging.getLogger("ezgh_flags")

_State = Literal["not_ready", "ready", "stale", "error", "fatal"]


class _Fatal(Exception):
    """The data plane refused the key (401/403): retrying can't help."""


class _Transient(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


_FATAL_END_REASONS = ("revoked", "environmentDeleted")
"""Stream end reasons after which the key can't work again."""


class _Run:
    """One initialize's sync thread: its own stop signal, socket and retry state, so an older
    thread (a shutdown that raced initialize, or a join that timed out) never affects a newer one."""

    def __init__(self) -> None:
        self.stop = threading.Event()
        self.failures = 0
        self.disconnected_since: float | None = None
        self.next_poll = 0.0
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None

    def attach(self, sock: socket.socket) -> bool:
        """Records the stream's socket; False if the run already stopped."""
        with self._lock:
            if self.stop.is_set():
                return False
            self._sock = sock
            return True

    def detach(self) -> None:
        with self._lock:
            self._sock = None

    def close(self) -> None:
        """Stops the run and unblocks a stream read in progress."""
        with self._lock:
            self.stop.set()
            sock = self._sock
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)


class EzghProvider(AbstractProvider):
    """OpenFeature provider for EZGH Feature Flags (server side).

    `sdk_key` is a server SDK key (`ezgh_fs_…`). `bootstrap_path` names a snapshot file (from
    `ezgh flags export`) that makes the provider ready without the network; with `offline=True`
    it uses only that file and never calls the data plane. `timeout` bounds each HTTP request's
    connect and read (the stream's reads use `stream_read_timeout`) and the time `initialize`
    spends retrying.
    """

    # Timings. The defaults are the design's; tests shrink them.
    poll_interval: float = 30.0
    """Seconds between snapshot polls while the stream is failing."""
    stale_after: float = 30.0
    """Seconds without a connection before the provider reports PROVIDER_STALE."""
    min_backoff: float = 1.0
    max_backoff: float = 60.0
    stream_failures_before_poll: int = 3
    stream_read_timeout: float = 45.0
    """The data plane sends a heartbeat every 15 s; a stream silent for this long is dead."""

    def __init__(
        self,
        sdk_key: str,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        bootstrap_path: str | None = None,
        offline: bool = False,
        timeout: float = 10.0,
    ) -> None:
        super().__init__()
        if offline and not bootstrap_path:
            raise ValueError("offline=True needs a bootstrap_path to evaluate from")
        url = urlsplit(endpoint)
        if url.scheme not in ("http", "https") or not url.hostname:
            raise ValueError(f"endpoint must be an http(s) URL, not {endpoint!r}")
        self._sdk_key = sdk_key
        self._scheme = url.scheme
        self._host = url.hostname
        self._port = url.port
        self._base = url.path.rstrip("/")
        self._bootstrap_path = bootstrap_path
        self._offline = offline
        self._timeout = timeout

        self._lock = threading.Lock()
        self._evaluator: Evaluator | None = None
        self._synced = False  # the evaluator came from the data plane (not a bootstrap file)
        self._state: _State = "not_ready"
        self._run: _Run | None = None  # the current initialize's sync run
        self._thread: threading.Thread | None = None

    def get_metadata(self) -> Metadata:
        return Metadata(name="ezgh-flags")

    # ---- lifecycle

    def initialize(self, evaluation_context: EvaluationContext) -> None:
        run = _Run()
        with self._lock:
            previous, self._run = self._run, run
        if previous is not None:
            previous.close()
        if self._bootstrap_path is not None:
            try:
                with open(self._bootstrap_path, "rb") as f:
                    evaluator = Evaluator(f.read())
            except (OSError, InvalidSnapshotError) as err:
                self._state = "fatal"
                raise ProviderFatalError(f"can't load bootstrap_path {self._bootstrap_path}: {err}") from err
            with self._lock:
                self._evaluator = evaluator
                self._synced = False
        if self._offline:
            self._state = "ready"
            return

        failure: _Transient | None = None
        if self._evaluator is None:
            deadline = time.monotonic() + self._timeout
            attempt = 0
            while True:
                attempt += 1
                try:
                    body = self._get_snapshot(conditional=False)
                    if body is None or not self._install(run, body, emit=False):
                        raise _Transient("the data plane answered an invalid snapshot")
                    break
                except _Fatal as err:
                    self._state = "fatal"
                    raise ProviderFatalError(str(err)) from err
                except _Transient as err:
                    delay = self._delay(attempt, err.retry_after)
                    if run.stop.is_set() or time.monotonic() + delay > deadline:
                        failure = err
                        break
                    logger.info("ezgh-flags: snapshot fetch failed (%s); retrying in %.1fs", err, delay)
                    if run.stop.wait(delay):
                        failure = err
                        break

        if run.stop.is_set():
            raise GeneralError("the provider was shut down during initialize")
        self._state = "error" if failure is not None else "ready"
        thread = threading.Thread(target=self._sync, args=(run,), name="ezgh-flags-sync", daemon=True)
        with self._lock:
            self._thread = thread
        thread.start()
        if failure is not None:
            # Not fatal: the sync thread keeps trying and emits PROVIDER_READY once loaded.
            raise GeneralError(f"can't load the flags snapshot: {failure}")

    def shutdown(self) -> None:
        with self._lock:
            run, self._run = self._run, None
            thread, self._thread = self._thread, None
        if run is not None:
            run.close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)

    # ---- evaluation (local, no network)

    def _resolve(
        self, flag_type: FlagType, key: str, default: Any, context: EvaluationContext | None
    ) -> FlagResolutionDetails[Any]:
        with self._lock:
            evaluator = self._evaluator
        if evaluator is None:
            return FlagResolutionDetails(
                value=default,
                error_code=ErrorCode.PROVIDER_NOT_READY,
                error_message="no flags snapshot is loaded yet",
                reason=Reason.ERROR,
            )
        return evaluator.resolve(flag_type, key, default, context)

    def resolve_boolean_details(
        self, flag_key: str, default_value: bool, evaluation_context: EvaluationContext | None = None
    ) -> FlagResolutionDetails[bool]:
        return self._resolve("boolean", flag_key, default_value, evaluation_context)

    def resolve_string_details(
        self, flag_key: str, default_value: str, evaluation_context: EvaluationContext | None = None
    ) -> FlagResolutionDetails[str]:
        return self._resolve("string", flag_key, default_value, evaluation_context)

    def resolve_integer_details(
        self, flag_key: str, default_value: int, evaluation_context: EvaluationContext | None = None
    ) -> FlagResolutionDetails[int]:
        return self._resolve("integer", flag_key, default_value, evaluation_context)

    def resolve_float_details(
        self, flag_key: str, default_value: float, evaluation_context: EvaluationContext | None = None
    ) -> FlagResolutionDetails[float]:
        return self._resolve("float", flag_key, default_value, evaluation_context)

    def resolve_object_details(
        self,
        flag_key: str,
        default_value: Sequence[FlagValueType] | Mapping[str, FlagValueType],
        evaluation_context: EvaluationContext | None = None,
    ) -> FlagResolutionDetails[Sequence[FlagValueType] | Mapping[str, FlagValueType]]:
        return self._resolve("object", flag_key, default_value, evaluation_context)

    # ---- the sync thread (one per initialize, with its own _Run)

    def _sync(self, run: _Run) -> None:
        while not run.stop.is_set():
            try:
                self._cycle(run)
            except _Fatal as err:
                self._fatal(run, str(err))
                return
            except Exception:
                # Never exit silently: count it as a failed stream and keep going.
                logger.exception("ezgh-flags: unexpected error in the sync thread")
                self._failed(run)
                run.stop.wait(self._delay(run.failures, None))

    def _cycle(self, run: _Run) -> None:
        """One stream connection, then whatever wait its outcome calls for."""
        try:
            reason = self._stream(run)
        except _Transient as err:
            logger.info("ezgh-flags: stream failed (%s)", err)
            self._failed(run)
            self._wait_before_retry(run, err.retry_after)
            return
        if reason is None or reason == "rotate":
            return  # stopping, or a routine rotation: reconnect at once
        if reason in _FATAL_END_REASONS:
            raise _Fatal(f"the stream ended: {reason}")
        if reason == "shutdown":
            # The pod is draining: reconnect (to another pod) after a short jitter. Not a failure.
            run.stop.wait(random.uniform(0, self.min_backoff))
            return
        # An end reason this provider doesn't know: a failure, with backoff.
        logger.info("ezgh-flags: stream ended (%r)", reason)
        self._failed(run)
        self._wait_before_retry(run, None)

    def _failed(self, run: _Run) -> None:
        run.failures += 1
        if run.disconnected_since is None:
            run.disconnected_since = time.monotonic()

    def _wait_before_retry(self, run: _Run, retry_after: float | None) -> None:
        """Waits out the backoff, polling and checking staleness meanwhile."""
        resume_at = time.monotonic() + self._delay(run.failures, retry_after)
        while not run.stop.is_set():
            now = time.monotonic()
            if now >= resume_at:
                return
            polling = run.failures >= self.stream_failures_before_poll
            if polling and now >= run.next_poll:
                run.next_poll = now + self.poll_interval
                self._poll(run)
            self._check_stale(run)
            wake = resume_at
            if polling:
                wake = min(wake, run.next_poll)
            if self._state == "ready" and run.disconnected_since is not None:
                wake = min(wake, run.disconnected_since + self.stale_after)
            run.stop.wait(max(0.0, wake - time.monotonic()))

    def _poll(self, run: _Run) -> None:
        try:
            body = self._get_snapshot(conditional=True)
        except _Transient as err:
            logger.info("ezgh-flags: snapshot poll failed (%s)", err)
            if err.retry_after is not None:
                run.next_poll = max(run.next_poll, time.monotonic() + self._cap(err.retry_after))
            return
        if body is None:
            self._contact(run)
        else:
            self._install(run, body, emit=True)

    def _stream(self, run: _Run) -> str | None:
        """One stream connection. Answers the end reason, or None when stopping."""
        conn = self._connection()
        resp: http.client.HTTPResponse | None = None
        with self._lock:
            synced, evaluator = self._synced, self._evaluator
        try:
            known = {"version": evaluator.version} if synced and evaluator is not None else {}
            conn.request("POST", self._base + _STREAM_PATH, json.dumps(known), self._headers(json_body=True))
            # The connection drops its socket when a response will close it; keep a reference.
            sock = conn.sock
            if not isinstance(sock, socket.socket):
                raise _Transient("stream: no socket")
            if not run.attach(sock):
                return None
            resp = conn.getresponse()
            _check_status(resp)
            content_type = (resp.getheader("Content-Type") or "").split(";")[0].strip().lower()
            if content_type != "text/event-stream":
                raise _Transient(f"stream: unexpected Content-Type {content_type!r}")
            sock.settimeout(self.stream_read_timeout)
            opened = False

            def heard() -> None:
                # The stream counts as connected once a frame or comment arrives, not at the 200.
                nonlocal opened
                if not opened:
                    opened = True
                    run.failures = 0
                    self._contact(run)

            body = resp
            for event in read_sse(iter(lambda: body.read1(65536), b""), on_comment=lambda _: heard()):
                heard()
                if event.event == "snapshot":
                    self._install(run, event.data, emit=True)
                elif event.event == "end":
                    return _end_reason(event.data)
            if run.stop.is_set():
                return None
            raise _Transient("the stream closed without an end event")
        except (OSError, http.client.HTTPException) as err:
            if run.stop.is_set():
                return None
            raise _Transient(f"stream: {err}") from err
        finally:
            run.detach()
            if resp is not None:
                resp.close()
            conn.close()

    # ---- state

    def _install(self, run: _Run, snapshot: str | bytes, *, emit: bool) -> bool:
        """Swaps in a snapshot. False when it's invalid (the current one stays) or the run stopped.
        A snapshot older than the current one is ignored, though it still counts as contact."""
        if run.stop.is_set():
            return False
        try:
            evaluator = Evaluator(snapshot)
        except InvalidSnapshotError as err:
            logger.warning("ezgh-flags: ignoring an invalid snapshot: %s", err)
            return False
        with self._lock:
            old = self._evaluator
            keep = old is not None and self._synced and evaluator.version <= old.version
            if not keep:
                self._evaluator = evaluator
                self._synced = True
        if keep and old is not None and evaluator.version < old.version:
            logger.info("ezgh-flags: ignoring snapshot %d, older than %d", evaluator.version, old.version)
        if not keep and emit and old is not None:
            changed = changed_keys(old.fingerprints(), evaluator.fingerprints())
            self.emit_provider_configuration_changed(
                ProviderEventDetails(flags_changed=changed, metadata={"snapshotVersion": evaluator.version})
            )
        self._contact(run)
        return True

    def _contact(self, run: _Run) -> None:
        """The data plane answered: not disconnected, and ready again if it was stale or failing."""
        run.disconnected_since = None
        with self._lock:
            # "not_ready": initialize hasn't returned, and the SDK emits READY when it does.
            if run.stop.is_set() or self._evaluator is None or self._state not in ("stale", "error"):
                return
            self._state = "ready"
        self.emit_provider_ready(ProviderEventDetails())

    def _check_stale(self, run: _Run) -> None:
        since = run.disconnected_since
        if self._state != "ready" or since is None or time.monotonic() - since < self.stale_after:
            return
        with self._lock:
            if run.stop.is_set():
                return
            self._state = "stale"
        self.emit_provider_stale(ProviderEventDetails(message="lost the connection to the flags data plane"))

    def _fatal(self, run: _Run, message: str) -> None:
        with self._lock:
            if run.stop.is_set():
                return
            run.stop.set()
            self._state = "fatal"
        logger.error("ezgh-flags: %s", message)
        self.emit_provider_error(ProviderEventDetails(message=message, error_code=ErrorCode.PROVIDER_FATAL))

    def _delay(self, failures: int, retry_after: float | None) -> float:
        base = min(self.max_backoff, self.min_backoff * 2.0 ** max(0, failures - 1))
        delay = base / 2 + random.uniform(0, base / 2)
        return max(delay, self._cap(retry_after))

    def _cap(self, retry_after: float | None) -> float:
        """Retry-After, honoured up to the maximum backoff."""
        return min(retry_after or 0.0, self.max_backoff)

    # ---- HTTP (standard library only)

    def _connection(self) -> http.client.HTTPConnection:
        if self._scheme == "https":
            return http.client.HTTPSConnection(
                self._host, self._port, timeout=self._timeout, context=ssl.create_default_context()
            )
        return http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)

    def _headers(self, *, json_body: bool = False) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._sdk_key}", "User-Agent": _USER_AGENT}
        if json_body:
            headers["Content-Type"] = "application/json"
            headers["Accept"] = "text/event-stream"
        else:
            headers["Accept"] = "application/json"
        return headers

    def _get_snapshot(self, *, conditional: bool) -> bytes | None:
        """The current snapshot, or None for 304 Not Modified."""
        headers = self._headers()
        with self._lock:
            evaluator, synced = self._evaluator, self._synced
        if conditional and synced and evaluator is not None:
            headers["If-None-Match"] = f'"{evaluator.version}"'
        conn = self._connection()
        try:
            conn.request("GET", self._base + _SNAPSHOT_PATH, headers=headers)
            resp = conn.getresponse()
            if resp.status == 304:
                return None
            _check_status(resp)
            return resp.read()
        except (OSError, http.client.HTTPException) as err:
            raise _Transient(f"snapshot: {err}") from err
        finally:
            conn.close()


def _check_status(resp: http.client.HTTPResponse) -> None:
    if resp.status == 200:
        return
    detail = ""
    with contextlib.suppress(Exception):
        detail = str(json.loads(resp.read(4096))["error"]["code"])
    message = f"the flags data plane answered {resp.status}" + (f" ({detail})" if detail else "")
    if resp.status in (401, 403):
        raise _Fatal(message)
    raise _Transient(message, _retry_after(resp.getheader("Retry-After")))


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return min(float(value), _MAX_RETRY_AFTER)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return min(max(0.0, when.timestamp() - time.time()), _MAX_RETRY_AFTER)


def _end_reason(data: str) -> str:
    try:
        reason = json.loads(data).get("reason")
    except (ValueError, AttributeError):
        return ""
    return reason if isinstance(reason, str) else ""
