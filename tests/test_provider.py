"""EzghProvider against the fake data plane, through the OpenFeature API (plan T1 step 6, Y1 step 4)."""

from __future__ import annotations

import itertools
import json
import queue
import socket
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from fake_data_plane import SNAPSHOT_PATH, STREAM_PATH, FakeDataPlane, make_snapshot
from openfeature import api
from openfeature.client import OpenFeatureClient
from openfeature.event import EventDetails, ProviderEvent
from openfeature.exception import ErrorCode, GeneralError, ProviderFatalError
from openfeature.flag_evaluation import Reason
from openfeature.provider import ProviderStatus

from ezgh_flags import EzghProvider


@pytest.fixture
def fake() -> Iterator[FakeDataPlane]:
    f = FakeDataPlane()
    yield f
    f.close()


@pytest.fixture(autouse=True)
def _clear_providers() -> Iterator[None]:
    yield
    api.clear_providers()


def fast(provider: EzghProvider) -> EzghProvider:
    """Shrinks the provider's timers for tests."""
    provider.min_backoff = 0.05
    provider.max_backoff = 0.2
    provider.poll_interval = 0.2
    provider.stale_after = 0.3
    provider.stream_read_timeout = 2.0
    return provider


class Events:
    """Collects a client's provider events."""

    def __init__(self, client: OpenFeatureClient) -> None:
        self.q: queue.Queue[tuple[ProviderEvent, EventDetails]] = queue.Queue()
        for event in ProviderEvent:
            client.add_handler(event, self._handler(event))

    def _handler(self, event: ProviderEvent) -> Callable[[EventDetails], None]:
        return lambda details: self.q.put((event, details))

    def wait(self, event: ProviderEvent, timeout: float = 5.0) -> EventDetails:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"no {event} within {timeout}s")
            try:
                got, details = self.q.get(timeout=remaining)
            except queue.Empty:
                continue
            if got == event:
                return details

    def drain(self) -> None:
        time.sleep(0.05)
        while not self.q.empty():
            self.q.get_nowait()


def start(fake: FakeDataPlane, *, max_backoff: float = 0.2, **kwargs: object) -> tuple[EzghProvider, OpenFeatureClient]:
    domain = str(uuid.uuid4())
    provider = fast(EzghProvider(fake.sdk_key, endpoint=fake.url, **kwargs))  # type: ignore[arg-type]
    provider.max_backoff = max_backoff
    api.set_provider_and_wait(provider, domain)
    return provider, api.get_client(domain)


def test_ready_after_initialize(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    assert client.get_provider_status() == ProviderStatus.READY
    [get] = fake.requests_to(SNAPSHOT_PATH)
    assert get.method == "GET"
    assert get.headers["authorization"] == f"Bearer {fake.sdk_key}"
    fake.wait_for(lambda: fake.open_streams == 1)
    [stream] = fake.requests_to(STREAM_PATH)
    assert stream.headers["authorization"] == f"Bearer {fake.sdk_key}"
    assert stream.json() == {"version": 1}

    before = len(fake.requests)
    for _ in range(50):
        details = client.get_boolean_details("new-checkout", False)
    assert details.value is True
    assert details.variant == "true"
    assert details.reason == Reason.STATIC
    assert details.flag_metadata == {"flagVersion": 1}
    assert len(fake.requests) == before, "evaluation is local"


def test_unknown_flag(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    details = client.get_boolean_details("nope", False)
    assert details.value is False
    assert details.error_code == ErrorCode.FLAG_NOT_FOUND


def test_disabled_reason() -> None:
    snapshot = make_snapshot(1)
    snapshot["flags"]["new-checkout"]["metadata"]["enabled"] = False
    snapshot["flags"]["new-checkout"]["defaultVariant"] = "false"
    fake = FakeDataPlane(snapshot)
    try:
        _, client = start(fake)
        details = client.get_boolean_details("new-checkout", True)
        assert details.value is False
        assert details.reason == Reason.DISABLED
    finally:
        fake.close()


def test_configuration_changed(fake: FakeDataPlane) -> None:
    fake.set_snapshot(make_snapshot(1, {"new-checkout": (True, 1), "dark-mode": (False, 4)}))
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()

    fake.set_snapshot(make_snapshot(2, {"new-checkout": (True, 1), "dark-mode": (True, 5)}))
    details = events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert details.flags_changed == ["dark-mode"]
    assert client.get_boolean_value("dark-mode", False) is True

    fake.set_snapshot(make_snapshot(3, {"dark-mode": (True, 5), "brand-new": (True, 1)}))
    details = events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert details.flags_changed == ["brand-new", "new-checkout"]
    assert client.get_boolean_details("new-checkout", False).error_code == ErrorCode.FLAG_NOT_FOUND


def test_reconnects_after_end_rotate(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.set_snapshot(make_snapshot(2))
    events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)

    fake.end_streams("rotate")
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 2)
    assert fake.requests_to(STREAM_PATH)[1].json() == {"version": 2}
    fake.wait_for(lambda: fake.open_streams == 1)
    # A rotation is routine: no stale, no extra snapshot fetch.
    assert len(fake.requests_to(SNAPSHOT_PATH)) == 1
    assert client.get_provider_status() == ProviderStatus.READY


def test_stale_when_down(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()

    fake.fail(503)
    fake.drop_streams()
    events.wait(ProviderEvent.PROVIDER_STALE)
    assert client.get_provider_status() == ProviderStatus.STALE
    # The last snapshot keeps answering.
    assert client.get_boolean_value("new-checkout", False) is True

    fake.recover()
    events.wait(ProviderEvent.PROVIDER_READY)
    assert client.get_provider_status() == ProviderStatus.READY


def test_falls_back_to_polling(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.fail(503, paths=(STREAM_PATH,))
    fake.drop_streams()

    # After 3 failed streams in a row, the snapshot is polled with If-None-Match.
    fake.wait_for(lambda: len(fake.requests_to(SNAPSHOT_PATH)) >= 3)
    polls = fake.requests_to(SNAPSHOT_PATH)[1:]
    assert all(p.headers.get("if-none-match") == '"1"' for p in polls)
    assert len(fake.requests_to(STREAM_PATH)) >= 1 + 3

    # A poll picks up a new snapshot.
    events.drain()
    fake.set_snapshot(make_snapshot(2, {"new-checkout": (False, 2)}))
    details = events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert details.flags_changed == ["new-checkout"]
    assert client.get_boolean_value("new-checkout", True) is False
    # Polls are fresh data: the provider isn't stale.
    assert client.get_provider_status() == ProviderStatus.READY

    # The stream keeps being retried and takes over when it's back.
    fake.recover()
    fake.wait_for(lambda: fake.open_streams == 1)


def test_fatal_on_401(fake: FakeDataPlane) -> None:
    domain = str(uuid.uuid4())
    provider = fast(EzghProvider("ezgh_fs_" + "x" * 43, endpoint=fake.url))
    client = api.get_client(domain)
    events = Events(client)
    with pytest.raises(ProviderFatalError):
        api.set_provider_and_wait(provider, domain)
    assert client.get_provider_status() == ProviderStatus.FATAL
    details = events.wait(ProviderEvent.PROVIDER_ERROR)
    assert details.error_code == ErrorCode.PROVIDER_FATAL
    time.sleep(0.3)
    assert fake.requests_to(STREAM_PATH) == []


def test_revoked_while_streaming_is_fatal(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()
    fake.revoke()
    details = events.wait(ProviderEvent.PROVIDER_ERROR)
    assert details.error_code == ErrorCode.PROVIDER_FATAL
    assert client.get_provider_status() == ProviderStatus.FATAL
    n = len(fake.requests)
    time.sleep(0.4)
    assert len(fake.requests) == n, "a fatal provider stops calling the data plane"


def test_honours_retry_after_on_initialize(fake: FakeDataPlane) -> None:
    fake.fail(429, retry_after=1, paths=(SNAPSHOT_PATH,), times=1)
    _, client = start(fake, max_backoff=2)
    first, second = fake.requests_to(SNAPSHOT_PATH)
    assert second.at - first.at >= 0.95
    assert client.get_boolean_value("new-checkout", False) is True


def test_honours_retry_after_on_stream(fake: FakeDataPlane) -> None:
    start(fake, max_backoff=2)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.fail(429, retry_after=1, paths=(STREAM_PATH,), times=1)
    fake.drop_streams()
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 3, timeout=5)
    _, refused, retried = fake.requests_to(STREAM_PATH)
    assert retried.at - refused.at >= 0.95


def test_initialize_fails_then_recovers(fake: FakeDataPlane) -> None:
    fake.fail(503)
    domain = str(uuid.uuid4())
    provider = fast(EzghProvider(fake.sdk_key, endpoint=fake.url, timeout=0.5))
    client = api.get_client(domain)
    events = Events(client)
    with pytest.raises(GeneralError) as raised:
        api.set_provider_and_wait(provider, domain)
    assert "503" in (raised.value.error_message or "")
    assert client.get_provider_status() == ProviderStatus.ERROR
    events.drain()
    # Not fatal: it keeps trying, and is ready once the data plane answers.
    fake.recover()
    events.wait(ProviderEvent.PROVIDER_READY)
    assert client.get_boolean_value("new-checkout", False) is True


def write_bootstrap(tmp_path: Path) -> str:
    path = tmp_path / "flags.json"
    path.write_text(json.dumps(make_snapshot(7, {"from-file": (True, 3)})))
    return str(path)


def test_offline_never_calls_the_network(fake: FakeDataPlane, tmp_path: Path) -> None:
    provider, client = start(fake, bootstrap_path=write_bootstrap(tmp_path), offline=True)
    assert client.get_provider_status() == ProviderStatus.READY
    assert client.get_boolean_details("from-file", False).flag_metadata == {"flagVersion": 3}
    time.sleep(0.3)
    assert fake.requests == []
    provider.shutdown()


def test_offline_needs_a_bootstrap_file() -> None:
    with pytest.raises(ValueError, match="bootstrap_path"):
        EzghProvider("ezgh_fs_" + "x" * 43, offline=True)


def test_bootstrap_ready_without_the_network(tmp_path: Path) -> None:
    with socket.socket() as s:  # a port nothing listens on
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    domain = str(uuid.uuid4())
    provider = fast(
        EzghProvider(
            "ezgh_fs_" + "x" * 43, endpoint=f"http://127.0.0.1:{port}", bootstrap_path=write_bootstrap(tmp_path)
        )
    )
    started = time.monotonic()
    api.set_provider_and_wait(provider, domain)
    assert time.monotonic() - started < 1
    client = api.get_client(domain)
    assert client.get_provider_status() == ProviderStatus.READY
    assert client.get_boolean_value("from-file", False) is True


def test_bootstrap_replaced_by_the_data_plane(fake: FakeDataPlane, tmp_path: Path) -> None:
    _, client = start(fake, bootstrap_path=write_bootstrap(tmp_path))
    fake.wait_for(lambda: client.get_boolean_value("new-checkout", False) is True)
    # The bootstrap file isn't trusted as the data plane's version: the stream asks for everything.
    assert fake.requests_to(STREAM_PATH)[0].json() == {}
    assert fake.requests_to(SNAPSHOT_PATH) == []
    assert client.get_boolean_details("from-file", False).error_code == ErrorCode.FLAG_NOT_FOUND


def test_type_mismatch(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    details = client.get_string_details("new-checkout", "fallback")
    assert details.value == "fallback"
    assert details.error_code == ErrorCode.TYPE_MISMATCH
    assert client.get_float_details("new-checkout", 1.5).error_code == ErrorCode.TYPE_MISMATCH
    assert client.get_integer_details("new-checkout", 2).error_code == ErrorCode.TYPE_MISMATCH


def test_shutdown_stops_streaming(fake: FakeDataPlane) -> None:
    provider, _ = start(fake)
    fake.wait_for(lambda: fake.open_streams == 1)
    started = time.monotonic()
    provider.shutdown()
    assert time.monotonic() - started < 1
    fake.wait_for(lambda: fake.open_streams == 0)
    n = len(fake.requests)
    time.sleep(0.3)
    assert len(fake.requests) == n


def test_bad_snapshot_event_keeps_the_last_one(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    fake.wait_for(lambda: fake.open_streams == 1)
    broken = make_snapshot(2)
    broken["flags"] = ["not", "an", "object"]
    fake.set_snapshot(broken)
    time.sleep(0.3)
    assert client.get_boolean_value("new-checkout", False) is True


# ---- review fixes


def test_invalid_snapshot_on_initialize_is_not_ready(fake: FakeDataPlane) -> None:
    broken = make_snapshot(1)
    broken["flags"] = ["not", "an", "object"]
    fake.set_snapshot(broken)
    domain = str(uuid.uuid4())
    provider = fast(EzghProvider(fake.sdk_key, endpoint=fake.url, timeout=0.5))
    client = api.get_client(domain)
    events = Events(client)
    with pytest.raises(GeneralError) as raised:
        api.set_provider_and_wait(provider, domain)
    assert "invalid snapshot" in (raised.value.error_message or "")
    assert len(fake.requests_to(SNAPSHOT_PATH)) >= 2, "retried until the timeout"
    assert client.get_provider_status() == ProviderStatus.ERROR
    assert client.get_boolean_details("new-checkout", False).error_code == ErrorCode.PROVIDER_NOT_READY
    events.drain()
    fake.set_snapshot(make_snapshot(2))
    events.wait(ProviderEvent.PROVIDER_READY)
    assert client.get_boolean_value("new-checkout", False) is True


def test_older_snapshot_ignored(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.set_snapshot(make_snapshot(3, {"new-checkout": (False, 2)}))
    events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    events.drain()
    fake.set_snapshot(make_snapshot(2, {"new-checkout": (True, 1)}))
    time.sleep(0.3)
    assert client.get_boolean_value("new-checkout", True) is False
    assert events.q.empty(), "no event for an older snapshot"
    # And the next stream still says what it has.
    fake.end_streams("rotate")
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 2)
    assert fake.requests_to(STREAM_PATH)[1].json() == {"version": 3}


def test_environment_deleted_is_fatal(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()
    fake.end_streams("environmentDeleted")
    details = events.wait(ProviderEvent.PROVIDER_ERROR)
    assert details.error_code == ErrorCode.PROVIDER_FATAL
    n = len(fake.requests)
    time.sleep(0.4)
    assert len(fake.requests) == n


def test_polling_honours_retry_after(fake: FakeDataPlane) -> None:
    start(fake, max_backoff=2)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.fail(503, paths=(STREAM_PATH,))
    fake.fail(429, retry_after=1, paths=(SNAPSHOT_PATH,))
    fake.drop_streams()
    fake.wait_for(lambda: len(fake.requests_to(SNAPSHOT_PATH)) >= 4, timeout=10)
    polls = fake.requests_to(SNAPSHOT_PATH)[1:]
    gaps = [b.at - a.at for a, b in itertools.pairwise(polls)]
    assert all(gap >= 0.95 for gap in gaps), gaps


def test_unexpected_error_keeps_syncing(fake: FakeDataPlane, monkeypatch: pytest.MonkeyPatch) -> None:
    import ezgh_flags.provider as provider_module

    real = getattr(provider_module, "read_sse")  # noqa: B009
    calls = 0

    def flaky(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return real(*args, **kwargs)

    monkeypatch.setattr(provider_module, "read_sse", flaky)
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 2)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.set_snapshot(make_snapshot(2, {"new-checkout": (False, 2)}))
    events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert client.get_provider_status() == ProviderStatus.READY


def sync_threads() -> int:
    import threading

    return sum(1 for t in threading.enumerate() if t.name == "ezgh-flags-sync")


def test_reinitialize_leaves_one_thread(fake: FakeDataPlane) -> None:
    from openfeature.evaluation_context import EvaluationContext

    provider, _ = start(fake)
    fake.wait_for(lambda: fake.open_streams == 1)
    before = sync_threads()
    # A second initialize (as after a shutdown whose join timed out) replaces the first run.
    provider.initialize(EvaluationContext())
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 2)
    fake.wait_for(lambda: fake.open_streams == 1 and sync_threads() == before)
    provider.shutdown()
    fake.wait_for(lambda: fake.open_streams == 0 and sync_threads() == before - 1)


def test_shutdown_during_initialize(fake: FakeDataPlane) -> None:
    import threading

    from openfeature.evaluation_context import EvaluationContext

    fake.fail(503, paths=(SNAPSHOT_PATH,))
    provider = fast(EzghProvider(fake.sdk_key, endpoint=fake.url, timeout=5))
    provider.min_backoff = provider.max_backoff = 0.5
    errors: list[BaseException] = []

    def init() -> None:
        try:
            provider.initialize(EvaluationContext())
        except BaseException as err:
            errors.append(err)

    before = sync_threads()
    t = threading.Thread(target=init)
    t.start()
    fake.wait_for(lambda: len(fake.requests_to(SNAPSHOT_PATH)) == 1)
    provider.shutdown()
    t.join(timeout=2)
    assert not t.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], GeneralError)
    time.sleep(0.2)
    assert sync_threads() == before
    assert fake.requests_to(STREAM_PATH) == []


@pytest.mark.parametrize("mode", ["wrong content type", "closes at once"])
def test_stream_without_frames_is_a_failure(fake: FakeDataPlane, mode: str) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()
    if mode == "wrong content type":
        fake.stream_content_type = "application/json"
    else:
        fake.stream_closes_at_once = True
    fake.drop_streams()
    # Each 200 without a frame counts as a failure: polling starts, and the provider goes stale.
    fake.wait_for(lambda: len(fake.requests_to(SNAPSHOT_PATH)) >= 2)
    fake.fail(503, paths=(SNAPSHOT_PATH,))
    events.wait(ProviderEvent.PROVIDER_STALE)


def test_configuration_changed_fires_for_every_version(fake: FakeDataPlane) -> None:
    _, client = start(fake)
    events = Events(client)
    fake.wait_for(lambda: fake.open_streams == 1)
    events.drain()
    # A new version that changes no flag (e.g. an unused segment edit) still fires, with no keys.
    fake.set_snapshot(make_snapshot(2))
    details = events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert details.flags_changed == []
    assert details.metadata == {"snapshotVersion": 2}
    # A changed definition with the same flagVersion is still named.
    snapshot = make_snapshot(3)
    snapshot["flags"]["new-checkout"]["defaultVariant"] = "false"
    fake.set_snapshot(snapshot)
    details = events.wait(ProviderEvent.PROVIDER_CONFIGURATION_CHANGED)
    assert details.flags_changed == ["new-checkout"]


def test_retry_after_capped_at_max_backoff(fake: FakeDataPlane) -> None:
    start(fake, max_backoff=0.2)
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.fail(429, retry_after=30, paths=(STREAM_PATH,), times=1)
    fake.drop_streams()
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 3, timeout=2)


def test_end_shutdown_reconnects_without_a_failure(fake: FakeDataPlane) -> None:
    provider, client = start(fake)
    provider.stream_failures_before_poll = 1
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.end_streams("shutdown")
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) == 2)
    fake.wait_for(lambda: fake.open_streams == 1)
    assert len(fake.requests_to(SNAPSHOT_PATH)) == 1, "no poll: a drain isn't a failure"
    assert client.get_provider_status() == ProviderStatus.READY


def test_end_unknown_reason_is_a_failure(fake: FakeDataPlane) -> None:
    provider, _ = start(fake)
    provider.stream_failures_before_poll = 1
    fake.wait_for(lambda: fake.open_streams == 1)
    fake.end_streams("somethingNew")
    # Counted as a failure: with one failure enough to poll, the snapshot is polled.
    fake.wait_for(lambda: len(fake.requests_to(SNAPSHOT_PATH)) >= 2)
    fake.wait_for(lambda: len(fake.requests_to(STREAM_PATH)) >= 2)
