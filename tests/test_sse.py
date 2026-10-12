from __future__ import annotations

from ezgh_flags.sse import Event, read_sse


def parse(*chunks: bytes) -> list[Event]:
    return list(read_sse(iter(chunks)))


def test_one_frame() -> None:
    assert parse(b"event: snapshot\nid: 3\ndata: {}\n\n") == [Event(event="snapshot", data="{}", id="3")]


def test_frames_split_across_reads() -> None:
    stream = b'event: snapshot\nid: 1\ndata: {"a":1}\n\nevent: end\ndata: {"reason":"rotate"}\n\n'
    whole = parse(stream)
    assert whole == [
        Event(event="snapshot", data='{"a":1}', id="1"),
        Event(event="end", data='{"reason":"rotate"}', id=None),
    ]
    # Every split point, and one byte at a time.
    for i in range(len(stream) + 1):
        assert parse(stream[:i], stream[i:]) == whole
    assert parse(*(stream[i : i + 1] for i in range(len(stream)))) == whole


def test_comments_ignored() -> None:
    assert parse(b": open\n\n: heartbeat\n\nevent: changed\n: inside\ndata: {}\n\n") == [
        Event(event="changed", data="{}", id=None)
    ]


def test_id_and_event_carried() -> None:
    assert parse(b"id: 7\nevent: changed\ndata: x\n\n") == [Event(event="changed", data="x", id="7")]


def test_default_event_is_message() -> None:
    assert parse(b"data: x\n\n") == [Event(event="message", data="x", id=None)]


def test_multiline_data_joined() -> None:
    assert parse(b"data: a\ndata: b\ndata:c\n\n") == [Event(event="message", data="a\nb\nc", id=None)]


def test_crlf_and_cr_endings() -> None:
    expected = [Event(event="snapshot", data="{}", id="2")]
    assert parse(b"event: snapshot\r\nid: 2\r\ndata: {}\r\n\r\n") == expected
    assert parse(b"event: snapshot\rid: 2\rdata: {}\r\r") == expected
    # A CRLF split between reads is one line ending, not two.
    assert parse(b"event: snapshot\r", b"\nid: 2\r", b"\ndata: {}\r", b"\n\r", b"\n") == expected


def test_no_data_no_event() -> None:
    assert parse(b"event: ping\n\n") == []


def test_incomplete_frame_dropped_at_eof() -> None:
    assert parse(b"event: snapshot\ndata: {}\n") == []


def test_utf8_split_across_reads() -> None:
    raw = "data: héllo ✓\n\n".encode()
    assert parse(raw[:8], raw[8:]) == [Event(event="message", data="héllo ✓", id=None)]


def test_comments_reported() -> None:
    comments: list[str] = []
    events = list(read_sse(iter([b": open\n\n:heartbeat\n\ndata: x\n\n"]), on_comment=comments.append))
    assert comments == ["open", "heartbeat"]
    assert events == [Event(event="message", data="x", id=None)]
