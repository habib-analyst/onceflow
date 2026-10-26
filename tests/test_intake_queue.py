import json
import threading
import urllib.request

from onceflow.intake import ingest, serve
from onceflow.queue import FileQueue, InMemoryQueue


def test_ingest_preserves_producer_idempotency_key():
    q = InMemoryQueue()
    event = ingest(q, {"event_id": "e9", "idempotency_key": "order-42",
                       "payload": {"amount": 5}})
    assert event.idempotency_key == "order-42"
    assert len(q) == 1


def test_ingest_mints_key_only_when_missing():
    q = InMemoryQueue()
    event = ingest(q, {"payload": {}})
    assert event.idempotency_key  # something was assigned


def test_file_queue_roundtrip_and_resume(tmp_path):
    path = str(tmp_path / "queue.jsonl")
    q = FileQueue(path)
    ingest(q, {"event_id": "e1", "idempotency_key": "k1", "seq": 2,
               "payload": {"amount": 5}})
    ingest(q, {"event_id": "e2", "idempotency_key": "k2",
               "payload": {"amount": 6}})
    first = q.get()
    assert first.event_id == "e1" and first.seq == 2
    # A new consumer over the same log resumes after e1.
    q2 = FileQueue(path)
    second = q2.get()
    assert second.event_id == "e2"
    assert q2.get() is None


def test_webhook_http_intake_roundtrip():
    q = InMemoryQueue()
    server = serve(q, port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        body = json.dumps({"event_id": "wh-1", "idempotency_key": "whk-1",
                           "payload": {"amount": 5}}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/webhook", data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 202
            reply = json.loads(resp.read())
        assert reply["idempotency_key"] == "whk-1"
        assert len(q) == 1
    finally:
        server.shutdown()
