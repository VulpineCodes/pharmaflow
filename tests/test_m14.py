"""M14 — replay outcome linkage.

A replay's fate is decided by the next consume run for its group,
and M13 left that fate discoverable only by cross-referencing two
histories by event id. M14 records it on the replay itself: when a
completed ``stream-consume`` run processes the re-published record,
one outcome row is appended to ``dead_letter_replay_outcomes``
(schema v6) — applied, duplicate, or dead-lettered again, with the
new dead-letter row id when applicable and the consumption run id.
A replay with no outcome row has not been consumed yet (pending).

The link key is the broker coordinates of the re-published record
(topic/partition/offset), not the event id: coordinates identify
the record even when the run could not recover its event id. Only
the replay's own group gets an outcome — another group consuming
the same topic sees the replayed event as its own business (usually
a duplicate, since the dedupe table is per-store, see M11).

The migration test builds a database with the frozen pre-M14 (v5)
schema by hand, so opening it must migrate additively: existing
rows intact, version bumped to 6, outcome recording working
afterwards.
"""
import json
import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from api.app import create_app
from pharmaflow import SqliteStore
from pharmaflow.__main__ import main as cli_main
from pharmaflow.observability import render_replay_outcomes_prometheus
from pharmaflow.streaming import (
    DEFAULT_TOPIC,
    DeadLetter,
    DeadLetterReplay,
    EventProducer,
    FileLogBroker,
    ReplayOutcome,
    SaleRecorded,
    SimulatedCrashError,
    StockReceived,
    StreamingConsumer,
    consume_once,
    replay_dead_letter,
)

ROOT = Path(__file__).resolve().parents[1]
GROUP = "reconciliation-v1"
OTHER_GROUP = "alerts-v1"

# Frozen pre-M14 schema (v5): every table up to and including
# dead_letter_replays, with schema_version recorded as 5. Copied
# from the v5 _SCHEMA so the migration test does not depend on the
# current schema definition.
V5_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_hashes (
    file_hash TEXT PRIMARY KEY,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS row_hashes (
    row_hash TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS imports (
    file_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    kind TEXT NOT NULL,
    rows_in INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_quarantined INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost REAL NOT NULL DEFAULT 0,
    expiry_date TEXT,
    supplier TEXT NOT NULL DEFAULT '',
    batch TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_date TEXT,
    unit_price REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_number INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    errors_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    action TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_date TEXT NOT NULL,
    succeeded INTEGER NOT NULL,
    failed_task TEXT,
    recorded_at TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_consumption_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    records_read INTEGER NOT NULL DEFAULT 0,
    applied INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    dlq INTEGER NOT NULL DEFAULT 0,
    lag_before INTEGER NOT NULL DEFAULT 0,
    lag_after INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    errors_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letter_replays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dead_letter_id INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    corrected_json TEXT NOT NULL,
    replayed_at TEXT NOT NULL
);
"""


def _make_v5_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(V5_SCHEMA)
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (5, '2026-11-06T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date, supplier, batch)"
        " VALUES ('MED-001', 'Paracetamol 500mg 100 tabs', 120, 20, 4.50, '2027-06-30', '', '')"
    )
    conn.commit()
    conn.close()


def _seed_stock_plus_poison_sale(producer: EventProducer) -> None:
    """One valid stock receipt + one poison sale (quantity -2), both
    keyed to MED-001 so publish order is consume order."""
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-poison-1",
         "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
         "unit_price": 6.00},
        key="MED-001")


def _dead_lettered_db(log_dir: Path, db: Path) -> int:
    """Seed + consume so dead letter #1 (evt-poison-1) is recorded;
    return its record id."""
    _seed_stock_plus_poison_sale(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1 and result.dlq == 1
    assert result.dead_letter_ids == [1]
    return 1


def test_consume_result_carries_per_record_outcomes(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_stock_plus_poison_sale(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 2
    assert [(o.event_id, o.outcome) for o in result.event_outcomes] == [
        ("evt-1", "applied"),
        ("evt-poison-1", "dead_lettered"),
    ]
    # Both outcomes sit in the same partition (same SKU key) and
    # their offsets are the broker coordinates of the records.
    assert result.event_outcomes[0].partition == result.event_outcomes[1].partition == 2
    assert [o.offset for o in result.event_outcomes] == [0, 1]
    # No replay existed, so no outcome rows were linked.
    assert result.replay_outcome_ids == []


def test_applied_outcome_recorded_on_next_consume(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    replay = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    assert replay.replay_id == 1
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 1 and result.applied == 1
    # The run that decided the outcome is consumption run #2 (run #1
    # was the poison consume that recorded the dead letter).
    assert result.record_id == 2
    assert result.replay_outcome_ids == [1]
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert store.inventory["MED-001"].quantity_on_hand == 95
        assert store.count_replay_outcomes() == 1
        assert store.count_replay_outcomes(group=GROUP) == 1
        assert store.count_replay_outcomes(group="no-such-group") == 0
        outcome = store.get_replay_outcome(1, GROUP)
        assert outcome is not None
        assert outcome.outcome_id == 1
        assert outcome.replay_id == 1 and outcome.dead_letter_id == 1
        assert outcome.group == GROUP and outcome.event_id == "evt-poison-1"
        assert outcome.outcome == "applied"
        assert outcome.new_dead_letter_id is None
        assert outcome.consumption_run_id == 2
        assert store.latest_replay_outcome().outcome_id == 1
        assert store.replay_outcome_totals_by_group() == {GROUP: {"applied": 1}}


def test_duplicate_outcome_for_double_replay(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    first = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    second = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    assert first.replay_id == 1 and second.replay_id == 2
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 2 and result.applied == 1
    assert result.duplicates == 1 and result.dlq == 0
    assert result.replay_outcome_ids == [1, 2]
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 95
        assert store.count_replay_outcomes() == 2
        outcomes = store.list_replay_outcomes()
        # Newest first: the duplicate (replay #2) was recorded after
        # the applied outcome (replay #1).
        assert [(o.replay_id, o.outcome) for o in outcomes] == [
            (2, "duplicate"), (1, "applied")]
        assert outcomes[0].consumption_run_id == outcomes[1].consumption_run_id == 2
        assert outcomes[0].new_dead_letter_id is None
        assert store.replay_outcome_totals_by_group() == {
            GROUP: {"applied": 1, "duplicate": 1}}


def test_dead_lettered_again_outcome_links_new_dead_letter(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    # Parse-valid sale for a SKU that does not exist yet: the poison
    # is a state failure at apply time.
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-unknown-sku",
         "sale_id": "S-7", "sku": "MED-009", "quantity_sold": 7,
         "unit_price": 3.00},
        key="MED-009")
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 0 and result.dlq == 1
    assert result.dead_letter_ids == [1]
    # Replay unchanged — the SKU still does not exist, so the next
    # consume dead-letters the replayed event again, as a NEW dead
    # letter row (#2) for the same event id.
    replay = replay_dead_letter(log_dir, db, 1)
    assert replay.replay_id == 1 and replay.event_id == "evt-unknown-sku"
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 1 and result.dlq == 1
    assert result.dead_letter_ids == [2]
    assert result.replay_outcome_ids == [1]
    with SqliteStore(db) as store:
        assert store.count_dead_letters() == 2
        outcome = store.get_replay_outcome(1, GROUP)
        assert outcome is not None
        assert outcome.outcome == "dead_lettered"
        assert outcome.new_dead_letter_id == 2
        assert outcome.consumption_run_id == 2
        new_letter = store.get_dead_letter(2)
        assert new_letter is not None
        assert new_letter.event_id == "evt-unknown-sku"
        assert any("sale for unknown sku" in e for e in new_letter.errors)
    # The chain closes: stock the SKU, replay the NEW dead letter,
    # consume — the second replay applies.
    producer.publish(StockReceived(event_id="evt-stock-9", sku="MED-009",
                                   product_name="Ibuprofen 200mg 50 tabs",
                                   quantity=50, reorder_point=10, unit_cost=3.00))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1  # the stock receipt
    replay2 = replay_dead_letter(log_dir, db, 2)
    assert replay2.replay_id == 2
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1
    with SqliteStore(db) as store:
        assert store.inventory["MED-009"].quantity_on_hand == 43
        assert store.count_replay_outcomes() == 2
        outcome2 = store.get_replay_outcome(2, GROUP)
        assert outcome2 is not None and outcome2.outcome == "applied"
        assert outcome2.new_dead_letter_id is None
        assert store.replay_outcome_totals_by_group() == {
            GROUP: {"applied": 1, "dead_lettered": 1}}


def test_pending_replay_has_no_outcome(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 1
        assert store.count_replay_outcomes() == 0
        assert store.list_replay_outcomes() == []
        assert store.get_replay_outcome(1, GROUP) is None
        assert store.latest_replay_outcome() is None
        assert store.replay_outcome_totals_by_group() == {}


def test_crash_run_records_no_outcome(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    # The crash-simulation hook: the replayed event is applied (the
    # dedupe marker persists) and then the consumer "dies" before
    # committing offsets — consume_once never runs, so nothing is
    # recorded for this run.
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db)
    try:
        consumer = StreamingConsumer(broker, store, group=GROUP)
        with pytest.raises(SimulatedCrashError):
            consumer.consume_until_caught_up(crash_after_applies=1)
        assert store.inventory["MED-001"].quantity_on_hand == 95
        assert store.count_stream_consumption() == 1  # only the poison run
        assert store.count_replay_outcomes() == 0
    finally:
        store.close()
    # The recovery run redelivers the replayed event and sees a
    # duplicate — and that is what is recorded, because the outcome
    # describes what the completed run did.
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 1 and result.duplicates == 1
    assert result.applied == 0
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 95
        outcome = store.get_replay_outcome(1, GROUP)
        assert outcome is not None
        assert outcome.outcome == "duplicate"
        assert outcome.consumption_run_id == 2


def test_outcome_scoped_to_replay_group(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    # Another group consumes the topic first: it applies the
    # replayed event (the dedupe table is per-store, not per-group),
    # but that is not the replay's outcome — the replay belongs to
    # reconciliation-v1.
    result = consume_once(log_dir, db, group=OTHER_GROUP)
    assert result.applied == 1
    assert result.replay_outcome_ids == []
    with SqliteStore(db) as store:
        assert store.count_replay_outcomes() == 0
    # The replay's own group now sees the event as a duplicate.
    result = consume_once(log_dir, db, group=GROUP)
    assert result.duplicates == 1 and result.applied == 0
    assert result.replay_outcome_ids == [1]
    with SqliteStore(db) as store:
        assert store.count_replay_outcomes() == 1
        assert store.list_replay_outcomes(group=OTHER_GROUP) == []
        outcome = store.get_replay_outcome(1, GROUP)
        assert outcome is not None and outcome.outcome == "duplicate"


def test_v5_database_migrates_additively(tmp_path):
    db = tmp_path / "pharma.db"
    _make_v5_db(db)
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        # Pre-existing data survived the migration untouched.
        assert len(store.inventory) == 1
        assert store.inventory["MED-001"].quantity_on_hand == 120
        assert store.count_dead_letter_replays() == 0
        assert store.count_replay_outcomes() == 0
        replay_id = store.record_dead_letter_replay(DeadLetterReplay(
            dead_letter_id=1, group=GROUP,
            event_id="evt-poison-1", topic=DEFAULT_TOPIC,
            partition=2, offset=2,
            corrected_event={"event_type": "SaleRecorded",
                             "event_id": "evt-poison-1",
                             "quantity_sold": 5}))
        assert replay_id == 1
        # The replay sits at (topic, partition 2, offset 2) with no
        # outcome yet, so the lookup finds it; after recording an
        # outcome it is resolved and the lookup returns None.
        found = store.find_unresolved_replay_at_coordinates(
            DEFAULT_TOPIC, 2, 2, GROUP)
        assert found is not None and found.replay_id == 1
        outcome_id = store.record_replay_outcome(ReplayOutcome(
            replay_id=replay_id, dead_letter_id=1, group=GROUP,
            event_id="evt-poison-1", outcome="applied",
            consumption_run_id=1))
        assert outcome_id == 1
        assert store.find_unresolved_replay_at_coordinates(
            DEFAULT_TOPIC, 2, 2, GROUP) is None
        assert store.get_replay_outcome(1, GROUP) is not None
    # Reopening stays v6 and the rows are still there.
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert store.count_replay_outcomes() == 1
        assert store.get_replay_outcome(1, GROUP).outcome == "applied"


def test_store_api_filtering_and_limits(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as store:
        for i, (group, outcome) in enumerate([
            (GROUP, "applied"), (GROUP, "duplicate"),
            (OTHER_GROUP, "dead_lettered"),
        ]):
            store.record_replay_outcome(ReplayOutcome(
                replay_id=i + 1, dead_letter_id=i + 1, group=group,
                event_id=f"evt-{i}", outcome=outcome,
                new_dead_letter_id=9 if outcome == "dead_lettered" else None,
                consumption_run_id=i + 1))
        assert store.count_replay_outcomes() == 3
        assert store.count_replay_outcomes(group=GROUP) == 2
        assert store.count_replay_outcomes(group="no-such-group") == 0
        assert [o.outcome_id for o in store.list_replay_outcomes(limit=1)] == [3]
        # Limit clamps: 0 behaves as 1, 500 behaves as 100 (returns all 3).
        assert len(store.list_replay_outcomes(limit=0)) == 1
        assert len(store.list_replay_outcomes(limit=500)) == 3
        assert [o.replay_id for o in store.list_replay_outcomes(group=GROUP)] == [2, 1]
        assert [o.outcome_id for o in
                store.list_replay_outcomes(replay_id=2)] == [2]
        assert [o.outcome for o in
                store.list_replay_outcomes(outcome="dead_lettered")] == ["dead_lettered"]
        assert store.list_replay_outcomes(outcome="applied", group=OTHER_GROUP) == []
        latest = store.latest_replay_outcome(group=OTHER_GROUP)
        assert latest is not None and latest.outcome == "dead_lettered"
        assert latest.new_dead_letter_id == 9
        assert store.latest_replay_outcome(group="no-such-group") is None
        assert store.get_replay_outcome(99, GROUP) is None
        assert store.replay_outcome_totals_by_group() == {
            GROUP: {"applied": 1, "duplicate": 1},
            OTHER_GROUP: {"dead_lettered": 1}}
        # One outcome per (replay, group): a second recording for
        # the same pair raises instead of overwriting the first.
        with pytest.raises(sqlite3.IntegrityError):
            store.record_replay_outcome(ReplayOutcome(
                replay_id=1, dead_letter_id=1, group=GROUP,
                event_id="evt-0", outcome="duplicate"))


def test_render_replay_outcomes_prometheus():
    totals = {GROUP: {"applied": 2, "duplicate": 1},
              OTHER_GROUP: {"dead_lettered": 1}}
    text = render_replay_outcomes_prometheus(totals)
    assert (f'pharmaflow_dead_letter_replay_outcomes_total{{group="{GROUP}",outcome="applied"}} 2'
            in text)
    assert (f'pharmaflow_dead_letter_replay_outcomes_total{{group="{GROUP}",outcome="duplicate"}} 1'
            in text)
    assert (f'pharmaflow_dead_letter_replay_outcomes_total{{group="{OTHER_GROUP}",outcome="dead_lettered"}} 1'
            in text)
    assert render_replay_outcomes_prometheus({}) == ""


def test_cli_outcome_flow(tmp_path, capsys):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    events_file = tmp_path / "events.jsonl"
    events_file.write_text("\n".join([
        json.dumps({"event_type": "StockReceived", "event_id": "evt-1",
                    "sku": "MED-001", "product_name": "Paracetamol 500mg 100 tabs",
                    "quantity": 100, "reorder_point": 20, "unit_cost": 4.50}),
        json.dumps({"event_type": "SaleRecorded", "event_id": "evt-poison-1",
                    "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
                    "unit_price": 6.00}),
    ]) + "\n", encoding="utf-8")
    assert cli_main(["stream-produce", "--log-dir", str(log_dir),
                     "--events", str(events_file)]) == 0
    capsys.readouterr()
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Recorded 1 dead letter(s) (group reconciliation-v1)" in out
    assert cli_main(["dead-letter-replay", "--db", str(db),
                     "--log-dir", str(log_dir), "--id", "1",
                     "--corrections", '{"quantity_sold": 5}']) == 0
    capsys.readouterr()
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Consumed: read=1 applied=1 skipped_dlq=0 duplicates=0" in out
    assert "Recorded 1 replay outcome(s) (group reconciliation-v1)" in out
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 95
    assert cli_main(["replay-outcomes", "--db", str(db), "--json"]) == 0
    outcomes = json.loads(capsys.readouterr().out)
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_id"] == 1 and outcomes[0]["replay_id"] == 1
    assert outcomes[0]["dead_letter_id"] == 1
    assert outcomes[0]["outcome"] == "applied"
    assert outcomes[0]["new_dead_letter_id"] is None
    assert outcomes[0]["consumption_run_id"] == 2
    assert cli_main(["replay-outcomes", "--db", str(db), "--latest"]) == 0
    out = capsys.readouterr().out
    assert "#1 replay=#1 dead_letter=#1 group=reconciliation-v1 event_id=evt-poison-1 outcome=applied" in out
    assert cli_main(["replay-outcomes", "--db", str(db),
                     "--outcome", "dead_lettered"]) == 0
    assert "No replay outcomes recorded." in capsys.readouterr().out
    assert cli_main(["replay-outcomes", "--db", str(db),
                     "--replay-id", "99"]) == 0
    assert "No replay outcomes recorded." in capsys.readouterr().out


def test_api_outcome_endpoints_and_metrics(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    client = TestClient(create_app(db_path=db, stream_log_dir=log_dir))
    # Empty history: list is empty, latest is 404, no outcome gauge,
    # and the single-replay endpoint is 404 too.
    assert client.get("/replay-outcomes").json() == []
    assert client.get("/replay-outcomes/latest").status_code == 404
    assert client.get("/dead-letter-replays/1").status_code == 404
    assert "pharmaflow_dead_letter_replay_outcomes_total" not in client.get("/metrics").text
    # Dead-letter, replay, consume: the outcome appears everywhere.
    _dead_lettered_db(log_dir, db)
    resp = client.post("/dead-letters/1/replay",
                       json={"corrections": {"quantity_sold": 5}})
    assert resp.status_code == 201
    # Pending: the replay exists but has no outcome yet.
    single = client.get("/dead-letter-replays/1").json()
    assert single["replay_id"] == 1 and single["outcomes"] == []
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1
    outcomes = client.get("/replay-outcomes").json()
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_id"] == 1 and outcomes[0]["replay_id"] == 1
    assert outcomes[0]["dead_letter_id"] == 1
    assert outcomes[0]["group"] == GROUP and outcomes[0]["event_id"] == "evt-poison-1"
    assert outcomes[0]["outcome"] == "applied"
    assert outcomes[0]["new_dead_letter_id"] is None
    assert outcomes[0]["consumption_run_id"] == 2
    latest = client.get("/replay-outcomes/latest").json()
    assert latest["outcome_id"] == 1
    assert client.get("/replay-outcomes",
                      params={"outcome": "dead_lettered"}).json() == []
    assert client.get("/replay-outcomes",
                      params={"replay_id": 1}).json()[0]["outcome"] == "applied"
    single = client.get("/dead-letter-replays/1").json()
    assert len(single["outcomes"]) == 1
    assert single["outcomes"][0]["outcome"] == "applied"
    assert client.get("/dead-letter-replays/99").status_code == 404
    metrics = client.get("/metrics").text
    assert (f'pharmaflow_dead_letter_replay_outcomes_total{{group="{GROUP}",outcome="applied"}} 1'
            in metrics)
    # A second app instance on the same DB serves the same history
    # (the history is durable, not per-process).
    client2 = TestClient(create_app(db_path=db, stream_log_dir=log_dir))
    assert len(client2.get("/replay-outcomes").json()) == 1
    assert (f'pharmaflow_dead_letter_replay_outcomes_total{{group="{GROUP}",outcome="applied"}} 1'
            in client2.get("/metrics").text)
