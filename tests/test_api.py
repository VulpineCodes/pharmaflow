"""M5 — FastAPI serving against the durable SQLite store (TestClient).

These tests exercise the API exactly as a client would: CSV bytes go in
via ``POST /imports`` (multipart upload), state is read back through
``GET /inventory``, and a *second app instance on the same DB file*
proves the API serves persisted state, not in-memory demo data.

Sample-data expectations are hand-known from sample_data/inventory.csv:
5 SKUs; on 2026-10-22 MED-002 (8 <= reorder 15) and MED-005 (5 <= 10)
are low-stock, MED-005 expired 2026-09-01, MED-003 expires 2026-11-10
(19 days -> expiring_soon).
"""
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from api.app import create_app

ROOT = Path(__file__).resolve().parents[1]
INV = ROOT / "sample_data" / "inventory.csv"
REF_TODAY = "2026-10-22"


@pytest.fixture()
def client(tmp_path):
    with TestClient(create_app(tmp_path / "api.db")) as c:
        yield c


def _upload(client, path=INV, name="inventory.csv"):
    return client.post(
        "/imports",
        files={"file": (name, path.read_bytes(), "text/csv")},
    )


def test_health_empty_db(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok" and body["service"] == "pharmaflow"
    assert body["inventory_skus"] == 0


def test_ingest_csv_through_api_and_read_back_inventory(client):
    r = _upload(client)
    assert r.status_code == 200
    body = r.json()
    assert body["rows_in"] == 5 and body["rows_valid"] == 5
    assert body["rows_quarantined"] == 0 and body["already_imported"] is False

    inv = client.get("/inventory")
    assert inv.status_code == 200
    rows = inv.json()
    assert [row["sku"] for row in rows] == ["MED-001", "MED-002", "MED-003", "MED-004", "MED-005"]
    med1 = rows[0]
    assert med1["product_name"] == "Paracetamol 500mg 100 tabs"
    assert med1["quantity_on_hand"] == 120 and med1["unit_cost"] == 4.50
    assert med1["expiry_date"] == "2027-06-30"
    assert client.get("/health").json()["inventory_skus"] == 5


def test_duplicate_post_is_idempotent(client):
    first = _upload(client).json()
    assert first["already_imported"] is False
    second = _upload(client).json()
    assert second["already_imported"] is True
    assert second["duplicates_skipped"] == 5 and second["rows_valid"] == 0
    rows = client.get("/inventory").json()
    assert len(rows) == 5
    assert {r["sku"]: r["quantity_on_hand"] for r in rows}["MED-001"] == 120


def test_metrics_contains_counters_after_ingest(client):
    _upload(client)
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    text = r.text
    assert "pharmaflow_rows_in_total 5" in text
    assert "pharmaflow_rows_valid_total 5" in text
    assert "pharmaflow_imports_total_total 1" in text  # counter name as rendered by Metrics.render_prometheus


def test_alerts_reflect_sample_data(client):
    _upload(client)
    r = client.get("/alerts", params={"today": REF_TODAY})
    assert r.status_code == 200
    by = {(a["sku"], a["kind"]) for a in r.json()}
    assert ("MED-002", "low_stock") in by
    assert ("MED-005", "low_stock") in by
    assert ("MED-005", "expired") in by
    assert ("MED-003", "expiring_soon") in by
    # MED-001 / MED-004: well stocked, expiry far away -> no alerts.
    assert not any(sku in ("MED-001", "MED-004") for sku, _ in by)


def test_reconciliation_endpoint_detects_drift(client):
    _upload(client)
    r = client.get("/reconciliation", params={"expected": "MED-001=110,MED-002=8"})
    assert r.status_code == 200
    kinds = {d["sku"]: d["kind"] for d in r.json()}
    assert kinds["MED-001"] == "count_drift"  # expected 110, actual 120
    # MED-003/004/005 are in actual but not expected -> unexpected_sku.
    assert kinds["MED-003"] == "unexpected_sku"


def test_data_quality_endpoint_after_ingest(client):
    _upload(client)
    r = client.get("/data-quality")
    assert r.status_code == 200
    body = r.json()
    assert body["totals"] == {
        "rows_in": 5, "rows_valid": 5, "rows_quarantined": 0,
        "duplicates_skipped": 0, "duplicate_rate": 0.0,
    }
    assert len(body["imports"]) == 1 and body["imports"][0]["kind"] == "inventory"


def test_api_serves_persisted_state_across_app_restart(tmp_path):
    db = tmp_path / "persist.db"
    with TestClient(create_app(db)) as c1:
        assert _upload(c1).json()["rows_valid"] == 5
    # New app instance, same DB file: inventory must still be there.
    with TestClient(create_app(db)) as c2:
        rows = c2.get("/inventory").json()
        assert len(rows) == 5
        assert c2.get("/health").json()["inventory_skus"] == 5


def test_ingest_invalid_csv_returns_422(client):
    r = client.post(
        "/imports",
        files={"file": ("bad.csv", b"foo,bar\n1,2\n", "text/csv")},
    )
    assert r.status_code == 422
