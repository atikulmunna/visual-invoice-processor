from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from app.review_queue import dismiss_review_item, list_review_items, resolve_review_item


class ReviewResolveRequest(BaseModel):
    action: str = "approve"
    note: str | None = None
    corrected_record: dict[str, Any] | None = None


def create_monitoring_app(
    *,
    metrics_path: str | Path = "logs/metrics.jsonl",
    dead_letter_path: str | Path = "logs/dead_letter.jsonl",
    review_queue_dir: str | Path = "review_queue",
    postgres_dsn: str | None = None,
) -> FastAPI:
    app = FastAPI(title="Invoice Processor Monitoring API", version="0.1.0")
    active_postgres_dsn = postgres_dsn or os.getenv("POSTGRES_DSN")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard", status_code=307)

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        metric_events = _read_jsonl(metrics_path)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        dead_letters = _active_dead_letters(dead_letter_path, resolved_hashes)
        queue_size = _active_review_queue_size(review_queue_dir, resolved_hashes)
        counters = _aggregate_metrics(metric_events)
        counters["dead_letter_total"] = len(dead_letters)
        counters["review_queue_total"] = queue_size
        return counters

    @app.get("/failures")
    def failures(limit: int = 50) -> dict[str, Any]:
        items = _read_jsonl(dead_letter_path)
        return {"count": len(items), "items": items[-limit:]}

    @app.get("/backlog")
    def backlog() -> dict[str, Any]:
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        queue_size = _active_review_queue_size(review_queue_dir, resolved_hashes)
        dead_letters = len(_active_dead_letters(dead_letter_path, resolved_hashes))
        return {
            "review_queue_total": queue_size,
            "dead_letter_total": dead_letters,
            "attention_total": queue_size + dead_letters,
        }

    @app.get("/dashboard/data")
    def dashboard_data(limit: int = 20) -> dict[str, Any]:
        data = _query_dashboard_data(active_postgres_dsn, limit=limit)
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        data["review_queue_total"] = _active_review_queue_size(review_queue_dir, resolved_hashes)
        data["dead_letter_total"] = len(_active_dead_letters(dead_letter_path, resolved_hashes))
        return data

    @app.get("/review-items")
    def review_items() -> dict[str, Any]:
        resolved_hashes = _resolved_file_hashes(active_postgres_dsn)
        items = _active_review_items(review_queue_dir, resolved_hashes)
        return {"count": len(items), "items": items}

    @app.get("/review-history")
    def review_history(limit: int = 20) -> dict[str, Any]:
        items = _review_history_items(review_queue_dir, limit=limit)
        return {"count": len(items), "items": items}

    @app.post("/review-items/{document_id}/resolve")
    def review_resolve(document_id: str, payload: ReviewResolveRequest | None = None) -> dict[str, Any]:
        try:
            action = (payload.action if payload else "approve").strip().lower()
            if action == "approve":
                result = resolve_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    record_override=payload.corrected_record if payload else None,
                    note=payload.note if payload else None,
                )
            elif action == "reject":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="REJECTED",
                    note=payload.note if payload else None,
                )
            elif action == "duplicate":
                result = dismiss_review_item(
                    document_id=document_id,
                    queue_dir=review_queue_dir,
                    resolution_status="RESOLVED_DUPLICATE_MANUAL",
                    note=payload.note if payload else None,
                )
            else:
                raise ValueError(f"Unsupported review action: {action}")
            return {
                "status": "ok",
                "document_id": document_id,
                "action": action,
                "storage_result": result["storage_result"],
                "review_status": result["review_item"].get("status"),
            }
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard() -> str:
        return _dashboard_html()

    return app


def _read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _review_queue_size(path: str | Path) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return len([x for x in p.glob("*.json") if x.is_file()])


def _active_review_queue_size(path: str | Path, resolved_hashes: set[str]) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    total = 0
    for file_path in p.glob("*.json"):
        if not file_path.is_file():
            continue
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if payload.get("status") != "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        file_hash = str(metadata.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        total += 1
    return total


def _active_review_items(path: str | Path, resolved_hashes: set[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path):
        if payload.get("status") != "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        file_hash = str(metadata.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        normalized = metadata.get("normalized_record") if isinstance(metadata.get("normalized_record"), dict) else {}
        items.append(
            {
                "document_id": payload.get("document_id"),
                "status": payload.get("status"),
                "reason_codes": payload.get("reason_codes", []),
                "created_at_utc": payload.get("created_at_utc"),
                "source_file_id": metadata.get("source_file_id") or metadata.get("drive_file_id"),
                "file_hash": file_hash,
                "used_provider": metadata.get("used_provider", "unknown"),
                "vendor_name": normalized.get("vendor_name"),
                "invoice_number": normalized.get("invoice_number"),
                "invoice_date": normalized.get("invoice_date"),
                "currency": normalized.get("currency"),
                "total_amount": normalized.get("total_amount"),
                "normalized_record": normalized,
            }
        )
    return items


def _review_history_items(path: str | Path, *, limit: int = 20) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for payload in list_review_items(queue_dir=path):
        status = str(payload.get("status", "") or "")
        if status == "REVIEW_REQUIRED":
            continue
        metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
        resolved_record = payload.get("resolved_record") if isinstance(payload.get("resolved_record"), dict) else {}
        history.append(
            {
                "document_id": payload.get("document_id"),
                "status": status,
                "created_at_utc": payload.get("created_at_utc"),
                "resolved_at_utc": payload.get("resolved_at_utc"),
                "source_file_id": metadata.get("source_file_id") or metadata.get("drive_file_id"),
                "used_provider": metadata.get("used_provider", "unknown"),
                "vendor_name": resolved_record.get("vendor_name") or metadata.get("vendor_name") or "Unknown",
                "invoice_number": resolved_record.get("invoice_number") or "-",
                "total_amount": resolved_record.get("total_amount"),
                "currency": resolved_record.get("currency") or "NA",
                "resolution_note": payload.get("resolution_note"),
            }
        )
    history.sort(key=lambda item: str(item.get("resolved_at_utc") or item.get("created_at_utc") or ""), reverse=True)
    return history[:limit]


def _aggregate_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    counters: dict[str, int] = {}
    for event in events:
        name = event.get("metric")
        value = event.get("value")
        if isinstance(name, str) and isinstance(value, int):
            counters[name] = counters.get(name, 0) + value
    return counters


def _active_dead_letters(path: str | Path, resolved_hashes: set[str]) -> list[dict[str, Any]]:
    events = _read_jsonl(path)
    latest_by_key: dict[str, dict[str, Any]] = {}
    for event in events:
        status = str(event.get("status", "") or "")
        if status not in {"FAILED", "REVIEW_REQUIRED"}:
            continue
        file_hash = str(event.get("file_hash", "") or "")
        if file_hash and file_hash in resolved_hashes:
            continue
        key = (
            str(event.get("document_id", "") or "")
            or (str(event.get("drive_file_id", "") or "") + "|" + file_hash)
            or str(hash(json.dumps(event, sort_keys=True)))
        )
        latest_by_key[key] = event
    return list(latest_by_key.values())


def _resolved_file_hashes(postgres_dsn: str | None) -> set[str]:
    if not postgres_dsn:
        return set()
    try:
        import psycopg
    except ImportError:
        return set()

    try:
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select distinct file_hash
                    from public.ledger_records
                    where status in ('STORED', 'ARCHIVED')
                    """
                )
                return {str(row[0]) for row in cur.fetchall() if row and row[0]}
    except Exception:  # noqa: BLE001
        return set()


def _query_dashboard_data(postgres_dsn: str | None, *, limit: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kpis": {
            "records_total": 0,
            "stored_total": 0,
            "needs_review_total": 0,
            "total_amount_sum": 0.0,
        },
        "daily_summary": [],
        "vendor_spend": [],
        "provider_mix": [],
        "recent_records": [],
        "error": None,
    }
    if not postgres_dsn:
        payload["error"] = "POSTGRES_DSN not configured"
        return payload

    try:
        import psycopg
    except ImportError:
        payload["error"] = "psycopg not installed"
        return payload

    try:
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select
                      count(*)::int as records_total,
                      count(*) filter (where row_status = 'STORED')::int as stored_total,
                      count(*) filter (where needs_review = true)::int as needs_review_total,
                      coalesce(sum(total_amount), 0)::float as total_amount_sum
                    from public.ledger_records_flat
                    """
                )
                row = cur.fetchone()
                if row:
                    payload["kpis"] = {
                        "records_total": row[0],
                        "stored_total": row[1],
                        "needs_review_total": row[2],
                        "total_amount_sum": row[3],
                    }

                cur.execute(
                    """
                    select processing_date::text, records_total::int, stored_total::int, needs_review_total::int, coalesce(total_amount_sum,0)::float
                    from public.ledger_daily_summary
                    order by processing_date desc
                    limit 14
                    """
                )
                payload["daily_summary"] = [
                    {
                        "processing_date": r[0],
                        "records_total": r[1],
                        "stored_total": r[2],
                        "needs_review_total": r[3],
                        "total_amount_sum": r[4],
                    }
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select coalesce(vendor_name, 'Unknown') as vendor_name, count(*)::int as invoices, coalesce(sum(total_amount), 0)::float as total_spend
                    from public.ledger_records_flat
                    group by 1
                    order by total_spend desc
                    limit 10
                    """
                )
                payload["vendor_spend"] = [
                    {"vendor_name": r[0], "invoices": r[1], "total_spend": r[2]}
                    for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select coalesce(used_provider, 'unknown') as used_provider, count(*)::int as records_total
                    from public.ledger_records_flat
                    group by 1
                    order by records_total desc
                    """
                )
                payload["provider_mix"] = [
                    {"used_provider": r[0], "records_total": r[1]} for r in cur.fetchall()
                ]

                cur.execute(
                    """
                    select
                      processed_at_utc::text,
                      coalesce(vendor_name, 'Unknown') as vendor_name,
                      coalesce(currency, 'NA') as currency,
                      coalesce(total_amount, 0)::float as total_amount,
                      coalesce(invoice_number, '-') as invoice_number,
                      coalesce(used_provider, 'unknown') as used_provider,
                      coalesce(needs_review, false) as needs_review
                    from public.ledger_records_flat
                    order by processed_at_utc desc
                    limit %s
                    """,
                    (limit,),
                )
                payload["recent_records"] = [
                    {
                        "processed_at_utc": r[0],
                        "vendor_name": r[1],
                        "currency": r[2],
                        "total_amount": r[3],
                        "invoice_number": r[4],
                        "used_provider": r[5],
                        "needs_review": bool(r[6]),
                    }
                    for r in cur.fetchall()
                ]
    except Exception as exc:  # noqa: BLE001
        payload["error"] = str(exc)
    return payload


def _dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Invoice Ops Dashboard</title>
  <style>
    :root {
      --c-light: #c1c1c1;
      --c-ink: #2c4251;
      --c-warn: #d16666;
      --c-good: #b6c649;
      --c-white: #ffffff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      color: var(--c-ink);
      background:
        radial-gradient(circle at 15% 20%, rgba(182,198,73,0.12) 0, transparent 45%),
        radial-gradient(circle at 85% 5%, rgba(209,102,102,0.10) 0, transparent 35%),
        var(--c-white);
    }
    .wrap { max-width: 1160px; margin: 0 auto; padding: 28px 16px 40px; }
    .head {
      display: flex; align-items: baseline; justify-content: space-between; gap: 8px; margin-bottom: 18px;
      border-bottom: 1px solid rgba(44,66,81,0.15); padding-bottom: 10px;
    }
    .head h1 { margin: 0; font-size: 1.25rem; letter-spacing: 0.3px; }
    .muted { color: rgba(44,66,81,0.75); font-size: 0.9rem; }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .card {
      background: var(--c-white);
      border: 1px solid rgba(44,66,81,0.16);
      border-radius: 10px;
      padding: 12px;
      box-shadow: 0 3px 12px rgba(44,66,81,0.06);
    }
    .card h3 { margin: 0 0 6px; font-size: 0.8rem; color: rgba(44,66,81,0.8); font-weight: 600; }
    .value { font-size: 1.3rem; font-weight: 700; color: var(--c-ink); }
    .value.good { color: #5f7421; }
    .value.warn { color: #9f4040; }
    .pane-grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 12px;
      margin-bottom: 12px;
    }
    .table-wrap { overflow: auto; max-height: 380px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9rem; }
    th, td { text-align: left; padding: 8px 7px; border-bottom: 1px solid rgba(44,66,81,0.08); white-space: nowrap; }
    th { font-size: 0.78rem; color: rgba(44,66,81,0.76); text-transform: uppercase; letter-spacing: 0.05em; }
    .tag {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 0.74rem;
      font-weight: 600;
      border: 1px solid rgba(44,66,81,0.2);
      color: var(--c-ink);
      background: rgba(193,193,193,0.2);
    }
    .tag.good { border-color: rgba(182,198,73,0.6); background: rgba(182,198,73,0.22); }
    .tag.warn { border-color: rgba(209,102,102,0.6); background: rgba(209,102,102,0.22); }
    .bar-list { display: flex; flex-direction: column; gap: 8px; }
    .bar-row { display: grid; grid-template-columns: 90px 1fr 54px; gap: 8px; align-items: center; font-size: 0.86rem; }
    .bar-track { height: 10px; border-radius: 99px; background: rgba(193,193,193,0.35); overflow: hidden; }
    .bar-fill { height: 100%; background: linear-gradient(90deg, #2c4251, #b6c649); }
    .warn-box {
      margin-top: 10px; border: 1px solid rgba(209,102,102,0.35); background: rgba(209,102,102,0.08);
      color: #7d2f2f; border-radius: 8px; padding: 10px; font-size: 0.86rem;
    }
    .action-btn {
      border: 1px solid rgba(182,198,73,0.7);
      background: rgba(182,198,73,0.18);
      color: var(--c-ink);
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 0.78rem;
      cursor: pointer;
    }
    .action-btn:hover { background: rgba(182,198,73,0.3); }
    .action-btn.warn {
      border-color: rgba(209,102,102,0.7);
      background: rgba(209,102,102,0.14);
    }
    .json-preview {
      margin-top: 8px;
      padding: 10px;
      border-radius: 8px;
      background: rgba(44,66,81,0.05);
      font-family: Consolas, monospace;
      font-size: 0.76rem;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .editor-wrap { margin-top: 8px; display: grid; gap: 8px; }
    .editor-wrap textarea {
      width: 100%;
      min-height: 220px;
      resize: vertical;
      border-radius: 8px;
      border: 1px solid rgba(44,66,81,0.18);
      background: rgba(44,66,81,0.03);
      padding: 10px;
      font-family: Consolas, monospace;
      font-size: 0.76rem;
      color: var(--c-ink);
    }
    .editor-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .history-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
      margin-top: 12px;
    }
    .toolbar {
      display: flex;
      justify-content: flex-end;
      margin: 8px 0 10px;
    }
    .toolbar input {
      width: min(320px, 100%);
      border-radius: 999px;
      border: 1px solid rgba(44,66,81,0.18);
      background: rgba(44,66,81,0.03);
      padding: 8px 12px;
      font-size: 0.84rem;
      color: var(--c-ink);
    }
    @media (max-width: 920px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .pane-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 540px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <h1>Invoice Operations Dashboard</h1>
      <div class="muted" id="refreshAt">Loading...</div>
    </div>
    <div class="grid">
      <div class="card"><h3>Total Records</h3><div class="value" id="kpiTotal">0</div></div>
      <div class="card"><h3>Stored</h3><div class="value good" id="kpiStored">0</div></div>
      <div class="card"><h3>Needs Review</h3><div class="value warn" id="kpiReview">0</div></div>
      <div class="card"><h3>Total Amount</h3><div class="value" id="kpiAmount">0</div></div>
    </div>

    <div class="pane-grid">
      <div class="card">
        <h3>Recent Records</h3>
        <div class="toolbar">
          <input id="recentSearch" type="search" placeholder="Search vendor, invoice, provider..." />
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Processed</th><th>Vendor</th><th>Invoice #</th><th>Amount</th><th>Provider</th><th>Status</th>
              </tr>
            </thead>
            <tbody id="recentBody"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <h3>Provider Mix</h3>
        <div class="bar-list" id="providerBars"></div>
        <h3 style="margin-top:14px;">Backlog</h3>
        <div class="muted">Review Queue: <strong id="reviewQueue">0</strong></div>
        <div class="muted">Dead Letter: <strong id="deadLetter">0</strong></div>
      </div>
    </div>

    <div class="pane-grid">
      <div class="card">
        <h3>Daily Summary</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>Date</th><th>Records</th><th>Stored</th><th>Needs Review</th><th>Total Amount</th></tr>
            </thead>
            <tbody id="dailyBody"></tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <h3>Top Vendor Spend</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>Vendor</th><th>Invoices</th><th>Total Spend</th></tr>
            </thead>
            <tbody id="vendorBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Review Queue</h3>
      <div class="toolbar">
        <input id="reviewSearch" type="search" placeholder="Search document, vendor, source..." />
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>Document ID</th><th>Source</th><th>Vendor</th><th>Amount</th><th>Reasons</th><th>Action</th></tr>
          </thead>
          <tbody id="reviewBody"></tbody>
        </table>
      </div>
    </div>

    <div class="history-grid">
      <div class="card">
        <h3>Review History</h3>
        <div class="toolbar">
          <input id="historySearch" type="search" placeholder="Search status, note, document..." />
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>Resolved</th><th>Status</th><th>Document</th><th>Vendor</th><th>Amount</th><th>Note</th></tr>
            </thead>
            <tbody id="reviewHistoryBody"></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="warn-box" id="errorBox" style="display:none;"></div>
  </div>
  <script>
    let dashboardCache = {
      recent_records: [],
      review_items: [],
      review_history: [],
    };
    function fmtMoney(v) {
      const n = Number(v || 0);
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    function esc(s) {
      return String(s ?? "").replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',\"'\":'&#39;'}[c]));
    }
    function matchesSearch(value, search) {
      if (!search) return true;
      return String(value ?? '').toLowerCase().includes(search);
    }
    function renderRecentRecords(items, search = '') {
      const recentBody = document.getElementById('recentBody');
      recentBody.innerHTML = '';
      const filtered = (items || []).filter(r =>
        matchesSearch(r.processed_at_utc, search) ||
        matchesSearch(r.vendor_name, search) ||
        matchesSearch(r.invoice_number, search) ||
        matchesSearch(r.used_provider, search) ||
        matchesSearch(r.currency, search)
      );
      for (const r of filtered) {
        const statusTag = r.needs_review ? '<span class="tag warn">review</span>' : '<span class="tag good">ok</span>';
        recentBody.innerHTML += `<tr>
          <td>${esc(r.processed_at_utc)}</td>
          <td>${esc(r.vendor_name)}</td>
          <td>${esc(r.invoice_number)}</td>
          <td>${esc(r.currency)} ${fmtMoney(r.total_amount)}</td>
          <td>${esc(r.used_provider)}</td>
          <td>${statusTag}</td>
        </tr>`;
      }
      if (filtered.length === 0) {
        recentBody.innerHTML = '<tr><td colspan="6" class="muted">No recent records match this search.</td></tr>';
      }
    }
    function renderReviewItems(items, search = '') {
      const reviewBody = document.getElementById('reviewBody');
      reviewBody.innerHTML = '';
      const filtered = (items || []).filter(item =>
        matchesSearch(item.document_id, search) ||
        matchesSearch(item.source_file_id, search) ||
        matchesSearch(item.vendor_name, search) ||
        matchesSearch(item.invoice_number, search) ||
        matchesSearch((item.reason_codes || []).join(', '), search)
      );
      for (const item of filtered) {
        const preview = esc(JSON.stringify(item.normalized_record || {}, null, 2));
        const reasons = esc((item.reason_codes || []).join(', '));
        const rawJson = esc(JSON.stringify(item.normalized_record || {}, null, 2));
        reviewBody.innerHTML += `<tr>
          <td>${esc(item.document_id)}</td>
          <td>${esc(item.source_file_id || '-')}</td>
          <td>${esc(item.vendor_name || 'Unknown')}</td>
          <td>${esc(item.currency || 'NA')} ${fmtMoney(item.total_amount)}</td>
          <td>${reasons}</td>
          <td>
            <button class="action-btn" onclick="submitReviewAction('${esc(item.document_id)}', 'approve')">Approve</button>
            <button class="action-btn warn" onclick="submitReviewAction('${esc(item.document_id)}', 'duplicate')">Duplicate</button>
            <button class="action-btn warn" onclick="submitReviewAction('${esc(item.document_id)}', 'reject')">Reject</button>
          </td>
        </tr>
        <tr>
          <td colspan="6">
            <div class="json-preview">${preview}</div>
            <div class="editor-wrap">
              <textarea id="editor-${esc(item.document_id)}">${rawJson}</textarea>
              <div class="editor-actions">
                <button class="action-btn" onclick="submitReviewAction('${esc(item.document_id)}', 'approve')">Approve Edited Record</button>
              </div>
            </div>
          </td>
        </tr>`;
      }
      if (filtered.length === 0) {
        reviewBody.innerHTML = '<tr><td colspan="6" class="muted">No active review items match this search.</td></tr>';
      }
    }
    function renderReviewHistory(items, search = '') {
      const reviewHistoryBody = document.getElementById('reviewHistoryBody');
      reviewHistoryBody.innerHTML = '';
      const filtered = (items || []).filter(item =>
        matchesSearch(item.document_id, search) ||
        matchesSearch(item.status, search) ||
        matchesSearch(item.vendor_name, search) ||
        matchesSearch(item.resolution_note, search) ||
        matchesSearch(item.invoice_number, search)
      );
      for (const item of filtered) {
        reviewHistoryBody.innerHTML += `<tr>
          <td>${esc(item.resolved_at_utc || item.created_at_utc || '-')}</td>
          <td>${esc(item.status)}</td>
          <td>${esc(item.document_id)}</td>
          <td>${esc(item.vendor_name || 'Unknown')}</td>
          <td>${esc(item.currency || 'NA')} ${fmtMoney(item.total_amount)}</td>
          <td>${esc(item.resolution_note || '-')}</td>
        </tr>`;
      }
      if (filtered.length === 0) {
        reviewHistoryBody.innerHTML = '<tr><td colspan="6" class="muted">No review history items match this search.</td></tr>';
      }
    }
    function bindSearchInputs() {
      const recentSearch = document.getElementById('recentSearch');
      const reviewSearch = document.getElementById('reviewSearch');
      const historySearch = document.getElementById('historySearch');
      if (recentSearch && !recentSearch.dataset.bound) {
        recentSearch.addEventListener('input', (e) => renderRecentRecords(dashboardCache.recent_records, e.target.value.trim().toLowerCase()));
        recentSearch.dataset.bound = '1';
      }
      if (reviewSearch && !reviewSearch.dataset.bound) {
        reviewSearch.addEventListener('input', (e) => renderReviewItems(dashboardCache.review_items, e.target.value.trim().toLowerCase()));
        reviewSearch.dataset.bound = '1';
      }
      if (historySearch && !historySearch.dataset.bound) {
        historySearch.addEventListener('input', (e) => renderReviewHistory(dashboardCache.review_history, e.target.value.trim().toLowerCase()));
        historySearch.dataset.bound = '1';
      }
    }
    async function submitReviewAction(documentId, action) {
      const note = window.prompt(`Optional note for ${action}:`, '') ?? '';
      const editor = document.getElementById(`editor-${documentId}`);
      let correctedRecord = null;
      if (action === 'approve' && editor) {
        try {
          correctedRecord = JSON.parse(editor.value);
        } catch (err) {
          window.alert(`Invalid JSON for ${documentId}: ${err.message}`);
          return;
        }
      }
      const resp = await fetch(`/review-items/${encodeURIComponent(documentId)}/resolve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, note, corrected_record: correctedRecord })
      });
      const payload = await resp.json();
      if (!resp.ok) {
        window.alert(payload.detail || 'Review resolution failed.');
        return;
      }
      await loadData();
      window.alert(`${action} complete for ${documentId} (${payload.review_status})`);
    }
    async function loadData() {
      const [dashboardResp, reviewResp, historyResp] = await Promise.all([
        fetch('/dashboard/data?limit=25'),
        fetch('/review-items'),
        fetch('/review-history?limit=20')
      ]);
      const data = await dashboardResp.json();
      const reviewData = await reviewResp.json();
      const historyData = await historyResp.json();
      dashboardCache.recent_records = data.recent_records || [];
      dashboardCache.review_items = reviewData.items || [];
      dashboardCache.review_history = historyData.items || [];
      document.getElementById('refreshAt').textContent = 'Updated: ' + new Date().toLocaleString();
      document.getElementById('kpiTotal').textContent = data.kpis.records_total ?? 0;
      document.getElementById('kpiStored').textContent = data.kpis.stored_total ?? 0;
      document.getElementById('kpiReview').textContent = data.kpis.needs_review_total ?? 0;
      document.getElementById('kpiAmount').textContent = fmtMoney(data.kpis.total_amount_sum ?? 0);
      document.getElementById('reviewQueue').textContent = data.review_queue_total ?? 0;
      document.getElementById('deadLetter').textContent = data.dead_letter_total ?? 0;
      bindSearchInputs();
      renderRecentRecords(
        dashboardCache.recent_records,
        document.getElementById('recentSearch')?.value.trim().toLowerCase() || ''
      );

      const dailyBody = document.getElementById('dailyBody');
      dailyBody.innerHTML = '';
      for (const d of data.daily_summary || []) {
        dailyBody.innerHTML += `<tr>
          <td>${esc(d.processing_date)}</td>
          <td>${esc(d.records_total)}</td>
          <td>${esc(d.stored_total)}</td>
          <td>${esc(d.needs_review_total)}</td>
          <td>${fmtMoney(d.total_amount_sum)}</td>
        </tr>`;
      }

      const vendorBody = document.getElementById('vendorBody');
      vendorBody.innerHTML = '';
      for (const v of data.vendor_spend || []) {
        vendorBody.innerHTML += `<tr>
          <td>${esc(v.vendor_name)}</td>
          <td>${esc(v.invoices)}</td>
          <td>${fmtMoney(v.total_spend)}</td>
        </tr>`;
      }

      const bars = document.getElementById('providerBars');
      bars.innerHTML = '';
      const maxVal = Math.max(...(data.provider_mix || []).map(x => x.records_total), 1);
      for (const p of data.provider_mix || []) {
        const w = Math.round((p.records_total / maxVal) * 100);
        bars.innerHTML += `<div class="bar-row">
          <div>${esc(p.used_provider)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%"></div></div>
          <div>${esc(p.records_total)}</div>
        </div>`;
      }

      renderReviewItems(
        dashboardCache.review_items,
        document.getElementById('reviewSearch')?.value.trim().toLowerCase() || ''
      );
      renderReviewHistory(
        dashboardCache.review_history,
        document.getElementById('historySearch')?.value.trim().toLowerCase() || ''
      );

      const errorBox = document.getElementById('errorBox');
      if (data.error) {
        errorBox.style.display = 'block';
        errorBox.textContent = 'Dashboard query warning: ' + data.error;
      } else {
        errorBox.style.display = 'none';
      }
    }
    loadData();
    setInterval(loadData, 30000);
  </script>
</body>
</html>"""
