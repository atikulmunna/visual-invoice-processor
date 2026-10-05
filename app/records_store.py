"""Read-only queries over an organization's stored records, for the records list and the overview."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Literal

import psycopg

SortKey = Literal["added", "invoice_date", "vendor", "total"]

# Sortable columns by the name the API accepts. Only these SQL fragments ever reach the query text.
SORT_COLUMNS: dict[SortKey, str] = {
    "added": "processed_at_utc",
    "invoice_date": "invoice_date",
    "vendor": "lower(vendor_name)",
    "total": "total_amount",
}

MAX_VENDOR_OPTIONS = 200
# Keeps an export well inside the 6 MB Lambda response limit.
MAX_EXPORT_RECORDS = 5000
TREND_MONTHS = 12
TOP_VENDORS = 5

# One row per stored record of an organization, with the fields the workspace filters on.
# vendor_name is the linked vendor's chosen name, so every spelling of a vendor adds up together;
# printed_vendor_name keeps the document's own spelling.
_RECORDS = """
    select
      lr.id,
      lr.processed_at_utc,
      lr.record_json,
      lr.vendor_id,
      coalesce(v.name, lr.record_json ->> 'vendor_name') as vendor_name,
      lr.record_json ->> 'vendor_name' as printed_vendor_name,
      lr.record_json ->> 'vendor_tax_id' as vendor_tax_id,
      lr.record_json ->> 'invoice_number' as invoice_number,
      lr.record_json ->> 'invoice_date' as invoice_date,
      upper(lr.record_json ->> 'currency') as currency,
      (lr.record_json ->> 'total_amount')::numeric as total_amount,
      coalesce((lr.record_json ->> 'needs_review')::boolean, false) as flagged,
      coalesce(lr.metadata_json ->> 'resolution_source', '') = 'manual_review' as reviewed
    from public.ledger_records lr
    left join public.vendors v on v.id = lr.vendor_id
    where lr.org_id = %(org_id)s
"""


@dataclass(frozen=True)
class RecordFilters:
    query: str | None = None
    currency: str | None = None
    vendor: str | None = None
    date_from: date | None = None
    date_to: date | None = None
    flagged: bool = False
    reviewed: bool = False


def _escape_like(text: str) -> str:
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _filter_clause(filters: RecordFilters) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if filters.query and filters.query.strip():
        clauses.append(
            "(vendor_name ilike %(pattern)s or printed_vendor_name ilike %(pattern)s"
            " or invoice_number ilike %(pattern)s or vendor_tax_id ilike %(pattern)s)"
        )
        params["pattern"] = f"%{_escape_like(filters.query.strip())}%"
    if filters.currency:
        clauses.append("currency = %(currency)s")
        params["currency"] = filters.currency.upper()
    if filters.vendor:
        clauses.append("vendor_name = %(vendor)s")
        params["vendor"] = filters.vendor
    # Invoice dates are stored as ISO text, so text comparison orders them correctly.
    if filters.date_from:
        clauses.append("invoice_date >= %(date_from)s")
        params["date_from"] = filters.date_from.isoformat()
    if filters.date_to:
        clauses.append("invoice_date <= %(date_to)s")
        params["date_to"] = filters.date_to.isoformat()
    if filters.flagged:
        clauses.append("flagged")
    if filters.reviewed:
        clauses.append("reviewed")
    return (" where " + " and ".join(clauses)) if clauses else "", params


class ExportTooLarge(Exception):
    """More records match than one export may hold."""

    def __init__(self, total: int, limit: int) -> None:
        super().__init__(f"{total} records match; an export holds at most {limit}")
        self.total = total
        self.limit = limit


def _order_clause(sort: SortKey, descending: bool) -> str:
    direction = "desc" if descending else "asc"
    return f"order by {SORT_COLUMNS[sort]} {direction} nulls last, id {direction}"


def _count(conn: psycopg.Connection, where: str, params: dict[str, Any]) -> int:
    return conn.execute(f"with records as ({_RECORDS}) select count(*) from records{where}", params).fetchone()[0]


def _row_summary(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "id": row[0],
        "added_at": row[1].isoformat(),
        "vendor_name": row[2],
        "invoice_number": row[3],
        "invoice_date": row[4],
        "currency": row[5],
        "total_amount": float(row[6]) if row[6] is not None else None,
        "flagged": row[7],
        "reviewed": row[8],
    }


def search_records(
    dsn: str,
    *,
    org_id: str,
    filters: RecordFilters,
    sort: SortKey = "added",
    descending: bool = True,
    page: int = 1,
    page_size: int = 25,
) -> dict[str, Any]:
    """One page of the organization's records, plus how many match in total."""
    where, params = _filter_clause(filters)
    params.update(org_id=org_id, limit=page_size, offset=(page - 1) * page_size)
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        total = _count(conn, where, params)
        rows = conn.execute(
            f"""
            with records as ({_RECORDS})
            select id, processed_at_utc, vendor_name, invoice_number, invoice_date, currency,
                   total_amount, flagged, reviewed
            from records{where}
            {_order_clause(sort, descending)}
            limit %(limit)s offset %(offset)s
            """,
            params,
        ).fetchall()
    return {"items": [_row_summary(row) for row in rows], "total": total}


def export_records(
    dsn: str,
    *,
    org_id: str,
    filters: RecordFilters,
    sort: SortKey = "added",
    descending: bool = True,
    limit: int = MAX_EXPORT_RECORDS,
) -> list[dict[str, Any]]:
    """Every record matching the filters, in list order, with the full stored record for a file download.

    Raises ExportTooLarge rather than returning a file that silently leaves records out.
    """
    where, params = _filter_clause(filters)
    params.update(org_id=org_id)
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        total = _count(conn, where, params)
        if total > limit:
            raise ExportTooLarge(total, limit)
        rows = conn.execute(
            f"""
            with records as ({_RECORDS})
            select id, processed_at_utc, record_json, flagged, reviewed, vendor_name
            from records{where}
            {_order_clause(sort, descending)}
            """,
            params,
        ).fetchall()
    return [
        {"id": row[0], "added_at": row[1], "record": row[2] or {}, "flagged": row[3], "reviewed": row[4],
         "vendor_name": row[5]}
        for row in rows
    ]


def record_facets(dsn: str, *, org_id: str) -> dict[str, Any]:
    """The currencies and vendors the organization's records contain, to offer as filters."""
    params = {"org_id": org_id, "limit": MAX_VENDOR_OPTIONS}
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        currencies = conn.execute(
            f"""
            with records as ({_RECORDS})
            select currency, count(*) from records where currency is not null
            group by 1 order by 2 desc, 1
            """,
            params,
        ).fetchall()
        vendors = conn.execute(
            f"""
            with records as ({_RECORDS})
            select distinct vendor_name from records where vendor_name is not null
            order by vendor_name limit %(limit)s
            """,
            params,
        ).fetchall()
    return {
        "currencies": [{"code": code, "count": count} for code, count in currencies],
        "vendors": [row[0] for row in vendors],
    }


def get_record(dsn: str, *, org_id: str, record_id: int) -> dict[str, Any] | None:
    """A stored record with the key of its source file, or None when the organization has no such record."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        row = conn.execute(
            """
            select lr.id, lr.processed_at_utc, lr.drive_file_id, lr.record_json, lr.metadata_json, v.id, v.name
            from public.ledger_records lr
            left join public.vendors v on v.id = lr.vendor_id
            where lr.org_id = %s and lr.id = %s
            """,
            (org_id, record_id),
        ).fetchone()
    if row is None:
        return None
    record, metadata = row[3] or {}, row[4] or {}
    return {
        "id": row[0],
        "added_at": row[1].isoformat(),
        "source_key": row[2],
        "record": record,
        "vendor": {"id": row[5], "name": row[6]} if row[5] is not None else None,
        "flagged": bool(record.get("needs_review")),
        "reviewed": metadata.get("resolution_source") == "manual_review",
    }


def trend_months(today: date, count: int = TREND_MONTHS) -> list[str]:
    """The last `count` calendar months as YYYY-MM, oldest first, ending with today's month."""
    months = []
    year, month = today.year, today.month
    for _ in range(count):
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year, month - 1) if month > 1 else (year - 1, 12)
    return months[::-1]


def pick_currency(available: list[str], requested: str | None, base: str | None) -> str | None:
    """The currency to show: the one asked for, else the base currency, else the most used one."""
    for candidate in (requested, base):
        if candidate and candidate.upper() in available:
            return candidate.upper()
    return available[0] if available else None


def overview(dsn: str, *, org_id: str, currency: str | None, base_currency: str | None, today: date) -> dict[str, Any]:
    """Spending in one currency by invoice month, top vendors, and record counts.

    Totals never mix currencies; `currencies` lists the others the organization can switch to.
    """
    months = trend_months(today)
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        counts = conn.execute(
            f"""
            with records as ({_RECORDS})
            select count(*), count(*) filter (where flagged) from records
            """,
            {"org_id": org_id},
        ).fetchone()
        currencies = [
            row[0]
            for row in conn.execute(
                f"""
                with records as ({_RECORDS})
                select currency from records where currency is not null group by 1 order by count(*) desc, 1
                """,
                {"org_id": org_id},
            ).fetchall()
        ]
        chosen = pick_currency(currencies, currency, base_currency)
        params = {"org_id": org_id, "currency": chosen, "start": f"{months[0]}-01", "end": f"{months[-1]}-31"}
        monthly = {
            row[0]: {"total": float(row[1]), "count": row[2]}
            for row in conn.execute(
                f"""
                with records as ({_RECORDS})
                select substr(invoice_date, 1, 7), coalesce(sum(total_amount), 0), count(*) from records
                where currency = %(currency)s and invoice_date between %(start)s and %(end)s
                group by 1
                """,
                params,
            ).fetchall()
        }
        vendors = conn.execute(
            f"""
            with records as ({_RECORDS})
            select vendor_id, vendor_name, coalesce(sum(total_amount), 0), count(*) from records
            where currency = %(currency)s and invoice_date between %(start)s and %(end)s and vendor_name is not null
            group by 1, 2 order by 3 desc, 2 limit %(limit)s
            """,
            {**params, "limit": TOP_VENDORS},
        ).fetchall()
    return {
        "currency": chosen,
        "currencies": currencies,
        "records_total": counts[0],
        "flagged_total": counts[1],
        "months": [{"month": month, **monthly.get(month, {"total": 0.0, "count": 0})} for month in months],
        "top_vendors": [
            {"vendor_id": vendor_id, "vendor_name": name, "total": float(total), "count": count}
            for vendor_id, name, total, count in vendors
        ],
    }
