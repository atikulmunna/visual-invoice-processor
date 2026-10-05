"""The vendor master: one vendor per supplier of an organization, whatever spelling a document uses.

Records are linked as they are stored. Matching is deliberately safe: the same tax ID or BIN, or
the same name once case, punctuation, and company suffixes are ignored. Anything looser, such as
"RYANS" and "RYANS Computers", is only suggested, because a wrong merge would be silent.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from difflib import SequenceMatcher
from typing import Any

import psycopg

from app.records_store import trend_months

# Trailing words that name a company's legal form rather than the company itself.
LEGAL_SUFFIXES = {
    "co", "company", "corp", "corporation", "inc", "incorporated", "limited", "llc", "ltd", "plc", "pvt", "private",
}
SIMILAR_NAMES = 0.85
MAX_SUGGESTIONS = 20
CURRENCY_CODE = re.compile(r"^[A-Z]{3}$")


class VendorNotFound(Exception):
    """No such vendor in this organization."""


class VendorConflict(Exception):
    """Another vendor of the organization already has this name or tax ID."""

    def __init__(self, message: str, other_id: int) -> None:
        super().__init__(message)
        self.other_id = other_id


def normalize_vendor_name(name: str | None) -> str | None:
    """The form names are matched in: casefolded, punctuation dropped, legal suffixes removed.

    Letters, digits, and combining marks are kept, so Bangla vowel signs stay part of their word.
    """
    if not name:
        return None
    text = unicodedata.normalize("NFKC", name).casefold().replace("&", " and ")
    kept = "".join(ch if unicodedata.category(ch)[0] in "LNM" else " " for ch in text)
    words = kept.split()
    while len(words) > 1 and words[-1] in LEGAL_SUFFIXES:
        words.pop()
    return " ".join(words) or None


def normalize_tax_id(value: str | None) -> str | None:
    cleaned = re.sub(r"[^0-9A-Za-z]", "", value or "").upper()
    return cleaned or None


def _currency(value: str | None) -> str | None:
    code = (value or "").strip().upper()
    return code if CURRENCY_CODE.match(code) else None


def link_vendor(
    cur: psycopg.Cursor,
    *,
    org_id: str,
    name: str | None,
    tax_id: str | None,
    currency: str | None,
) -> int | None:
    """The vendor for a record being stored, found or created inside the caller's transaction.

    Returns None when the document names no vendor and its tax ID matches none.
    """
    normalized = normalize_vendor_name(name)
    tax = normalize_tax_id(tax_id)
    vendor_id = None
    if tax:
        row = cur.execute(
            "select id from public.vendors where org_id = %s and tax_id = %s order by id limit 1", (org_id, tax)
        ).fetchone()
        vendor_id = row[0] if row else None
    if vendor_id is None and normalized:
        row = cur.execute(
            "select vendor_id from public.vendor_aliases where org_id = %s and normalized = %s", (org_id, normalized)
        ).fetchone()
        vendor_id = row[0] if row else None
    if vendor_id is None:
        if not normalized:
            return None
        return _create_vendor(cur, org_id=org_id, name=str(name).strip(), normalized=normalized, tax=tax,
                              currency=_currency(currency))
    if tax:
        cur.execute(
            "update public.vendors set tax_id = %s, updated_at_utc = now() where id = %s and tax_id is null",
            (tax, vendor_id),
        )
    if normalized:
        cur.execute(
            """
            insert into public.vendor_aliases (org_id, normalized, vendor_id, alias) values (%s, %s, %s, %s)
            on conflict (org_id, normalized) do nothing
            """,
            (org_id, normalized, vendor_id, str(name).strip()),
        )
    return vendor_id


def _create_vendor(
    cur: psycopg.Cursor, *, org_id: str, name: str, normalized: str, tax: str | None, currency: str | None
) -> int:
    vendor_id = cur.execute(
        "insert into public.vendors (org_id, name, tax_id, default_currency) values (%s, %s, %s, %s) returning id",
        (org_id, name, tax, currency),
    ).fetchone()[0]
    claimed = cur.execute(
        """
        insert into public.vendor_aliases (org_id, normalized, vendor_id, alias) values (%s, %s, %s, %s)
        on conflict (org_id, normalized) do nothing returning vendor_id
        """,
        (org_id, normalized, vendor_id, name),
    ).fetchone()
    if claimed:
        return vendor_id
    # Another transaction created this vendor first; use theirs and drop ours.
    cur.execute("delete from public.vendors where id = %s", (vendor_id,))
    return cur.execute(
        "select vendor_id from public.vendor_aliases where org_id = %s and normalized = %s", (org_id, normalized)
    ).fetchone()[0]


def link_unlinked_records(dsn: str, *, org_id: str | None = None) -> int:
    """Links stored records that have no vendor yet, oldest first, so the first spelling seen becomes the name."""
    scope = "and org_id = %s" if org_id else ""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        rows = conn.execute(
            f"""
            select id, org_id::text, record_json ->> 'vendor_name', record_json ->> 'vendor_tax_id',
                   record_json ->> 'currency'
            from public.ledger_records
            where vendor_id is null and org_id is not null {scope}
            order by processed_at_utc, id
            """,
            (org_id,) if org_id else (),
        ).fetchall()
        linked = 0
        with conn.cursor() as cur:
            for record_id, record_org, name, tax_id, currency in rows:
                vendor_id = link_vendor(cur, org_id=record_org, name=name, tax_id=tax_id, currency=currency)
                if vendor_id is not None:
                    cur.execute("update public.ledger_records set vendor_id = %s where id = %s", (vendor_id, record_id))
                    linked += 1
    return linked


def _totals(conn: psycopg.Connection, org_id: str, vendor_id: int | None = None) -> dict[int, list[dict[str, Any]]]:
    """Spend per currency for each vendor, largest first."""
    only_one = "and vendor_id = %s" if vendor_id else ""
    rows = conn.execute(
        f"""
        select vendor_id, upper(record_json ->> 'currency'), coalesce(sum((record_json ->> 'total_amount')::numeric), 0)
        from public.ledger_records
        where org_id = %s and vendor_id is not null {only_one}
          and record_json ->> 'currency' is not null
        group by 1, 2 order by 3 desc
        """,
        (org_id, vendor_id) if vendor_id else (org_id,),
    ).fetchall()
    totals: dict[int, list[dict[str, Any]]] = {}
    for owner, currency, total in rows:
        totals.setdefault(owner, []).append({"currency": currency, "total": float(total)})
    return totals


_VENDOR_ROWS = """
    select v.id, v.name, v.tax_id, v.default_currency,
           coalesce((select array_agg(a.alias order by a.alias) from public.vendor_aliases a where a.vendor_id = v.id),
                    array[]::text[]),
           count(lr.id), min(lr.record_json ->> 'invoice_date'), max(lr.record_json ->> 'invoice_date')
    from public.vendors v
    left join public.ledger_records lr on lr.vendor_id = v.id
    where v.org_id = %s {extra}
    group by v.id
"""


def _vendor_view(row: tuple[Any, ...], totals: dict[int, list[dict[str, Any]]]) -> dict[str, Any]:
    return {
        "id": row[0],
        "name": row[1],
        "tax_id": row[2],
        "default_currency": row[3],
        "aliases": [alias for alias in row[4] if alias != row[1]],
        "records": row[5],
        "first_invoice_date": row[6],
        "last_invoice_date": row[7],
        "totals": totals.get(row[0], []),
    }


def list_vendors(dsn: str, *, org_id: str) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        rows = conn.execute(_VENDOR_ROWS.format(extra="") + " order by lower(v.name), v.id", (org_id,)).fetchall()
        totals = _totals(conn, org_id)
    return [_vendor_view(row, totals) for row in rows]


def vendor_detail(dsn: str, *, org_id: str, vendor_id: int, today: date) -> dict[str, Any]:
    """A vendor with its spend per currency and by invoice month in its main currency."""
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        row = conn.execute(_VENDOR_ROWS.format(extra="and v.id = %s"), (org_id, vendor_id)).fetchone()
        if row is None:
            raise VendorNotFound(vendor_id)
        vendor = _vendor_view(row, _totals(conn, org_id, vendor_id))
        codes = [total["currency"] for total in vendor["totals"]]
        currency = vendor["default_currency"] if vendor["default_currency"] in codes else (codes[0] if codes else None)
        months = trend_months(today)
        monthly = {
            month: {"total": float(total), "count": count}
            for month, total, count in conn.execute(
                """
                select substr(record_json ->> 'invoice_date', 1, 7), coalesce(sum((record_json ->> 'total_amount')::numeric), 0),
                       count(*)
                from public.ledger_records
                where org_id = %s and vendor_id = %s and upper(record_json ->> 'currency') = %s
                  and record_json ->> 'invoice_date' between %s and %s
                group by 1
                """,
                (org_id, vendor_id, currency, f"{months[0]}-01", f"{months[-1]}-31"),
            ).fetchall()
        }
    vendor["currency"] = currency
    vendor["months"] = [{"month": month, **monthly.get(month, {"total": 0.0, "count": 0})} for month in months]
    return vendor


def suggest_merges(vendors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Pairs that look like the same supplier: one name begins with the other's words, or the names are
    nearly identical. The vendor with more records is proposed as the one to keep."""
    named = [(vendor, (normalize_vendor_name(vendor["name"]) or "").split()) for vendor in vendors]
    suggestions = []
    for index, (first, first_words) in enumerate(named):
        for second, second_words in named[index + 1:]:
            if not first_words or not second_words:
                continue
            shorter, longer = sorted((first_words, second_words), key=len)
            prefix = longer[: len(shorter)] == shorter
            similar = SequenceMatcher(None, " ".join(first_words), " ".join(second_words)).ratio() >= SIMILAR_NAMES
            if not (prefix or similar):
                continue
            keep, merge = sorted((first, second), key=lambda vendor: (-vendor["records"], vendor["id"]))
            suggestions.append({"keep": keep["id"], "merge": merge["id"], "keep_name": keep["name"],
                                "merge_name": merge["name"]})
    return suggestions[:MAX_SUGGESTIONS]


def merge_vendors(dsn: str, *, org_id: str, keep_id: int, merge_ids: list[int]) -> None:
    """Moves the records and names of the merged vendors onto the one kept, then removes them."""
    others = sorted(set(merge_ids) - {keep_id})
    if not others:
        raise ValueError("Choose at least one other vendor to merge")
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        found = {row[0] for row in conn.execute(
            "select id from public.vendors where org_id = %s and id = any(%s)", (org_id, [keep_id, *others])
        ).fetchall()}
        if found != {keep_id, *others}:
            raise VendorNotFound(sorted({keep_id, *others} - found))
        conn.execute(
            "update public.ledger_records set vendor_id = %s where org_id = %s and vendor_id = any(%s)",
            (keep_id, org_id, others),
        )
        conn.execute(
            "update public.vendor_aliases set vendor_id = %s where org_id = %s and vendor_id = any(%s)",
            (keep_id, org_id, others),
        )
        # The kept vendor learns a tax ID or usual currency it was missing.
        conn.execute(
            """
            update public.vendors kept set
              tax_id = coalesce(kept.tax_id, (select tax_id from public.vendors where id = any(%s) and tax_id is not null
                                              order by id limit 1)),
              default_currency = coalesce(kept.default_currency,
                                          (select default_currency from public.vendors
                                           where id = any(%s) and default_currency is not null order by id limit 1)),
              updated_at_utc = now()
            where kept.id = %s
            """,
            (others, others, keep_id),
        )
        conn.execute("delete from public.vendors where id = any(%s)", (others,))


def update_vendor(
    dsn: str,
    *,
    org_id: str,
    vendor_id: int,
    name: str,
    tax_id: str | None,
    default_currency: str | None,
) -> None:
    """Renames a vendor or sets its tax ID and usual currency. The new name also becomes an alias,
    so documents printed with it link here from now on."""
    normalized = normalize_vendor_name(name)
    if not normalized:
        raise ValueError("Enter the vendor's name")
    tax = normalize_tax_id(tax_id)
    currency = _currency(default_currency)
    if default_currency and not currency:
        raise ValueError("Use a three-letter currency code, such as BDT or USD")
    with psycopg.connect(dsn, prepare_threshold=None) as conn:
        if conn.execute(
            "select 1 from public.vendors where org_id = %s and id = %s", (org_id, vendor_id)
        ).fetchone() is None:
            raise VendorNotFound(vendor_id)
        owner = conn.execute(
            "select a.vendor_id, v.name from public.vendor_aliases a join public.vendors v on v.id = a.vendor_id "
            "where a.org_id = %s and a.normalized = %s",
            (org_id, normalized),
        ).fetchone()
        if owner and owner[0] != vendor_id:
            raise VendorConflict(f"{owner[1]} already goes by this name. Merge the two vendors instead.", owner[0])
        if tax:
            holder = conn.execute(
                "select id, name from public.vendors where org_id = %s and tax_id = %s and id <> %s",
                (org_id, tax, vendor_id),
            ).fetchone()
            if holder:
                raise VendorConflict(f"{holder[1]} already has this tax ID. Merge the two vendors instead.", holder[0])
        conn.execute(
            "update public.vendors set name = %s, tax_id = %s, default_currency = %s, updated_at_utc = now() "
            "where id = %s",
            (name.strip(), tax, currency, vendor_id),
        )
        conn.execute(
            """
            insert into public.vendor_aliases (org_id, normalized, vendor_id, alias) values (%s, %s, %s, %s)
            on conflict (org_id, normalized) do nothing
            """,
            (org_id, normalized, vendor_id, name.strip()),
        )
