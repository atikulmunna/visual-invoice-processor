import { useEffect, useState, type ReactNode } from "react";
import { Link } from "react-router";

import { api } from "../lib/api";
import { formatDate, formatMoney } from "../lib/format";
import { currencyNote } from "../lib/labels";
import type { RecordDetail } from "../lib/records";
import {
  AMOUNT_FIELDS,
  AMOUNT_LABELS,
  DOCUMENT_TYPE_OPTIONS,
  optionLabel,
  PAYMENT_METHOD_OPTIONS,
  type StoredRecord,
} from "../lib/review";
import { Dialog } from "./Dialog";
import { DocumentViewer } from "./DocumentViewer";
import { Badge, EmptyState, Skeleton } from "./ui";

function text(value: unknown): string | null {
  return value === null || value === undefined || value === "" ? null : String(value);
}

function amount(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="fact">
      <dt>{label}</dt>
      <dd>{children ?? <span className="muted">Not shown</span>}</dd>
    </div>
  );
}

function LineItems({ lines, currency }: { lines: StoredRecord[]; currency: string | null }) {
  return (
    <div className="table-wrap" role="region" aria-label="Line items" tabIndex={0}>
      <table className="table table-compact">
        <thead>
          <tr>
            <th scope="col">Description</th>
            <th scope="col" className="numeric">Qty</th>
            <th scope="col" className="numeric">Unit price</th>
            <th scope="col" className="numeric">Line total</th>
          </tr>
        </thead>
        <tbody>
          {lines.map((line, index) => (
            <tr key={index}>
              <td>{text(line.description)}</td>
              <td className="numeric">{text(line.quantity)}</td>
              <td className="numeric">{formatMoney(amount(line.unit_price) ?? 0, currency)}</td>
              <td className="numeric">{formatMoney(amount(line.line_total) ?? 0, currency)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RecordFacts({ detail }: { detail: RecordDetail }) {
  const record = detail.record;
  const currency = text(record.currency);
  const lines = Array.isArray(record.line_items) ? (record.line_items as StoredRecord[]) : [];
  const note = currencyNote(Boolean(record.currency_assumed), currency);
  const printed = text(record.vendor_name);

  return (
    <div className="record-facts">
      <div className="row">
        {detail.reviewed && <Badge tone="success">Approved by a reviewer</Badge>}
        {detail.flagged && <Badge tone="warning">Flagged by checks</Badge>}
        <span className="muted">Added {formatDate(detail.added_at)}</span>
      </div>
      <section>
        <h3 className="facts-title">Details</h3>
        <dl className="facts">
          <Fact label="Vendor">
            {detail.vendor ? <Link to={`/vendors/${detail.vendor.id}`}>{detail.vendor.name}</Link> : text(record.vendor_name)}
            {detail.vendor && printed && printed !== detail.vendor.name && (
              <span className="fact-note fact-note-muted">Printed as {printed}</span>
            )}
          </Fact>
          <Fact label="Invoice number">{text(record.invoice_number)}</Fact>
          <Fact label="Invoice date">{formatDate(text(record.invoice_date))}</Fact>
          <Fact label="Due date">{formatDate(text(record.due_date))}</Fact>
          <Fact label="Currency">
            {currency && (
              <>
                {currency}
                {note && <span className="fact-note">{note}</span>}
              </>
            )}
          </Fact>
          <Fact label="Vendor tax ID or BIN">{text(record.vendor_tax_id)}</Fact>
          <Fact label="Document type">{optionLabel(DOCUMENT_TYPE_OPTIONS, record.document_type)}</Fact>
          <Fact label="Payment method">{optionLabel(PAYMENT_METHOD_OPTIONS, record.payment_method)}</Fact>
        </dl>
      </section>
      <section>
        <h3 className="facts-title">Amounts</h3>
        <dl className="amounts">
          {AMOUNT_FIELDS.map((field) => (
            <div key={field} className={field === "total_amount" ? "amount-row amount-total" : "amount-row"}>
              <dt>{AMOUNT_LABELS[field]}</dt>
              <dd>{formatMoney(amount(record[field]) ?? 0, currency)}</dd>
            </div>
          ))}
        </dl>
      </section>
      {lines.length > 0 && (
        <section>
          <h3 className="facts-title">Line items</h3>
          <LineItems lines={lines} currency={currency} />
        </section>
      )}
    </div>
  );
}

type Loaded = { id: number; detail: RecordDetail } | { id: number; error: string };

/** A stored record beside its original document, opened from the records list. */
export function RecordDrawer({ recordId, onClose }: { recordId: number | null; onClose: () => void }) {
  const [loaded, setLoaded] = useState<Loaded | null>(null);

  useEffect(() => {
    if (recordId === null) {
      return;
    }
    let cancelled = false;
    api.record(recordId).then(
      (detail) => !cancelled && setLoaded({ id: recordId, detail }),
      (error: unknown) =>
        !cancelled && setLoaded({ id: recordId, error: error instanceof Error ? error.message : "Unknown error" }),
    );
    return () => {
      cancelled = true;
    };
  }, [recordId]);

  const current = loaded?.id === recordId ? loaded : null;
  const detail = current && "detail" in current ? current.detail : null;
  const title = detail
    ? (detail.vendor?.name ?? text(detail.record.vendor_name) ?? "Vendor not shown")
    : current
      ? "Record not available"
      : "Loading record";

  return (
    <Dialog open={recordId !== null} onClose={onClose} title={title} variant="drawer" className="record-drawer">
      {recordId !== null && !current && (
        <div className="stack" aria-busy="true">
          <Skeleton height={320} />
          <Skeleton width="60%" />
        </div>
      )}
      {current && "error" in current && (
        <EmptyState icon="alert" title="This record could not be opened">
          {current.error}
        </EmptyState>
      )}
      {detail && (
        <div className="record-detail">
          <DocumentViewer document={detail.document} />
          <RecordFacts detail={detail} />
        </div>
      )}
    </Dialog>
  );
}
