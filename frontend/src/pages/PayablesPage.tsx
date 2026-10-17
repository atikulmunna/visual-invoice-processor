import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router";

import { PageHeader } from "../components/PageHeader";
import { PaymentControls } from "../components/PaymentControls";
import { RecordDrawer } from "../components/RecordDrawer";
import { buttonClass, Card, EmptyState, Skeleton } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatMoney } from "../lib/format";
import { agenda, localToday, type AgendaGroup, type Payable } from "../lib/payables";

function dueText(item: Payable): string {
  const due = formatDate(item.payment.due_date);
  if (!due) {
    return "No due date";
  }
  return item.payment.due_printed ? `Due ${due}` : `Due ${due} (invoice date)`;
}

function AgendaRow({ item, open, changed }: { item: Payable; open: (id: number) => void; changed: () => void }) {
  return (
    <li className="agenda-row">
      <button type="button" className="agenda-bill" onClick={() => open(item.id)}>
        <strong>{item.vendor_name ?? "Vendor not shown"}</strong>
        <span className="muted">
          {[item.invoice_number, dueText(item)].filter(Boolean).join(" · ")}
        </span>
      </button>
      <span className="agenda-amount">
        {item.total_amount !== null ? formatMoney(item.total_amount, item.currency) : ""}
      </span>
      <PaymentControls recordId={item.id} payment={item.payment} onChange={changed} />
    </li>
  );
}

function AgendaSection({ group, open, changed }: { group: AgendaGroup; open: (id: number) => void; changed: () => void }) {
  return (
    <Card>
      <section aria-labelledby={`agenda-${group.key}`}>
        <div className="agenda-head">
          <h2 id={`agenda-${group.key}`} className={group.key === "overdue" ? "card-title agenda-overdue" : "card-title"}>
            {group.label}
          </h2>
          <span className="muted agenda-totals">
            {group.totals.map((total) => formatMoney(total.total, total.currency)).join(" and ")}
          </span>
        </div>
        <ul className="agenda-list">
          {group.items.map((item) => (
            <AgendaRow key={item.id} item={item} open={open} changed={changed} />
          ))}
        </ul>
      </section>
    </Card>
  );
}

export function PayablesPage() {
  const [items, setItems] = useState<Payable[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openRecord, setOpenRecord] = useState<number | null>(null);

  const load = useCallback(() => {
    api.payables().then(
      (response) => {
        setItems(response.items);
        setError(null);
      },
      (failure: unknown) => setError(failure instanceof Error ? failure.message : "Unknown error"),
    );
  }, []);

  useEffect(load, [load]);

  const groups = items ? agenda(items, localToday()) : [];

  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Payables"
        title="Bills to pay"
        subtitle="Unpaid and scheduled bills, by when they should be paid. A bill with no printed due date is due on its invoice date."
      />
      {error && !items && (
        <EmptyState icon="alert" title="Payables could not be loaded" action={
          <button type="button" className={buttonClass("tinted")} onClick={load}>Try again</button>}>
          {error}
        </EmptyState>
      )}
      {!items && !error && (
        <div className="stack" aria-busy="true" aria-label="Loading payables">
          <Skeleton height={160} />
          <Skeleton height={160} />
        </div>
      )}
      {items && groups.length === 0 && (
        <Card>
          <EmptyState icon="check" title="Nothing to pay"
            action={<Link to="/records" className={buttonClass("tinted")}>See all records</Link>}>
            Every stored bill is marked paid. Receipts count as paid; invoices wait here until you mark them.
          </EmptyState>
        </Card>
      )}
      {groups.map((group) => (
        <AgendaSection key={group.key} group={group} open={setOpenRecord} changed={load} />
      ))}
      <RecordDrawer
        recordId={openRecord}
        onClose={() => {
          setOpenRecord(null);
          load();
        }}
      />
    </div>
  );
}
