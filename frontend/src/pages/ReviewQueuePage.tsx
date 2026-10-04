import { useEffect, useState } from "react";
import { Link } from "react-router";

import { DataTable, type Column } from "../components/DataTable";
import { Icon } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { buttonClass, EmptyState } from "../components/ui";
import { api } from "../lib/api";
import { formatMoney, formatRelativeTime } from "../lib/format";
import { reasonLabel } from "../lib/labels";
import type { ReviewSummary } from "../lib/review";

const COLUMNS: Column<ReviewSummary>[] = [
  {
    key: "document",
    header: "Document",
    render: (item) => (
      <div className="batch-name">
        <Link to={`/review/${encodeURIComponent(item.document_id)}`}>
          <strong>{item.vendor_name ?? "Vendor not found"}</strong>
        </Link>
        <span className="muted">{item.invoice_number ? `Invoice ${item.invoice_number}` : "No invoice number"}</span>
      </div>
    ),
  },
  {
    key: "why",
    header: "Why it needs review",
    render: (item) => <span className="review-reasons-cell">{item.reason_codes.map(reasonLabel).join(". ")}</span>,
  },
  {
    key: "total",
    header: "Total",
    numeric: true,
    render: (item) => (item.total_amount != null ? formatMoney(item.total_amount, item.currency) : ""),
  },
  { key: "received", header: "Received", render: (item) => formatRelativeTime(item.created_at) ?? "" },
];

export function ReviewQueuePage() {
  const [items, setItems] = useState<ReviewSummary[] | null>(null);

  useEffect(() => {
    api.reviewQueue().then(
      (response) => setItems(response.items),
      () => setItems([]),
    );
  }, []);

  const first = items?.[0];

  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Review"
        title="Review queue"
        subtitle="Documents that need a person to confirm or fill in details before they are stored. Oldest first."
        actions={
          first ? (
            <Link to={`/review/${encodeURIComponent(first.document_id)}`} className={buttonClass("primary")}>
              Start reviewing <Icon name="arrowRight" size={16} />
            </Link>
          ) : null
        }
      />
      <DataTable
        caption="Documents waiting for review"
        columns={COLUMNS}
        rows={items ?? []}
        rowKey={(item) => item.document_id}
        loading={items === null}
        empty={
          <EmptyState
            icon="check"
            title="Nothing to review"
            action={
              <Link to="/upload" className={buttonClass("tinted")}>
                Upload documents
              </Link>
            }
          >
            Documents that need attention will appear here.
          </EmptyState>
        }
      />
    </div>
  );
}
