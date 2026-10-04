import { useState, type ReactNode } from "react";

import { DataTable, type Column } from "../components/DataTable";
import { Dialog } from "../components/Dialog";
import { PageHeader } from "../components/PageHeader";
import { useToast } from "../components/Toast";
import {
  Badge,
  Button,
  Card,
  Chip,
  EmptyState,
  Skeleton,
  Stat,
  StatusBadge,
  Stepper,
  TextField,
} from "../components/ui";
import { formatDate, formatMoney } from "../lib/format";
import { currencyNote, reasonLabel } from "../lib/labels";

interface SampleRecord {
  id: string;
  vendor: string | null;
  date: string | null;
  amount: number;
  currency: string;
  currencyAssumed: boolean;
  status: string;
}

const SAMPLE_RECORDS: SampleRecord[] = [
  { id: "1", vendor: "Tech Land BD", date: "2026-03-02", amount: 216500, currency: "BDT", currencyAssumed: false, status: "STORED" },
  { id: "2", vendor: "RYANS", date: "2022-05-17", amount: 1954, currency: "BDT", currencyAssumed: true, status: "STORED" },
  { id: "3", vendor: null, date: null, amount: 5274.88, currency: "BDT", currencyAssumed: false, status: "REVIEW_REQUIRED" },
  { id: "4", vendor: "Roboflow, Inc", date: "2026-02-13", amount: 12, currency: "USD", currencyAssumed: false, status: "FAILED" },
];

const COLUMNS: Column<SampleRecord>[] = [
  { key: "vendor", header: "Vendor", render: (row) => row.vendor ?? <span className="muted">Missing</span> },
  { key: "date", header: "Date", render: (row) => formatDate(row.date) ?? <span className="muted">Missing</span> },
  { key: "status", header: "Status", render: (row) => <StatusBadge status={row.status} /> },
  {
    key: "amount",
    header: "Total",
    numeric: true,
    render: (row) => (
      <span title={currencyNote(row.currencyAssumed, row.currency) ?? undefined}>
        {formatMoney(row.amount, row.currency)}
        {row.currencyAssumed && <span className="muted"> (assumed)</span>}
      </span>
    ),
  },
];

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="stack" style={{ gap: 14 }}>
      <h2 className="section-title">{title}</h2>
      {children}
    </section>
  );
}

export function KitPage() {
  const toast = useToast();
  const [filter, setFilter] = useState("all");
  const [modalOpen, setModalOpen] = useState(false);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [loadingTable, setLoadingTable] = useState(false);

  return (
    <div className="page stack" style={{ gap: 44 }}>
      <PageHeader
        eyebrow="Design system"
        title="Component kit"
        subtitle="Every building block of the new workspace in one place, for review."
      />

      <Section title="Buttons">
        <div className="row">
          <Button>Primary</Button>
          <Button variant="tinted">Tinted</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="danger">Reject</Button>
          <Button icon="upload" size="sm">
            Small with icon
          </Button>
          <Button size="lg">Large</Button>
          <Button disabled>Disabled</Button>
        </div>
        <Card dark>
          <div className="row">
            <Button variant="light">Light on dark</Button>
            <Button variant="glass">Glass on dark</Button>
          </div>
        </Card>
      </Section>

      <Section title="Status and filters">
        <div className="row">
          {["STORED", "REVIEW_REQUIRED", "PROCESSING", "DUPLICATE", "FAILED", "RESOLVED_STORED"].map((status) => (
            <StatusBadge key={status} status={status} />
          ))}
          <Badge tone="info">Currency assumed</Badge>
        </div>
        <div className="row" role="group" aria-label="Filter records">
          {["all", "stored", "needs review", "failed"].map((option) => (
            <Chip key={option} pressed={filter === option} onToggle={() => setFilter(option)}>
              {option[0].toUpperCase() + option.slice(1)}
            </Chip>
          ))}
        </div>
      </Section>

      <Section title="Stats">
        <div className="grid-cards">
          <Stat label="Stored this month" value="128" hint="12 more than last month" />
          <Stat label="Spend" value={formatMoney(216500, "BDT")} hint="Lakh grouping for taka" />
          <Stat label="Needs review" value="3" />
        </div>
      </Section>

      <Section title="Table">
        <div className="row">
          <Button variant="tinted" size="sm" onClick={() => setLoadingTable((value) => !value)}>
            {loadingTable ? "Show rows" : "Show loading state"}
          </Button>
        </div>
        <DataTable
          caption="Sample records"
          columns={COLUMNS}
          rows={SAMPLE_RECORDS}
          rowKey={(row) => row.id}
          loading={loadingTable}
        />
        <DataTable
          caption="Empty sample"
          columns={COLUMNS}
          rows={[]}
          rowKey={(row) => row.id}
          empty={<EmptyState title="No records yet">Upload a document and it will appear here.</EmptyState>}
        />
      </Section>

      <Section title="Progress">
        <Stepper
          steps={[
            { label: "Authorize", state: "done" },
            { label: "Upload", state: "done" },
            { label: "Extract", state: "active" },
            { label: "Validate", state: "pending" },
          ]}
        />
        <Stepper
          steps={[
            { label: "Authorize", state: "done" },
            { label: "Upload", state: "error" },
            { label: "Extract", state: "pending" },
            { label: "Validate", state: "pending" },
          ]}
        />
      </Section>

      <Section title="Fields and review reasons">
        <div className="grid-cards">
          <TextField label="Vendor name" placeholder="As printed on the invoice" />
          <TextField label="Invoice date" type="date" error="Invoice date not found on the document" />
        </div>
        <ul className="stack" style={{ gap: 6, paddingLeft: 18, margin: 0 }}>
          {["missing_vendor", "missing_invoice_date", "low_confidence", "validation_failed"].map((code) => (
            <li key={code}>{reasonLabel(code)}</li>
          ))}
        </ul>
      </Section>

      <Section title="Loading and empty">
        <Card>
          <div className="stack" style={{ gap: 10 }}>
            <Skeleton width="40%" height={18} />
            <Skeleton />
            <Skeleton width="85%" />
          </div>
        </Card>
        <Card>
          <EmptyState icon="check" title="Nothing to review" action={<Button variant="tinted">Upload documents</Button>}>
            New documents that need attention will appear here.
          </EmptyState>
        </Card>
      </Section>

      <Section title="Overlays">
        <div className="row">
          <Button variant="tinted" onClick={() => setModalOpen(true)}>
            Open modal
          </Button>
          <Button variant="tinted" onClick={() => setDrawerOpen(true)}>
            Open drawer
          </Button>
          <Button
            variant="tinted"
            onClick={() => toast({ tone: "success", title: "Record approved", description: "It now appears in Records." })}
          >
            Show toast
          </Button>
        </div>
      </Section>

      <Dialog open={modalOpen} onClose={() => setModalOpen(false)} title="Reject this document?">
        <p className="muted">It will leave the review queue and will not be stored.</p>
        <div className="row" style={{ marginTop: 20, justifyContent: "flex-end" }}>
          <Button variant="ghost" onClick={() => setModalOpen(false)}>
            Cancel
          </Button>
          <Button variant="danger" onClick={() => setModalOpen(false)}>
            Reject
          </Button>
        </div>
      </Dialog>
      <Dialog open={drawerOpen} onClose={() => setDrawerOpen(false)} title="Record details" variant="drawer">
        <dl className="stack" style={{ gap: 14 }}>
          <div>
            <dt className="stat-label">Vendor</dt>
            <dd style={{ margin: "4px 0 0" }}>Tech Land BD</dd>
          </div>
          <div>
            <dt className="stat-label">Total</dt>
            <dd style={{ margin: "4px 0 0" }}>{formatMoney(216500, "BDT")}</dd>
          </div>
        </dl>
      </Dialog>
    </div>
  );
}
