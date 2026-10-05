import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router";

import { DataTable, type Column } from "../components/DataTable";
import { Dialog } from "../components/Dialog";
import { Icon } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { useToast } from "../components/Toast";
import { Button, buttonClass, Card, EmptyState } from "../components/ui";
import { api } from "../lib/api";
import { formatDate } from "../lib/format";
import { plural } from "../lib/overview";
import {
  filterVendors,
  readDismissed,
  saveDismissed,
  suggestionKey,
  totalsLabel,
  type MergeSuggestion,
  type Vendor,
} from "../lib/vendors";

const COLUMNS: Column<Vendor>[] = [
  {
    key: "vendor",
    header: "Vendor",
    render: (vendor) => (
      <div className="batch-name">
        <Link to={`/vendors/${vendor.id}`}>
          <strong>{vendor.name}</strong>
        </Link>
        {vendor.aliases.length > 0 && <span className="muted">Also printed as {vendor.aliases.join(", ")}</span>}
      </div>
    ),
  },
  { key: "tax", header: "Tax ID or BIN", render: (vendor) => vendor.tax_id ?? <span className="muted">Not shown</span> },
  { key: "records", header: "Records", numeric: true, render: (vendor) => vendor.records },
  { key: "spend", header: "Spend", render: (vendor) => totalsLabel(vendor.totals) },
  { key: "last", header: "Last invoice", render: (vendor) => formatDate(vendor.last_invoice_date) ?? "" },
];

function Suggestions({
  suggestions,
  vendors,
  onMerge,
  onDismiss,
}: {
  suggestions: MergeSuggestion[];
  vendors: Map<number, Vendor>;
  onMerge: (suggestion: MergeSuggestion) => void;
  onDismiss: (suggestion: MergeSuggestion) => void;
}) {
  const count = (id: number) => plural(vendors.get(id)?.records ?? 0, "record");
  return (
    <Card>
      <h2 className="card-title">Possibly the same vendor</h2>
      <p className="muted chart-subtitle">
        These names look alike. Merging keeps one vendor and moves every record and spelling onto it.
      </p>
      <ul className="merge-list">
        {suggestions.map((suggestion) => (
          <li key={suggestionKey(suggestion)} className="merge-row">
            <span>
              <strong>{suggestion.merge_name}</strong> <span className="muted">({count(suggestion.merge)})</span> and{" "}
              <strong>{suggestion.keep_name}</strong> <span className="muted">({count(suggestion.keep)})</span>
            </span>
            <span className="row" style={{ gap: 8 }}>
              <Button size="sm" variant="tinted" onClick={() => onMerge(suggestion)}>
                Merge into {suggestion.keep_name}
              </Button>
              <Button size="sm" variant="ghost" onClick={() => onDismiss(suggestion)}>
                Not the same
              </Button>
            </span>
          </li>
        ))}
      </ul>
    </Card>
  );
}

export function VendorsPage() {
  const toast = useToast();
  const [data, setData] = useState<{ vendors: Vendor[]; suggestions: MergeSuggestion[] } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [dismissed, setDismissed] = useState(readDismissed);
  const [pending, setPending] = useState<MergeSuggestion | null>(null);
  const [merging, setMerging] = useState(false);

  const load = useCallback(() => {
    api.vendors().then(
      (response) => {
        setData(response);
        setError(null);
      },
      (failure: unknown) => setError(failure instanceof Error ? failure.message : "Unknown error"),
    );
  }, []);

  useEffect(load, [load]);

  const byId = useMemo(() => new Map((data?.vendors ?? []).map((vendor) => [vendor.id, vendor])), [data]);
  const suggestions = (data?.suggestions ?? []).filter((suggestion) => !dismissed.has(suggestionKey(suggestion)));
  const rows = filterVendors(data?.vendors ?? [], search);

  function dismiss(suggestion: MergeSuggestion) {
    const next = new Set(dismissed).add(suggestionKey(suggestion));
    setDismissed(next);
    saveDismissed(next);
  }

  async function confirmMerge() {
    if (!pending) {
      return;
    }
    setMerging(true);
    try {
      await api.mergeVendors(pending.keep, [pending.merge]);
      toast({ tone: "success", title: `Merged into ${pending.keep_name}` });
      setPending(null);
      load();
    } catch (failure) {
      toast({ tone: "danger", title: "Not merged", description: failure instanceof Error ? failure.message : undefined });
    } finally {
      setMerging(false);
    }
  }

  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Ledger"
        title="Vendors"
        subtitle={
          data ? `${plural(data.vendors.length, "vendor")}, each with every spelling its documents used.` : "Everyone you buy from."
        }
      />
      {error && !data && (
        <EmptyState icon="alert" title="Vendors could not load" action={<Button onClick={load}>Try again</Button>}>
          {error}
        </EmptyState>
      )}
      {suggestions.length > 0 && (
        <Suggestions suggestions={suggestions} vendors={byId} onMerge={setPending} onDismiss={dismiss} />
      )}
      {!(error && !data) && (
        <>
          <div className="filter-bar vendor-filter">
            <form className="filter-search" role="search" aria-label="Filter vendors" onSubmit={(event) => event.preventDefault()}>
              <Icon name="search" size={16} />
              <label className="visually-hidden" htmlFor="vendor-search">
                Filter vendors
              </label>
              <input
                id="vendor-search"
                type="search"
                value={search}
                placeholder="Name, earlier spelling, or tax ID"
                onChange={(event) => setSearch(event.target.value)}
              />
            </form>
          </div>
          <DataTable
            caption="Vendors"
            columns={COLUMNS}
            rows={rows}
            rowKey={(vendor) => String(vendor.id)}
            loading={data === null}
            empty={
              search ? (
                <EmptyState icon="search" title="No vendor matches">
                  Try another name or tax ID.
                </EmptyState>
              ) : (
                <EmptyState
                  icon="vendors"
                  title="No vendors yet"
                  action={<Link to="/upload" className={buttonClass("tinted")}>Upload documents</Link>}
                >
                  Vendors appear here as their invoices are stored.
                </EmptyState>
              )
            }
          />
        </>
      )}
      <Dialog open={pending !== null} onClose={() => setPending(null)} title="Merge these vendors?">
        {pending && (
          <div className="stack" style={{ gap: 16 }}>
            <p>
              {pending.merge_name} will be merged into <strong>{pending.keep_name}</strong>. Its records and spellings
              move over, and documents printed as {pending.merge_name} will link to {pending.keep_name} from now on.
            </p>
            <div className="row" style={{ justifyContent: "flex-end" }}>
              <Button variant="ghost" onClick={() => setPending(null)}>
                Cancel
              </Button>
              <Button onClick={() => void confirmMerge()} disabled={merging}>
                {merging ? "Merging" : `Merge into ${pending.keep_name}`}
              </Button>
            </div>
          </div>
        )}
      </Dialog>
    </div>
  );
}
