import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { Link, useSearchParams } from "react-router";

import { DataTable, type Column } from "../components/DataTable";
import { ExportMenu } from "../components/ExportMenu";
import { Icon } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { Pagination } from "../components/Pagination";
import { RecordDrawer } from "../components/RecordDrawer";
import { Badge, Button, buttonClass, Chip, EmptyState } from "../components/ui";
import { api } from "../lib/api";
import { formatDate, formatMoney, formatRelativeTime } from "../lib/format";
import { plural } from "../lib/overview";
import {
  clearFilters,
  hasFilters,
  pageCount,
  paramsFromQuery,
  queryFromParams,
  recordsApiPath,
  toggleSort,
  withChange,
  type RecordFacets,
  type RecordPage,
  type RecordQuery,
  type RecordRow,
  type SortKey,
} from "../lib/records";

type QueryChange = (current: RecordQuery) => RecordQuery;

function ChecksCell({ row }: { row: RecordRow }) {
  if (!row.flagged && !row.reviewed) {
    return <Badge tone="success">Passed</Badge>;
  }
  return (
    <div className="row" style={{ gap: 6 }}>
      {row.flagged && <Badge tone="warning">Flagged</Badge>}
      {row.reviewed && <Badge tone="info">Approved by reviewer</Badge>}
    </div>
  );
}

function columns(recordLink: (id: number) => string): Column<RecordRow>[] {
  return [
    {
      key: "vendor",
      header: "Vendor",
      sortKey: "vendor",
      render: (row) => (
        <div className="batch-name">
          <Link to={recordLink(row.id)}>
            <strong>{row.vendor_name ?? "Vendor not shown"}</strong>
          </Link>
          <span className="muted">{row.invoice_number ? `Invoice ${row.invoice_number}` : "No invoice number"}</span>
        </div>
      ),
    },
    { key: "date", header: "Invoice date", sortKey: "invoice_date", render: (row) => formatDate(row.invoice_date) ?? "" },
    { key: "checks", header: "Checks", render: (row) => <ChecksCell row={row} /> },
    {
      key: "total",
      header: "Total",
      sortKey: "total",
      numeric: true,
      render: (row) => (row.total_amount != null ? formatMoney(row.total_amount, row.currency) : ""),
    },
    { key: "added", header: "Added", sortKey: "added", render: (row) => formatRelativeTime(row.added_at) ?? "" },
  ];
}

function SearchBox({ value, onSearch }: { value: string; onSearch: (text: string) => void }) {
  const [draft, setDraft] = useState(value);

  useEffect(() => setDraft(value), [value]);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onSearch(draft.trim());
  }

  return (
    <form className="filter-search" role="search" aria-label="Filter records" onSubmit={submit}>
      <Icon name="search" size={16} />
      <label className="visually-hidden" htmlFor="records-search">
        Search records
      </label>
      <input
        id="records-search"
        type="search"
        value={draft}
        maxLength={100}
        placeholder="Vendor, invoice number, or tax ID"
        onChange={(event) => {
          setDraft(event.target.value);
          if (!event.target.value && value) onSearch("");
        }}
      />
    </form>
  );
}

function FilterBar({
  query,
  facets,
  onChange,
}: {
  query: RecordQuery;
  facets: RecordFacets | null;
  onChange: (change: QueryChange) => void;
}) {
  const change = (patch: (current: RecordQuery) => Partial<RecordQuery>) =>
    onChange((current) => withChange(current, patch(current)));
  const currencies = facets?.currencies ?? [];

  return (
    <div className="filter-bar">
      <div className="filter-row">
        <SearchBox value={query.q} onSearch={(q) => change(() => ({ q }))} />
        <label className="filter-field">
          <span>Vendor</span>
          <select value={query.vendor ?? ""} onChange={(event) => change(() => ({ vendor: event.target.value || null }))}>
            <option value="">All vendors</option>
            {query.vendor && !facets?.vendors.includes(query.vendor) && <option value={query.vendor}>{query.vendor}</option>}
            {facets?.vendors.map((vendor) => (
              <option key={vendor} value={vendor}>
                {vendor}
              </option>
            ))}
          </select>
        </label>
        <label className="filter-field">
          <span>From</span>
          <input type="date" value={query.from ?? ""} max={query.to ?? undefined}
            onChange={(event) => change(() => ({ from: event.target.value || null }))} />
        </label>
        <label className="filter-field">
          <span>To</span>
          <input type="date" value={query.to ?? ""} min={query.from ?? undefined}
            onChange={(event) => change(() => ({ to: event.target.value || null }))} />
        </label>
      </div>
      <div className="filter-row" role="group" aria-label="Quick filters">
        {currencies.length > 1 &&
          currencies.map(({ code }) => (
            <Chip key={code} pressed={query.currency === code}
              onToggle={() => change((current) => ({ currency: current.currency === code ? null : code }))}>
              {code}
            </Chip>
          ))}
        <Chip pressed={query.flagged} onToggle={() => change((current) => ({ flagged: !current.flagged }))}>
          Flagged
        </Chip>
        <Chip pressed={query.reviewed} onToggle={() => change((current) => ({ reviewed: !current.reviewed }))}>
          Approved by reviewer
        </Chip>
        {hasFilters(query) && (
          <Button variant="ghost" size="sm" icon="close" className="filter-clear" onClick={() => onChange(clearFilters)}>
            Clear filters
          </Button>
        )}
      </div>
    </div>
  );
}

export function RecordsPage() {
  const [params, setParams] = useSearchParams();
  const query = useMemo(() => queryFromParams(params), [params]);
  const requestPath = recordsApiPath(query);
  const recordParam = Number.parseInt(params.get("record") ?? "", 10);
  const openRecord = recordParam > 0 ? recordParam : null;
  const [facets, setFacets] = useState<RecordFacets | null>(null);
  const [result, setResult] = useState<{ path: string; page: RecordPage } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const tableTop = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.recordFacets().then(setFacets, () => setFacets({ currencies: [], vendors: [] }));
  }, []);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    // requestPath changes whenever the query does, so the query read here is current.
    api.records(query).then(
      (page) => !cancelled && setResult({ path: requestPath, page }),
      (failure: unknown) => !cancelled && setError(failure instanceof Error ? failure.message : "Unknown error"),
    );
    return () => {
      cancelled = true;
    };
  }, [requestPath, attempt]);

  // Each change starts from the address bar rather than the last render, so two quick
  // clicks both count even if the first has not re-rendered yet.
  function update(change: QueryChange) {
    setParams(paramsFromQuery(change(queryFromParams(new URLSearchParams(window.location.search)))));
  }

  function changePage(page: number) {
    update((current) => withChange(current, { page }));
    tableTop.current?.scrollIntoView({ block: "start" });
  }

  function recordLink(id: number): string {
    const next = new URLSearchParams(params);
    next.set("record", String(id));
    return `?${next}`;
  }

  function closeRecord() {
    const next = new URLSearchParams(params);
    next.delete("record");
    setParams(next, { replace: true });
  }

  const page = result?.page ?? null;
  const filtered = hasFilters(query);
  const subtitle = page
    ? `${plural(page.total, "record")} ${filtered ? (page.total === 1 ? "matches" : "match") + " these filters" : "stored for your organization"}.`
    : "Every invoice and receipt stored for your organization.";

  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Ledger"
        title="Records"
        subtitle={subtitle}
        actions={
          <>
            <ExportMenu query={query} total={page?.total ?? null} />
            <Link to="/upload" className={buttonClass("primary")}>
              <Icon name="upload" size={16} /> Upload documents
            </Link>
          </>
        }
      />
      <FilterBar query={query} facets={facets} onChange={update} />
      <div ref={tableTop} className="scroll-anchor" />
      {error && page && (
        <div className="callout tone-danger row" role="alert">
          <Icon name="alert" size={18} />
          <span>The list could not be updated: {error}</span>
          <Button size="sm" variant="ghost" onClick={() => setAttempt((n) => n + 1)}>
            Try again
          </Button>
        </div>
      )}
      {error && !page ? (
        <EmptyState icon="alert" title="Records could not load" action={<Button onClick={() => setAttempt((n) => n + 1)}>Try again</Button>}>
          {error}
        </EmptyState>
      ) : (
        <DataTable
          caption="Stored records"
          columns={columns(recordLink)}
          rows={page?.items ?? []}
          rowKey={(row) => String(row.id)}
          loading={page === null}
          refreshing={result !== null && result.path !== requestPath}
          sort={{ key: query.sort, order: query.order }}
          onSort={(key) => update((current) => toggleSort(current, key as SortKey))}
          empty={
            filtered ? (
              <EmptyState icon="search" title="No records match"
                action={<Button variant="tinted" onClick={() => update(clearFilters)}>Clear filters</Button>}>
                Try a different search or fewer filters.
              </EmptyState>
            ) : (
              <EmptyState icon="records" title="No records yet"
                action={<Link to="/upload" className={buttonClass("tinted")}>Upload documents</Link>}>
                Invoices and receipts appear here once they are read and stored.
              </EmptyState>
            )
          }
        />
      )}
      {page && <Pagination page={query.page} count={pageCount(page.total, page.page_size)} onChange={changePage} />}
      <RecordDrawer recordId={openRecord} onClose={closeRecord} />
    </div>
  );
}
