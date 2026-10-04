import { useSearchParams } from "react-router";

import { PageHeader, RebuildNotice } from "../components/PageHeader";
import { EmptyState } from "../components/ui";

export function RecordsPage() {
  const [params] = useSearchParams();
  const query = params.get("q");

  return (
    <div className="page stack">
      <PageHeader eyebrow="Ledger" title="Records" subtitle="Every invoice and receipt stored for your organization." />
      {query && (
        <p className="muted">
          Search for <strong>{query}</strong> will run here once the new records screen is ready.
        </p>
      )}
      <RebuildNotice what="records list with filters, search, and export" />
    </div>
  );
}

export function NotFoundPage() {
  return (
    <div className="page">
      <EmptyState icon="alert" title="Page not found" headingLevel="h1">
        The address may be mistyped, or the page has moved.
      </EmptyState>
    </div>
  );
}
