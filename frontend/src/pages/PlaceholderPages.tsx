import { useSearchParams } from "react-router";

import { PageHeader, RebuildNotice } from "../components/PageHeader";
import { EmptyState, Stepper } from "../components/ui";
import { useSession } from "../shell/session";

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

export function ReviewPage() {
  const { reviewCount } = useSession();

  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Review"
        title="Review queue"
        subtitle="Documents that need a person to confirm or fill in details before they are stored."
      />
      {reviewCount === 0 ? (
        <EmptyState icon="check" title="Nothing to review">
          New documents that need attention will appear here.
        </EmptyState>
      ) : (
        <RebuildNotice what="side-by-side review workspace" />
      )}
    </div>
  );
}

export function UploadPage() {
  return (
    <div className="page stack">
      <PageHeader
        eyebrow="Upload"
        title="Process documents"
        subtitle="PDF, PNG, or JPEG, up to 5 MB and 5 pages each. Files go straight to private storage."
      />
      <Stepper
        steps={[
          { label: "Authorize", state: "pending" },
          { label: "Upload", state: "pending" },
          { label: "Extract", state: "pending" },
          { label: "Validate", state: "pending" },
        ]}
      />
      <RebuildNotice what="multi-file upload with live progress" />
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
