import { EmptyState } from "../components/ui";

export function NotFoundPage() {
  return (
    <div className="page">
      <EmptyState icon="alert" title="Page not found" headingLevel="h1">
        The address may be mistyped, or the page has moved.
      </EmptyState>
    </div>
  );
}
