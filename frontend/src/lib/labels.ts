export type Tone = "success" | "warning" | "danger" | "neutral" | "info";

const STATUSES: Record<string, { label: string; tone: Tone }> = {
  AUTHORIZED: { label: "Uploading", tone: "info" },
  PROCESSING: { label: "Processing", tone: "info" },
  STORED: { label: "Stored", tone: "success" },
  REVIEW_REQUIRED: { label: "Needs review", tone: "warning" },
  DUPLICATE: { label: "Duplicate", tone: "neutral" },
  REJECTED: { label: "Rejected", tone: "danger" },
  FAILED: { label: "Failed", tone: "danger" },
  RESOLVED_STORED: { label: "Approved", tone: "success" },
  RESOLVED_DUPLICATE: { label: "Approved, already stored", tone: "neutral" },
  RESOLVED_DUPLICATE_MANUAL: { label: "Marked duplicate", tone: "neutral" },
};

const REVIEW_REASONS: Record<string, string> = {
  missing_vendor: "Vendor name not found on the document",
  missing_invoice_date: "Invoice date not found on the document",
  missing_currency: "Currency not found on the document",
  low_confidence: "Low extraction confidence",
  validation_failed: "Amounts do not add up",
  schema_validation_failed: "Some fields could not be read",
};

function humanize(code: string): string {
  const words = code.toLowerCase().replace(/_/g, " ").trim();
  return words ? words[0].toUpperCase() + words.slice(1) : "Unknown";
}

export function statusLabel(status: string): { label: string; tone: Tone } {
  return STATUSES[status] ?? { label: humanize(status), tone: "neutral" };
}

export function reasonLabel(code: string): string {
  return REVIEW_REASONS[code] ?? humanize(code);
}

export function currencyNote(currencyAssumed: boolean | undefined, currency: string | null | undefined): string | null {
  return currencyAssumed && currency ? `Not shown on the document; assumed ${currency}, your base currency` : null;
}
