import { formatMoney } from "./format";
import { plural, type MonthTotal } from "./overview";
import { FIELD_LABELS } from "./review";

export interface CurrencyTotal {
  currency: string;
  total: number;
}

export interface Vendor {
  id: number;
  name: string;
  tax_id: string | null;
  default_currency: string | null;
  /** Other spellings documents have used for this vendor. */
  aliases: string[];
  records: number;
  first_invoice_date: string | null;
  last_invoice_date: string | null;
  totals: CurrencyTotal[];
}

export interface VendorDetail extends Vendor {
  /** The currency the monthly chart is in; null when the vendor has no records. */
  currency: string | null;
  months: MonthTotal[];
  memory: VendorMemory;
}

export interface Correction {
  field: string;
  before: unknown;
  after: unknown;
  reviewed_at: string;
}

/** What reviewers have taught us about a vendor: its recent reviews and the values they corrected. */
export interface VendorMemory {
  reviews: number;
  unchanged: number;
  /** Its documents may skip review when only the model doubts them. */
  trusted: boolean;
  trusted_after: number;
  corrections: Correction[];
}

export function fieldLabel(field: string): string {
  return (FIELD_LABELS as Record<string, string>)[field] ?? field;
}

export function correctionValue(value: unknown): string {
  return value === null || value === undefined || value === "" ? "empty" : String(value);
}

/** One sentence on where the vendor stands and what it means for its documents. */
export function trackRecordText(memory: VendorMemory): string {
  if (memory.reviews === 0) {
    return "No documents from this vendor have been reviewed yet.";
  }
  if (memory.trusted) {
    return `The last ${memory.trusted_after} reviews needed no changes, so documents only the model is unsure about are stored without review. Values missing from the document, odd dates, and possible duplicates still go to review.`;
  }
  return `${memory.unchanged} of ${plural(memory.reviews, "recent review")} needed no changes. Once ${memory.trusted_after} in a row need none, documents only the model is unsure about are stored without review.`;
}

export interface MergeSuggestion {
  keep: number;
  merge: number;
  keep_name: string;
  merge_name: string;
}

/** Spend in every currency, largest first: "BDT 2,44,810.89 and USD 41.61". */
export function totalsLabel(totals: CurrencyTotal[]): string {
  if (totals.length === 0) {
    return "No spend yet";
  }
  const parts = totals.map((total) => formatMoney(total.total, total.currency));
  return parts.length === 1 ? parts[0] : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;
}

/** Narrows the list by name, alias, or tax ID. */
export function filterVendors(vendors: Vendor[], text: string): Vendor[] {
  const needle = text.trim().toLowerCase();
  if (!needle) {
    return vendors;
  }
  return vendors.filter((vendor) =>
    [vendor.name, vendor.tax_id ?? "", ...vendor.aliases].some((value) => value.toLowerCase().includes(needle)),
  );
}

const DISMISSED_KEY = "ledgerly.dismissedVendorMerges";

export function suggestionKey(suggestion: MergeSuggestion): string {
  return [suggestion.keep, suggestion.merge].sort((a, b) => a - b).join(":");
}

/** Pairs this browser was told are different vendors. Saved locally, so it never blocks anything. */
export function readDismissed(): Set<string> {
  try {
    const stored = JSON.parse(window.localStorage.getItem(DISMISSED_KEY) ?? "[]");
    return new Set(Array.isArray(stored) ? stored.map(String) : []);
  } catch {
    return new Set();
  }
}

export function saveDismissed(keys: Set<string>): void {
  try {
    window.localStorage.setItem(DISMISSED_KEY, JSON.stringify([...keys]));
  } catch {
    // Private windows can refuse storage; the suggestion simply shows again next time.
  }
}
