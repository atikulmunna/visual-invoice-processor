export interface MonthTotal {
  /** Calendar month of the invoice date, as YYYY-MM. */
  month: string;
  total: number;
  count: number;
}

export interface VendorTotal {
  vendor_name: string;
  total: number;
  count: number;
}

export type Spotlight =
  | { kind: "files_expiring"; count: number; days_left: number }
  | { kind: "uploads_failed"; count: number }
  | { kind: "review_waiting"; count: number; oldest_days: number }
  | { kind: "records_flagged"; count: number };

export interface Overview {
  /** Every total below is in this currency; null when nothing is stored yet. */
  currency: string | null;
  currencies: string[];
  records_total: number;
  flagged_total: number;
  months: MonthTotal[];
  top_vendors: VendorTotal[];
  spotlight: Spotlight | null;
}

export function plural(count: number, noun: string): string {
  return `${count.toLocaleString("en-US")} ${noun}${count === 1 ? "" : "s"}`;
}

const shortMonth = new Intl.DateTimeFormat("en-GB", { month: "short", timeZone: "UTC" });
const longMonth = new Intl.DateTimeFormat("en-GB", { month: "long", year: "numeric", timeZone: "UTC" });

/** "Oct" or, in long form, "October 2026". */
export function monthLabel(month: string, long = false): string {
  const [year, index] = month.split("-").map(Number);
  return (long ? longMonth : shortMonth).format(new Date(Date.UTC(year, index - 1, 1)));
}

export function spendSummary(months: MonthTotal[]) {
  return {
    thisMonth: months.at(-1) ?? null,
    lastMonth: months.at(-2) ?? null,
    yearTotal: months.reduce((sum, month) => sum + month.total, 0),
    yearCount: months.reduce((sum, month) => sum + month.count, 0),
  };
}

/** Round axis values from zero to at least `max`, about `target` steps apart (0, 2,500, 5,000...). */
export function axisTicks(max: number, target = 4): number[] {
  if (max <= 0) {
    return [0];
  }
  const rough = max / target;
  const magnitude = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map((factor) => factor * magnitude).find((candidate) => candidate >= rough) ?? rough;
  const steps = Math.ceil(max / step - 1e-9);
  return Array.from({ length: steps + 1 }, (_, index) => Number((index * step).toPrecision(12)));
}

const compactFormats = new Map<string, Intl.NumberFormat>();

/** Short axis labels: "250K" in most currencies, "2.5L" where amounts are grouped in lakhs. */
export function compactAmount(value: number, currency: string | null): string {
  const locale = currency === "BDT" || currency === "INR" ? "en-IN" : "en-US";
  let format = compactFormats.get(locale);
  if (!format) {
    format = new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 });
    compactFormats.set(locale, format);
  }
  return format.format(value);
}

export interface SpotlightCopy {
  urgent: boolean;
  eyebrow: string;
  title: string;
  body: string;
  action: { label: string; to: string };
}

export function spotlightCopy(spotlight: Spotlight | null): SpotlightCopy {
  if (!spotlight) {
    return {
      urgent: false,
      eyebrow: "All clear",
      title: "Nothing needs your attention",
      body: "Every document has been read and stored. New uploads show up here if anything needs a look.",
      action: { label: "Upload documents", to: "/upload" },
    };
  }
  switch (spotlight.kind) {
    case "files_expiring":
      return {
        urgent: true,
        eyebrow: "Act soon",
        title:
          spotlight.days_left > 0
            ? `${plural(spotlight.count, "document")} in review will lose the original file in ${plural(spotlight.days_left, "day")}`
            : `${plural(spotlight.count, "document")} in review no longer ${spotlight.count === 1 ? "has its" : "have their"} original file`,
        body: "Source files are deleted 30 days after upload. Review them while you can still check the details against the document.",
        action: { label: "Open the review queue", to: "/review" },
      };
    case "uploads_failed":
      return {
        urgent: true,
        eyebrow: "Needs a retry",
        title: `${plural(spotlight.count, "upload")} could not be read`,
        body: "Something went wrong while reading them. Retry from the upload screen today, while the files are still kept.",
        action: { label: "Go to uploads", to: "/upload" },
      };
    case "review_waiting":
      return {
        urgent: false,
        eyebrow: "Waiting on you",
        title: `${plural(spotlight.count, "document")} ${spotlight.count === 1 ? "needs" : "need"} review`,
        body:
          spotlight.oldest_days > 0
            ? `The oldest has waited ${plural(spotlight.oldest_days, "day")}. Nothing is stored until a person confirms it.`
            : "They arrived today. Nothing is stored until a person confirms it.",
        action: { label: "Start reviewing", to: "/review" },
      };
    case "records_flagged":
      return {
        urgent: false,
        eyebrow: "Worth a second look",
        title: `${plural(spotlight.count, "stored record")} ${spotlight.count === 1 ? "was" : "were"} flagged`,
        body: "They passed the required checks and were stored, but some details looked doubtful.",
        action: { label: "See flagged records", to: "/records?flagged=1" },
      };
  }
}
