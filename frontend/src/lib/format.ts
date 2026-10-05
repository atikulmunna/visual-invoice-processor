// Currencies written with South Asian digit grouping (2,16,500 rather than 216,500).
const LAKH_GROUPED = new Set(["BDT", "INR"]);

const numberFormats = new Map<string, Intl.NumberFormat>();

function amountFormat(locale: string): Intl.NumberFormat {
  let format = numberFormats.get(locale);
  if (!format) {
    format = new Intl.NumberFormat(locale, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    numberFormats.set(locale, format);
  }
  return format;
}

/** The digits of an amount, grouped the way its currency is usually written, with two decimals. */
export function formatAmount(amount: number, currency?: string | null): string {
  const code = currency?.trim().toUpperCase();
  return amountFormat(code && LAKH_GROUPED.has(code) ? "en-IN" : "en-US").format(amount);
}

/** Accounting style: always two decimals, prefixed with the ISO code when known. */
export function formatMoney(amount: number, currency?: string | null): string {
  const code = currency?.trim().toUpperCase() || null;
  const digits = formatAmount(amount, code);
  return code ? `${code} ${digits}` : digits;
}

const dateFormat = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});

/** Unambiguous day-month-year ("4 Mar 2026") for ISO dates; null when missing or invalid. */
export function formatDate(isoDate?: string | null): string | null {
  if (!isoDate) {
    return null;
  }
  const parsed = new Date(isoDate.length === 10 ? `${isoDate}T00:00:00Z` : isoDate);
  return Number.isNaN(parsed.getTime()) ? null : dateFormat.format(parsed);
}

export function initials(name: string): string {
  const parts = name.split(/[\s._-]+/).filter(Boolean);
  const letters = parts.length > 1 ? parts[0][0] + parts[1][0] : name.slice(0, 2);
  return letters.toUpperCase();
}

const relative = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** "just now", "3 minutes ago", "yesterday"; older than a week falls back to the date. */
export function formatRelativeTime(iso: string | null | undefined, now: number = Date.now()): string | null {
  if (!iso) {
    return null;
  }
  const elapsed = now - new Date(iso).getTime();
  if (Number.isNaN(elapsed)) {
    return null;
  }
  if (elapsed < 45_000) {
    return "just now";
  }
  if (elapsed < HOUR) {
    return relative.format(-Math.round(elapsed / MINUTE), "minute");
  }
  if (elapsed < DAY) {
    return relative.format(-Math.round(elapsed / HOUR), "hour");
  }
  if (elapsed < 7 * DAY) {
    return relative.format(-Math.round(elapsed / DAY), "day");
  }
  return formatDate(iso);
}

/** Offered as suggestions wherever a currency code is typed. */
export const COMMON_CURRENCIES = ["BDT", "USD", "EUR", "GBP", "INR", "AED", "SAR", "SGD", "MYR", "CNY", "JPY", "CAD", "AUD"];
