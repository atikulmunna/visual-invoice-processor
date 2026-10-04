import type { ReviewDocument, StoredRecord } from "./review";

export interface RecordRow {
  id: number;
  added_at: string;
  vendor_name: string | null;
  invoice_number: string | null;
  invoice_date: string | null;
  currency: string | null;
  total_amount: number | null;
  flagged: boolean;
  reviewed: boolean;
}

export interface RecordPage {
  items: RecordRow[];
  total: number;
  page: number;
  page_size: number;
}

export interface RecordFacets {
  currencies: { code: string; count: number }[];
  vendors: string[];
}

export interface RecordDetail {
  id: number;
  added_at: string;
  record: StoredRecord;
  flagged: boolean;
  reviewed: boolean;
  document: ReviewDocument;
}

export type SortKey = "added" | "invoice_date" | "vendor" | "total";
export type SortOrder = "asc" | "desc";

/** The records list as the URL describes it, so every view can be linked and bookmarked. */
export interface RecordQuery {
  q: string;
  currency: string | null;
  vendor: string | null;
  from: string | null;
  to: string | null;
  flagged: boolean;
  reviewed: boolean;
  sort: SortKey;
  order: SortOrder;
  page: number;
}

export const PAGE_SIZE = 25;

const SORT_KEYS: SortKey[] = ["added", "invoice_date", "vendor", "total"];
// Names read A to Z first; dates and amounts read newest or largest first.
const FIRST_ORDER: Record<SortKey, SortOrder> = { added: "desc", invoice_date: "desc", vendor: "asc", total: "desc" };
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

export const DEFAULT_QUERY: RecordQuery = {
  q: "",
  currency: null,
  vendor: null,
  from: null,
  to: null,
  flagged: false,
  reviewed: false,
  sort: "added",
  order: "desc",
  page: 1,
};

function isoDateOrNull(value: string | null): string | null {
  return value && ISO_DATE.test(value) ? value : null;
}

/** Reads the query from the address bar, dropping anything malformed instead of failing. */
export function queryFromParams(params: URLSearchParams): RecordQuery {
  const rawSort = params.get("sort") as SortKey;
  const sort = SORT_KEYS.includes(rawSort) ? rawSort : DEFAULT_QUERY.sort;
  const rawOrder = params.get("order");
  const currency = params.get("currency")?.trim().toUpperCase() ?? "";
  const page = Number.parseInt(params.get("page") ?? "", 10);
  return {
    q: params.get("q")?.trim().slice(0, 100) ?? "",
    currency: /^[A-Z]{3}$/.test(currency) ? currency : null,
    vendor: params.get("vendor")?.trim() || null,
    from: isoDateOrNull(params.get("from")),
    to: isoDateOrNull(params.get("to")),
    flagged: params.get("flagged") === "1",
    reviewed: params.get("reviewed") === "1",
    sort,
    order: rawOrder === "asc" || rawOrder === "desc" ? rawOrder : FIRST_ORDER[sort],
    page: Number.isFinite(page) && page > 0 ? page : 1,
  };
}

/** The address bar form of a query; defaults are left out to keep links short. */
export function paramsFromQuery(query: RecordQuery): URLSearchParams {
  const params = new URLSearchParams();
  if (query.q) params.set("q", query.q);
  if (query.currency) params.set("currency", query.currency);
  if (query.vendor) params.set("vendor", query.vendor);
  if (query.from) params.set("from", query.from);
  if (query.to) params.set("to", query.to);
  if (query.flagged) params.set("flagged", "1");
  if (query.reviewed) params.set("reviewed", "1");
  if (query.sort !== DEFAULT_QUERY.sort) params.set("sort", query.sort);
  if (query.order !== FIRST_ORDER[query.sort]) params.set("order", query.order);
  if (query.page > 1) params.set("page", String(query.page));
  return params;
}

/** The API request for a query; unlike the address bar, it spells out every setting. */
export function recordsApiPath(query: RecordQuery): string {
  const params = paramsFromQuery(query);
  params.set("sort", query.sort);
  params.set("order", query.order);
  params.set("page", String(query.page));
  params.set("page_size", String(PAGE_SIZE));
  return `/api/records?${params}`;
}

/** A new query with some filters changed. Any change other than the page starts again at page one. */
export function withChange(query: RecordQuery, change: Partial<RecordQuery>): RecordQuery {
  return { ...query, page: 1, ...change };
}

/** Clicking the sorted column flips its direction; another column starts in its natural direction. */
export function toggleSort(query: RecordQuery, key: SortKey): RecordQuery {
  const order = query.sort === key ? (query.order === "asc" ? "desc" : "asc") : FIRST_ORDER[key];
  return withChange(query, { sort: key, order });
}

export function hasFilters(query: RecordQuery): boolean {
  return Boolean(query.q || query.currency || query.vendor || query.from || query.to || query.flagged || query.reviewed);
}

export function clearFilters(query: RecordQuery): RecordQuery {
  return { ...DEFAULT_QUERY, sort: query.sort, order: query.order };
}

export function pageCount(total: number, pageSize: number = PAGE_SIZE): number {
  return Math.max(1, Math.ceil(total / pageSize));
}

/** Page numbers to show: the first, the last, and the current one with its neighbors. */
export function pageWindow(current: number, count: number): (number | "gap")[] {
  const pages = [...new Set([1, current - 1, current, current + 1, count])]
    .filter((page) => page >= 1 && page <= count)
    .sort((a, b) => a - b);
  const result: (number | "gap")[] = [];
  pages.forEach((page, index) => {
    if (index > 0 && page - pages[index - 1] > 1) {
      result.push("gap");
    }
    result.push(page);
  });
  return result;
}
