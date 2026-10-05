import { describe, expect, it } from "vitest";

import {
  clearFilters,
  DEFAULT_QUERY,
  hasFilters,
  pageCount,
  pageWindow,
  paramsFromQuery,
  queryFromParams,
  recordsApiPath,
  recordsExportPath,
  toggleSort,
  withChange,
} from "./records";

describe("queryFromParams", () => {
  it("reads every filter from the address bar", () => {
    const query = queryFromParams(
      new URLSearchParams("q=acme&currency=usd&vendor=Acme+Ltd&from=2026-09-01&to=2026-09-30&flagged=1&reviewed=1&sort=total&order=asc&page=3"),
    );

    expect(query).toEqual({
      q: "acme",
      currency: "USD",
      vendor: "Acme Ltd",
      from: "2026-09-01",
      to: "2026-09-30",
      flagged: true,
      reviewed: true,
      sort: "total",
      order: "asc",
      page: 3,
    });
  });

  it("falls back to defaults for anything malformed", () => {
    const query = queryFromParams(
      new URLSearchParams("currency=dollars&from=yesterday&to=2026-9-1&sort=drop&order=up&page=-2&flagged=yes"),
    );

    expect(query).toEqual(DEFAULT_QUERY);
  });

  it("gives a sort without a direction its natural order", () => {
    expect(queryFromParams(new URLSearchParams("sort=vendor")).order).toBe("asc");
    expect(queryFromParams(new URLSearchParams("sort=total")).order).toBe("desc");
  });

  it("caps the search text", () => {
    expect(queryFromParams(new URLSearchParams(`q=${"x".repeat(150)}`)).q).toHaveLength(100);
  });
});

describe("paramsFromQuery", () => {
  it("leaves defaults out so links stay short", () => {
    expect(paramsFromQuery(DEFAULT_QUERY).toString()).toBe("");
    expect(paramsFromQuery({ ...DEFAULT_QUERY, sort: "vendor", order: "asc" }).toString()).toBe("sort=vendor");
    expect(paramsFromQuery({ ...DEFAULT_QUERY, order: "asc" }).toString()).toBe("order=asc");
  });

  it("round-trips through the address bar", () => {
    const query = { ...DEFAULT_QUERY, q: "d_1", currency: "BDT", flagged: true, sort: "vendor" as const, order: "desc" as const, page: 2 };

    expect(queryFromParams(paramsFromQuery(query))).toEqual(query);
  });
});

describe("recordsApiPath", () => {
  it("spells out sort, page, and page size for the API", () => {
    expect(recordsApiPath({ ...DEFAULT_QUERY, q: "a&b", flagged: true })).toBe(
      "/api/records?q=a%26b&flagged=1&sort=added&order=desc&page=1&page_size=25",
    );
  });
});

describe("recordsExportPath", () => {
  it("exports the whole filtered list in its order, never one page", () => {
    const query = { ...DEFAULT_QUERY, vendor: "Star Tech", flagged: true, sort: "total" as const, order: "asc" as const, page: 3 };

    expect(recordsExportPath(query, "xlsx")).toBe(
      "/api/records/export?vendor=Star+Tech&flagged=1&sort=total&order=asc&format=xlsx",
    );
    expect(recordsExportPath(DEFAULT_QUERY, "line-items-csv")).toBe(
      "/api/records/export?sort=added&order=desc&format=line-items-csv",
    );
  });
});

describe("changing the query", () => {
  it("starts again at page one when a filter changes", () => {
    const onPageThree = { ...DEFAULT_QUERY, page: 3 };

    expect(withChange(onPageThree, { currency: "USD" }).page).toBe(1);
    expect(withChange(onPageThree, { page: 4 }).page).toBe(4);
  });

  it("flips the direction of the sorted column and starts others in their natural order", () => {
    const byTotal = toggleSort(DEFAULT_QUERY, "total");
    expect([byTotal.sort, byTotal.order]).toEqual(["total", "desc"]);
    expect(toggleSort(byTotal, "total").order).toBe("asc");
    expect(toggleSort(byTotal, "vendor").order).toBe("asc");
  });

  it("knows when filters are active and clears them while keeping the sort", () => {
    const filtered = { ...DEFAULT_QUERY, vendor: "Acme", sort: "total" as const, page: 2 };

    expect(hasFilters(DEFAULT_QUERY)).toBe(false);
    expect(hasFilters(filtered)).toBe(true);
    expect(clearFilters(filtered)).toEqual({ ...DEFAULT_QUERY, sort: "total" });
  });
});

describe("pagination", () => {
  it("always has at least one page", () => {
    expect(pageCount(0)).toBe(1);
    expect(pageCount(25)).toBe(1);
    expect(pageCount(26)).toBe(2);
  });

  it("shows the ends and the neighbors of the current page", () => {
    expect(pageWindow(1, 1)).toEqual([1]);
    expect(pageWindow(2, 3)).toEqual([1, 2, 3]);
    expect(pageWindow(5, 10)).toEqual([1, "gap", 4, 5, 6, "gap", 10]);
    expect(pageWindow(1, 10)).toEqual([1, 2, "gap", 10]);
    expect(pageWindow(10, 10)).toEqual([1, "gap", 9, 10]);
  });
});
