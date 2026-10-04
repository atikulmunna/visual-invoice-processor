import { describe, expect, it } from "vitest";

import { axisTicks, compactAmount, monthLabel, plural, spendSummary, spotlightCopy } from "./overview";

describe("monthLabel", () => {
  it("names the month briefly or in full", () => {
    expect(monthLabel("2026-01")).toBe("Jan");
    expect(monthLabel("2025-12", true)).toBe("December 2025");
  });
});

describe("spendSummary", () => {
  it("reads this month, last month, and the twelve-month total", () => {
    const summary = spendSummary([
      { month: "2026-08", total: 100, count: 1 },
      { month: "2026-09", total: 250.5, count: 2 },
      { month: "2026-10", total: 0, count: 0 },
    ]);

    expect(summary.thisMonth?.month).toBe("2026-10");
    expect(summary.lastMonth?.total).toBe(250.5);
    expect(summary.yearTotal).toBe(350.5);
    expect(summary.yearCount).toBe(3);
  });

  it("copes with no months", () => {
    expect(spendSummary([])).toEqual({ thisMonth: null, lastMonth: null, yearTotal: 0, yearCount: 0 });
  });
});

describe("axisTicks", () => {
  it("uses round steps that reach the largest value", () => {
    expect(axisTicks(9500)).toEqual([0, 2500, 5000, 7500, 10000]);
    expect(axisTicks(40)).toEqual([0, 10, 20, 30, 40]);
    expect(axisTicks(1)).toEqual([0, 0.25, 0.5, 0.75, 1]);
  });

  it("has a single zero line when there is nothing to show", () => {
    expect(axisTicks(0)).toEqual([0]);
  });
});

describe("compactAmount", () => {
  it("abbreviates in lakhs for taka and thousands elsewhere", () => {
    expect(compactAmount(250000, "BDT")).toBe("2.5L");
    expect(compactAmount(250000, "USD")).toBe("250K");
    expect(compactAmount(0, null)).toBe("0");
  });
});

describe("plural", () => {
  it("agrees with the count", () => {
    expect(plural(1, "day")).toBe("1 day");
    expect(plural(1200, "record")).toBe("1,200 records");
  });
});

describe("spotlightCopy", () => {
  it("warns about files that are about to expire", () => {
    const copy = spotlightCopy({ kind: "files_expiring", count: 2, days_left: 3 });

    expect(copy.urgent).toBe(true);
    expect(copy.title).toBe("2 documents in review will lose the original file in 3 days");
    expect(copy.action.to).toBe("/review");
  });

  it("says when the files are already gone", () => {
    expect(spotlightCopy({ kind: "files_expiring", count: 1, days_left: 0 }).title).toBe(
      "1 document in review no longer has its original file",
    );
  });

  it("points failed uploads and flagged records to where they can be handled", () => {
    expect(spotlightCopy({ kind: "uploads_failed", count: 1 }).action.to).toBe("/upload");
    expect(spotlightCopy({ kind: "records_flagged", count: 4 })).toMatchObject({
      title: "4 stored records were flagged",
      action: { to: "/records?flagged=1" },
    });
  });

  it("mentions how long the oldest review has waited", () => {
    expect(spotlightCopy({ kind: "review_waiting", count: 1, oldest_days: 0 }).title).toBe("1 document needs review");
    expect(spotlightCopy({ kind: "review_waiting", count: 3, oldest_days: 2 }).body).toContain("waited 2 days");
  });

  it("is calm when nothing needs attention", () => {
    expect(spotlightCopy(null)).toMatchObject({ urgent: false, eyebrow: "All clear" });
  });
});
