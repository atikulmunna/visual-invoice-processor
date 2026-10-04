import { describe, expect, it } from "vitest";

import { formatDate, formatMoney, formatRelativeTime, initials } from "./format";

describe("formatMoney", () => {
  it("groups taka and rupee amounts in lakhs", () => {
    expect(formatMoney(216500, "BDT")).toBe("BDT 2,16,500.00");
    expect(formatMoney(12345678.9, "inr")).toBe("INR 1,23,45,678.90");
  });

  it("uses thousands grouping elsewhere and always shows two decimals", () => {
    expect(formatMoney(1234.5, "USD")).toBe("USD 1,234.50");
    expect(formatMoney(12, "EUR")).toBe("EUR 12.00");
  });

  it("handles negative amounts and a missing currency", () => {
    expect(formatMoney(-50.1, "USD")).toBe("USD -50.10");
    expect(formatMoney(5274.88, null)).toBe("5,274.88");
  });
});

describe("formatDate", () => {
  it("formats ISO dates day first without timezone drift", () => {
    expect(formatDate("2026-03-04")).toBe("4 Mar 2026");
    expect(formatDate("2026-12-31T23:30:00Z")).toBe("31 Dec 2026");
  });

  it("returns null for missing or invalid dates", () => {
    expect(formatDate(null)).toBeNull();
    expect(formatDate("")).toBeNull();
    expect(formatDate("not a date")).toBeNull();
  });
});

describe("initials", () => {
  it("builds two-letter initials from usernames", () => {
    expect(initials("tester.one")).toBe("TO");
    expect(initials("munna")).toBe("MU");
  });
});

describe("formatRelativeTime", () => {
  const now = Date.parse("2026-10-04T12:00:00Z");

  it("describes recent moments in words", () => {
    expect(formatRelativeTime("2026-10-04T11:59:40Z", now)).toBe("just now");
    expect(formatRelativeTime("2026-10-04T11:57:00Z", now)).toBe("3 minutes ago");
    expect(formatRelativeTime("2026-10-04T10:00:00Z", now)).toBe("2 hours ago");
    expect(formatRelativeTime("2026-10-03T12:00:00Z", now)).toBe("yesterday");
  });

  it("falls back to a date after a week and handles missing values", () => {
    expect(formatRelativeTime("2026-09-01T09:00:00Z", now)).toBe("1 Sept 2026");
    expect(formatRelativeTime(null, now)).toBeNull();
    expect(formatRelativeTime("garbage", now)).toBeNull();
  });
});
