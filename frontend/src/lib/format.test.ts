import { describe, expect, it } from "vitest";

import { formatDate, formatMoney, initials } from "./format";

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
