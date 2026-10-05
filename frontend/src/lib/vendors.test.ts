import { describe, expect, it } from "vitest";

import { filterVendors, suggestionKey, totalsLabel, type Vendor } from "./vendors";

function vendor(overrides: Partial<Vendor>): Vendor {
  return {
    id: 1,
    name: "RYANS Computers",
    tax_id: null,
    default_currency: "BDT",
    aliases: [],
    records: 1,
    first_invoice_date: null,
    last_invoice_date: null,
    totals: [],
    ...overrides,
  };
}

describe("totalsLabel", () => {
  it("lists every currency without adding them together", () => {
    expect(totalsLabel([])).toBe("No spend yet");
    expect(totalsLabel([{ currency: "BDT", total: 244810.89 }])).toBe("BDT 2,44,810.89");
    expect(
      totalsLabel([
        { currency: "BDT", total: 1000 },
        { currency: "USD", total: 41.61 },
        { currency: "EUR", total: 5 },
      ]),
    ).toBe("BDT 1,000.00, USD 41.61 and EUR 5.00");
  });
});

describe("filterVendors", () => {
  const vendors = [
    vendor({ id: 1, name: "RYANS Computers", aliases: ["RYANS"] }),
    vendor({ id: 2, name: "Star Tech", tax_id: "0012345" }),
  ];

  it("matches names, earlier spellings, and tax IDs", () => {
    expect(filterVendors(vendors, " ").map((row) => row.id)).toEqual([1, 2]);
    expect(filterVendors(vendors, "ryans").map((row) => row.id)).toEqual([1]);
    expect(filterVendors(vendors, "2345").map((row) => row.id)).toEqual([2]);
  });
});

describe("suggestionKey", () => {
  it("is the same whichever vendor is kept", () => {
    expect(suggestionKey({ keep: 7, merge: 3, keep_name: "A", merge_name: "B" })).toBe(
      suggestionKey({ keep: 3, merge: 7, keep_name: "B", merge_name: "A" }),
    );
  });
});
