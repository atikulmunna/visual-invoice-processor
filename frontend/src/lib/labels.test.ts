import { describe, expect, it } from "vitest";

import { currencyNote, reasonLabel, statusLabel } from "./labels";

describe("statusLabel", () => {
  it("maps pipeline statuses to plain language and a tone", () => {
    expect(statusLabel("REVIEW_REQUIRED")).toEqual({ label: "Needs review", tone: "warning" });
    expect(statusLabel("STORED")).toEqual({ label: "Stored", tone: "success" });
    expect(statusLabel("FAILED").tone).toBe("danger");
  });

  it("humanizes unknown statuses instead of showing raw codes", () => {
    expect(statusLabel("SOMETHING_NEW")).toEqual({ label: "Something new", tone: "neutral" });
  });
});

describe("reasonLabel", () => {
  it("explains the review reasons added for missing fields", () => {
    expect(reasonLabel("missing_vendor")).toBe("Vendor name not found on the document");
    expect(reasonLabel("missing_invoice_date")).toBe("Invoice date not found on the document");
  });

  it("humanizes unknown codes", () => {
    expect(reasonLabel("line_item_sum_mismatch")).toBe("Line item sum mismatch");
    expect(reasonLabel("")).toBe("Unknown");
  });
});

describe("currencyNote", () => {
  it("explains an assumed currency and stays silent otherwise", () => {
    expect(currencyNote(true, "BDT")).toBe("Not shown on the document; assumed BDT, your base currency");
    expect(currencyNote(false, "BDT")).toBeNull();
    expect(currencyNote(undefined, "USD")).toBeNull();
  });
});
