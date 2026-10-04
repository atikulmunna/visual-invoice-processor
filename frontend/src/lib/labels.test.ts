import { describe, expect, it } from "vitest";

import { currencyNote, processingErrorLabel, reasonLabel, statusLabel } from "./labels";

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
    expect(reasonLabel("some_new_check")).toBe("Some new check");
    expect(reasonLabel("line_item_sum_mismatch")).toBe("Line items do not add up to the subtotal");
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

describe("processingErrorLabel", () => {
  it("explains known problems with the file", () => {
    expect(processingErrorLabel("page_limit_exceeded")).toBe("The PDF has more pages than allowed.");
  });

  it("uses a rejection's own message and a generic line otherwise", () => {
    expect(processingErrorLabel("something_new", "PDFs may contain at most 5 pages")).toBe(
      "PDFs may contain at most 5 pages",
    );
    expect(processingErrorLabel("provider_request_failed", null)).toBe("Processing failed. Try again.");
    expect(processingErrorLabel(null)).toBe("Processing failed. Try again.");
  });
});
