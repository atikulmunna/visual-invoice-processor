import { describe, expect, it } from "vitest";

import { checkForm, formFromRecord, nextAfter, recordFromForm, type ReviewForm } from "./review";

const RECORD = {
  document_type: "receipt",
  vendor_name: null,
  vendor_tax_id: null,
  invoice_number: "INV-7",
  invoice_date: "2026-03-02",
  due_date: null,
  currency: "BDT",
  currency_assumed: true,
  payment_method: "cash",
  subtotal: 100,
  tax_amount: 15,
  shipping_amount: 0,
  discount_amount: 0,
  total_amount: 115,
  line_items: [{ description: "Paper", quantity: 2, unit_price: 50, line_total: 100, category: "office" }],
  model_confidence: 0.4,
  validation_score: 0.5,
};

function form(overrides: Partial<ReviewForm> = {}): ReviewForm {
  return { ...formFromRecord(RECORD), vendor_name: "Acme", ...overrides };
}

describe("formFromRecord", () => {
  it("turns a stored record into editable text, leaving missing values empty", () => {
    const result = formFromRecord(RECORD);

    expect(result.vendor_name).toBe("");
    expect(result.due_date).toBe("");
    expect(result.total_amount).toBe("115");
    expect(result.line_items[0]).toEqual({
      description: "Paper",
      quantity: "2",
      unit_price: "50",
      line_total: "100",
      category: "office",
    });
  });
});

describe("recordFromForm", () => {
  it("applies edits and keeps fields the form does not show", () => {
    const record = recordFromForm(form({ vendor_name: "  Acme Traders ", invoice_number: "" }), RECORD);

    expect(record.vendor_name).toBe("Acme Traders");
    expect(record.invoice_number).toBeNull();
    expect(record.total_amount).toBe(115);
    expect(record.model_confidence).toBe(0.4);
    expect(record.line_items).toEqual([
      { description: "Paper", quantity: 2, unit_price: 50, line_total: 100, category: "office" },
    ]);
  });

  it("stops calling the currency assumed once the reviewer changes it", () => {
    expect(recordFromForm(form(), RECORD).currency_assumed).toBe(true);
    expect(recordFromForm(form({ currency: "usd" }), RECORD)).toMatchObject({ currency: "USD", currency_assumed: false });
  });
});

describe("checkForm", () => {
  it("passes a complete, consistent record", () => {
    expect(checkForm(form())).toEqual([]);
  });

  it("blocks approval when required fields are missing", () => {
    const issues = checkForm(form({ vendor_name: " ", invoice_date: "", currency: "Taka" }));

    expect(issues.filter((issue) => issue.blocking).map((issue) => issue.field)).toEqual([
      "vendor_name",
      "invoice_date",
      "currency",
    ]);
  });

  it("blocks invalid amounts and line items the server would refuse", () => {
    const issues = checkForm(
      form({
        total_amount: "0",
        tax_amount: "-1",
        line_items: [{ description: "", quantity: "0", unit_price: "abc", line_total: "100", category: null }],
      }),
    );

    expect(issues.filter((issue) => issue.blocking).map((issue) => issue.field)).toEqual([
      "tax_amount",
      "total_amount",
      "line_items.0.description",
      "line_items.0.quantity",
      "line_items.0.unit_price",
    ]);
  });

  it("warns, without blocking, when the arithmetic does not add up", () => {
    const issues = checkForm(form({ total_amount: "120", line_items: [{ ...form().line_items[0], line_total: "90" }] }));

    expect(issues).toEqual([
      {
        field: "total_amount",
        message: "Subtotal, tax, shipping, and discount come to BDT 115.00, but the total is BDT 120.00.",
        blocking: false,
      },
      {
        field: "subtotal",
        message: "Line items add up to BDT 90.00, but the subtotal is BDT 100.00.",
        blocking: false,
      },
    ]);
  });

  it("accepts amounts typed with lakh or thousands separators", () => {
    const lakh = form({ subtotal: "2,16,500", tax_amount: "0", total_amount: "2,16,500.00", line_items: [] });

    expect(checkForm(lakh)).toEqual([]);
  });
});

describe("nextAfter", () => {
  it("moves to the next item, or the previous one at the end, or nothing when done", () => {
    expect(nextAfter(["a", "b", "c"], "a")).toBe("b");
    expect(nextAfter(["a", "b", "c"], "c")).toBe("b");
    expect(nextAfter(["a"], "a")).toBeNull();
    expect(nextAfter(["a", "b"], "gone")).toBe("a");
  });
});
