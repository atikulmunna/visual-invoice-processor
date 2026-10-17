import { describe, expect, it } from "vitest";

import { agenda, daysBetween, localToday, paymentState, settleBy, type Payable, type Payment } from "./payables";

const TODAY = "2026-10-16";

function payment(overrides: Partial<Payment>): Payment {
  return { status: "unpaid", date: null, due_date: TODAY, due_printed: true, ...overrides };
}

function bill(id: number, overrides: Partial<Payment>, currency = "BDT", total = 100): Payable {
  return { id, vendor_name: "Star Tech", invoice_number: `INV-${id}`, invoice_date: "2026-10-01",
           payment: payment(overrides), currency, total_amount: total };
}

describe("dates", () => {
  it("reads the local date and counts whole days across months", () => {
    expect(localToday(new Date(2026, 0, 5, 23, 30))).toBe("2026-01-05");
    expect(daysBetween("2026-10-16", "2026-11-02")).toBe(17);
    expect(daysBetween("2026-10-16", "2026-10-10")).toBe(-6);
  });

  it("settles a scheduled bill on its planned date, anything else on its due date", () => {
    expect(settleBy(payment({ status: "scheduled", date: "2026-10-30" }))).toBe("2026-10-30");
    expect(settleBy(payment({ due_date: "2026-10-20" }))).toBe("2026-10-20");
  });
});

describe("agenda", () => {
  it("groups bills by how soon they are settled, with totals per currency", () => {
    const groups = agenda(
      [
        bill(1, { due_date: "2026-10-10" }, "BDT", 500),
        bill(2, { status: "scheduled", date: "2026-10-12", due_date: "2026-10-30" }, "USD", 20),
        bill(3, { due_date: TODAY }, "BDT", 100),
        bill(4, { due_date: "2026-10-22" }, "BDT", 50),
        bill(5, { due_date: "2026-10-23" }),
        bill(6, { due_date: "2026-11-20" }),
        bill(7, { due_date: null }),
      ],
      TODAY,
    );

    expect(groups.map((group) => [group.key, group.items.map((item) => item.id)])).toEqual([
      ["overdue", [1, 2]],
      ["week", [3, 4]],
      ["next", [5]],
      ["later", [6, 7]],
    ]);
    expect(groups[0].totals).toEqual([{ currency: "BDT", total: 500 }, { currency: "USD", total: 20 }]);
    expect(groups[1].totals).toEqual([{ currency: "BDT", total: 150 }]);
  });

  it("leaves out empty groups", () => {
    expect(agenda([], TODAY)).toEqual([]);
    expect(agenda([bill(1, { due_date: "2026-12-01" })], TODAY).map((group) => group.key)).toEqual(["later"]);
  });
});

describe("paymentState", () => {
  it("says where each bill stands", () => {
    expect(paymentState(payment({ due_date: "2026-10-15" }), TODAY)).toEqual({ label: "Overdue by 1 day", tone: "danger" });
    expect(paymentState(payment({ due_date: "2026-10-06" }), TODAY).label).toBe("Overdue by 10 days");
    expect(paymentState(payment({}), TODAY)).toEqual({ label: "Due today", tone: "warning" });
    expect(paymentState(payment({ due_date: "2026-10-30" }), TODAY)).toEqual({ label: "Unpaid", tone: "neutral" });
    expect(paymentState(payment({ status: "scheduled", date: "2026-10-20" }), TODAY)).toEqual({
      label: "Scheduled for 20 Oct 2026", tone: "info" });
    expect(paymentState(payment({ status: "scheduled", date: "2026-10-12" }), TODAY)).toEqual({
      label: "Planned date passed", tone: "danger" });
    expect(paymentState(payment({ status: "paid", date: "2026-10-14" }), TODAY)).toEqual({
      label: "Paid 14 Oct 2026", tone: "success" });
  });
});
