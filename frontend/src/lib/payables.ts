import { formatDate } from "./format";
import type { Tone } from "./labels";

export type PaymentStatus = "unpaid" | "scheduled" | "paid";

/** How a bill stands for payment. */
export interface Payment {
  status: PaymentStatus;
  /** The planned date while scheduled, the day it was paid once paid. */
  date: string | null;
  /** The printed due date, or the invoice date when none is printed. */
  due_date: string | null;
  due_printed: boolean;
}

export interface Payable {
  id: number;
  vendor_name: string | null;
  invoice_number: string | null;
  invoice_date: string | null;
  payment: Payment;
  currency: string | null;
  total_amount: number | null;
}

export interface AgendaGroup {
  key: "overdue" | "week" | "next" | "later";
  label: string;
  items: Payable[];
  totals: { currency: string; total: number }[];
}

const GROUPS: { key: AgendaGroup["key"]; label: string; lastDay: number }[] = [
  { key: "overdue", label: "Overdue", lastDay: -1 },
  { key: "week", label: "Due in the next 7 days", lastDay: 6 },
  { key: "next", label: "Due in 8 to 14 days", lastDay: 13 },
  { key: "later", label: "Later", lastDay: Number.POSITIVE_INFINITY },
];

/** Today in the browser's own time zone, as YYYY-MM-DD. */
export function localToday(now = new Date()): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`;
}

/** Whole days from one YYYY-MM-DD date to another. */
export function daysBetween(from: string, to: string): number {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86_400_000);
}

/** The date a bill should be settled by: the planned date once scheduled, otherwise its due date. */
export function settleBy(payment: Payment): string | null {
  return payment.status === "scheduled" && payment.date ? payment.date : payment.due_date;
}

function totalsOf(items: Payable[]): AgendaGroup["totals"] {
  const sums = new Map<string, number>();
  for (const item of items) {
    if (item.currency && item.total_amount !== null) {
      sums.set(item.currency, (sums.get(item.currency) ?? 0) + item.total_amount);
    }
  }
  return [...sums].map(([currency, total]) => ({ currency, total })).sort((a, b) => b.total - a.total);
}

/** Bills still to pay, grouped by how soon they should be settled; empty groups are left out. */
export function agenda(items: Payable[], today: string): AgendaGroup[] {
  const groupOf = (item: Payable) => {
    const date = settleBy(item.payment);
    const days = date ? daysBetween(today, date) : Number.POSITIVE_INFINITY;
    return GROUPS.find((group) => days <= group.lastDay) ?? GROUPS[GROUPS.length - 1];
  };
  return GROUPS.map(({ key, label }) => {
    const members = items.filter((item) => groupOf(item).key === key);
    return { key, label, items: members, totals: totalsOf(members) };
  }).filter((group) => group.items.length > 0);
}

/** A short, plain description of where a bill stands, and the tone to show it in. */
export function paymentState(payment: Payment, today: string): { label: string; tone: Tone } {
  if (payment.status === "paid") {
    return { label: `Paid ${formatDate(payment.date) ?? ""}`.trim(), tone: "success" };
  }
  const by = settleBy(payment);
  const days = by ? daysBetween(today, by) : null;
  if (payment.status === "scheduled") {
    return days !== null && days < 0
      ? { label: "Planned date passed", tone: "danger" }
      : { label: `Scheduled for ${formatDate(payment.date) ?? ""}`.trim(), tone: "info" };
  }
  if (days === null) {
    return { label: "Unpaid", tone: "neutral" };
  }
  if (days < 0) {
    return { label: `Overdue by ${Math.abs(days)} ${Math.abs(days) === 1 ? "day" : "days"}`, tone: "danger" };
  }
  return days === 0 ? { label: "Due today", tone: "warning" } : { label: "Unpaid", tone: "neutral" };
}
