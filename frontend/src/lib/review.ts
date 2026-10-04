import { formatMoney } from "./format";

export interface ReviewSummary {
  document_id: string;
  created_at: string | null;
  reason_codes: string[];
  vendor_name: string | null;
  invoice_number: string | null;
  invoice_date: string | null;
  currency: string | null;
  total_amount: number | null;
}

export interface ReviewDocument {
  name: string | null;
  content_type: string | null;
  url: string | null;
}

export type StoredRecord = Record<string, unknown>;

export interface ReviewDetail {
  document_id: string;
  status: string;
  created_at: string | null;
  reason_codes: string[];
  violation_codes: string[];
  record: StoredRecord;
  document: ReviewDocument;
}

export interface LineItemForm {
  description: string;
  quantity: string;
  unit_price: string;
  line_total: string;
  category: string | null;
}

export interface ReviewForm {
  document_type: string;
  vendor_name: string;
  vendor_tax_id: string;
  invoice_number: string;
  invoice_date: string;
  due_date: string;
  currency: string;
  payment_method: string;
  subtotal: string;
  tax_amount: string;
  shipping_amount: string;
  discount_amount: string;
  total_amount: string;
  line_items: LineItemForm[];
}

export type AmountField = "subtotal" | "tax_amount" | "shipping_amount" | "discount_amount" | "total_amount";
export const AMOUNT_FIELDS: AmountField[] = ["subtotal", "tax_amount", "shipping_amount", "discount_amount", "total_amount"];

export const DOCUMENT_TYPE_OPTIONS = [
  { value: "invoice", label: "Invoice" },
  { value: "receipt", label: "Receipt" },
];
export const PAYMENT_METHOD_OPTIONS = [
  { value: "unknown", label: "Not shown" },
  { value: "card", label: "Card" },
  { value: "cash", label: "Cash" },
  { value: "bank", label: "Bank transfer" },
];
export const AMOUNT_LABELS: Record<AmountField, string> = {
  subtotal: "Subtotal",
  tax_amount: "Tax or VAT",
  shipping_amount: "Shipping",
  discount_amount: "Discount",
  total_amount: "Total",
};

/** The label shown for a stored value, such as "Bank transfer" for "bank". */
export function optionLabel(options: { value: string; label: string }[], value: unknown): string | null {
  return options.find((option) => option.value === value)?.label ?? null;
}

export interface Issue {
  /** A form field name, or "line_items.<index>.<field>" for a line item. */
  field: string;
  message: string;
  /** Blocking issues would make the server refuse the approval. */
  blocking: boolean;
}

const TOLERANCE = 0.01;

function text(value: unknown): string {
  return value === null || value === undefined ? "" : String(value);
}

function parseAmount(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed.replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

export function formFromRecord(record: StoredRecord): ReviewForm {
  const lines = Array.isArray(record.line_items) ? (record.line_items as StoredRecord[]) : [];
  return {
    document_type: text(record.document_type) || "invoice",
    vendor_name: text(record.vendor_name),
    vendor_tax_id: text(record.vendor_tax_id),
    invoice_number: text(record.invoice_number),
    invoice_date: text(record.invoice_date),
    due_date: text(record.due_date),
    currency: text(record.currency).toUpperCase(),
    payment_method: text(record.payment_method) || "unknown",
    subtotal: text(record.subtotal),
    tax_amount: text(record.tax_amount),
    shipping_amount: text(record.shipping_amount),
    discount_amount: text(record.discount_amount),
    total_amount: text(record.total_amount),
    line_items: lines.map((line) => ({
      description: text(line.description),
      quantity: text(line.quantity),
      unit_price: text(line.unit_price),
      line_total: text(line.line_total),
      category: line.category ? String(line.category) : null,
    })),
  };
}

/** The record to approve: the reviewer's edits on top of everything the form does not show. */
export function recordFromForm(form: ReviewForm, original: StoredRecord): StoredRecord {
  const currency = form.currency.trim().toUpperCase();
  const amount = (value: string) => parseAmount(value) ?? 0;
  return {
    ...original,
    document_type: form.document_type,
    vendor_name: form.vendor_name.trim(),
    vendor_tax_id: form.vendor_tax_id.trim() || null,
    invoice_number: form.invoice_number.trim() || null,
    invoice_date: form.invoice_date,
    due_date: form.due_date || null,
    currency,
    // A reviewer who changes the currency has confirmed it from the document.
    currency_assumed: Boolean(original.currency_assumed) && currency === text(original.currency).toUpperCase(),
    payment_method: form.payment_method,
    subtotal: amount(form.subtotal),
    tax_amount: amount(form.tax_amount),
    shipping_amount: amount(form.shipping_amount),
    discount_amount: amount(form.discount_amount),
    total_amount: amount(form.total_amount),
    line_items: form.line_items.map((line) => ({
      description: line.description.trim(),
      quantity: amount(line.quantity),
      unit_price: amount(line.unit_price),
      line_total: amount(line.line_total),
      category: line.category,
    })),
  };
}

function checkAmounts(form: ReviewForm, issues: Issue[]): void {
  for (const field of AMOUNT_FIELDS) {
    const raw = form[field];
    const value = parseAmount(raw);
    const optional = field !== "total_amount" && field !== "subtotal";
    if (raw.trim() === "" && optional) continue;
    if (value === null) {
      issues.push({ field, message: "Enter a number.", blocking: true });
    } else if (value < 0) {
      issues.push({ field, message: "Amounts cannot be negative.", blocking: true });
    }
  }
  const total = parseAmount(form.total_amount);
  if (total !== null && total <= 0) {
    issues.push({ field: "total_amount", message: "Enter the total from the document.", blocking: true });
  }
}

function checkLines(form: ReviewForm, issues: Issue[]): void {
  form.line_items.forEach((line, index) => {
    const prefix = `line_items.${index}`;
    if (!line.description.trim()) {
      issues.push({ field: `${prefix}.description`, message: `Describe line ${index + 1}.`, blocking: true });
    }
    const quantity = parseAmount(line.quantity);
    if (quantity === null || quantity <= 0) {
      issues.push({ field: `${prefix}.quantity`, message: `Line ${index + 1} needs a quantity above zero.`, blocking: true });
    }
    for (const field of ["unit_price", "line_total"] as const) {
      const value = parseAmount(line[field]);
      if (value === null || value < 0) {
        issues.push({ field: `${prefix}.${field}`, message: `Line ${index + 1} needs a valid amount.`, blocking: true });
      }
    }
  });
}

/** Arithmetic that does not add up is worth a look but does not block approval. */
function checkArithmetic(form: ReviewForm, issues: Issue[]): void {
  const currency = form.currency.trim().toUpperCase() || null;
  const value = (field: AmountField) => parseAmount(form[field]) ?? 0;
  const total = parseAmount(form.total_amount);
  if (total !== null) {
    const computed = value("subtotal") + value("tax_amount") + value("shipping_amount") - value("discount_amount");
    if (Math.abs(computed - total) > TOLERANCE) {
      issues.push({
        field: "total_amount",
        message: `Subtotal, tax, shipping, and discount come to ${formatMoney(computed, currency)}, but the total is ${formatMoney(total, currency)}.`,
        blocking: false,
      });
    }
  }
  if (form.line_items.length > 0) {
    const lineSum = form.line_items.reduce((sum, line) => sum + (parseAmount(line.line_total) ?? 0), 0);
    const subtotal = value("subtotal");
    if (Math.abs(lineSum - subtotal) > TOLERANCE) {
      issues.push({
        field: "subtotal",
        message: `Line items add up to ${formatMoney(lineSum, currency)}, but the subtotal is ${formatMoney(subtotal, currency)}.`,
        blocking: false,
      });
    }
  }
}

export function checkForm(form: ReviewForm): Issue[] {
  const issues: Issue[] = [];
  if (!form.vendor_name.trim()) {
    issues.push({ field: "vendor_name", message: "Enter the vendor name from the document.", blocking: true });
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(form.invoice_date)) {
    issues.push({ field: "invoice_date", message: "Enter the invoice date.", blocking: true });
  }
  if (form.due_date && !/^\d{4}-\d{2}-\d{2}$/.test(form.due_date)) {
    issues.push({ field: "due_date", message: "Enter a valid due date or leave it empty.", blocking: true });
  }
  if (!/^[A-Za-z]{3}$/.test(form.currency.trim())) {
    issues.push({ field: "currency", message: "Use a three-letter currency code, such as BDT or USD.", blocking: true });
  }
  checkAmounts(form, issues);
  checkLines(form, issues);
  checkArithmetic(form, issues);
  return issues;
}

/** The item to open after this one is resolved: the next in the queue, else the previous. */
export function nextAfter(queue: string[], current: string): string | null {
  const index = queue.indexOf(current);
  const remaining = queue.filter((id) => id !== current);
  if (remaining.length === 0) return null;
  if (index < 0) return remaining[0];
  return remaining[Math.min(index, remaining.length - 1)];
}
