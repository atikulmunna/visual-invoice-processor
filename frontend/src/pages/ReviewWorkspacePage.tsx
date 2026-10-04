import { useEffect, useRef, useState, type ChangeEvent, type FormEvent } from "react";
import { Link, useNavigate, useParams } from "react-router";

import { Dialog } from "../components/Dialog";
import { DocumentViewer } from "../components/DocumentViewer";
import { Icon } from "../components/Icon";
import { useToast } from "../components/Toast";
import { Button, buttonClass, Card, EmptyState, SelectField, Skeleton, TextField } from "../components/ui";
import { api, ApiError } from "../lib/api";
import { formatMoney } from "../lib/format";
import { reasonLabel, statusLabel } from "../lib/labels";
import {
  AMOUNT_LABELS,
  checkForm,
  DOCUMENT_TYPE_OPTIONS,
  formFromRecord,
  nextAfter,
  PAYMENT_METHOD_OPTIONS,
  recordFromForm,
  type AmountField,
  type Issue,
  type LineItemForm,
  type ReviewDetail,
  type ReviewForm,
} from "../lib/review";
import { useShortcuts } from "../lib/shortcuts";
import { useSession } from "../shell/session";

type Dismissal = "reject" | "duplicate";

const DISMISSALS: Record<Dismissal, { title: string; action: string; done: string; text: string }> = {
  reject: {
    title: "Reject this document?",
    action: "Reject",
    done: "Document rejected",
    text: "It leaves the review queue and is not stored as a record.",
  },
  duplicate: {
    title: "Mark as a duplicate?",
    action: "Mark duplicate",
    done: "Marked as a duplicate",
    text: "Use this when the same invoice is already in your records. It will not be stored again.",
  },
};

function issueText(issues: Issue[], field: string): string | null {
  return issues.find((issue) => issue.field === field && issue.blocking)?.message ?? null;
}

function ReasonsPanel({ detail }: { detail: ReviewDetail }) {
  const reasons = [...new Set([...detail.reason_codes, ...detail.violation_codes])].map(reasonLabel);
  const assumed = Boolean(detail.record.currency_assumed);
  return (
    <div className="callout tone-warning" role="note">
      <Icon name="alert" size={18} />
      <div>
        <strong>Why this needs review</strong>
        <ul className="reason-list">
          {reasons.map((reason) => (
            <li key={reason}>{reason}</li>
          ))}
          {assumed && <li>The currency was not shown, so your base currency was assumed. Confirm it below.</li>}
        </ul>
      </div>
    </div>
  );
}

interface FieldsProps {
  form: ReviewForm;
  issues: Issue[];
  set: (field: keyof ReviewForm) => (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => void;
}

function DetailFields({ form, issues, set }: FieldsProps) {
  return (
    <Card>
      <h2 className="card-title">Details</h2>
      <div className="form-grid">
        <TextField label="Vendor name" name="vendor_name" value={form.vendor_name} onChange={set("vendor_name")}
          error={issueText(issues, "vendor_name")} autoComplete="off" />
        <TextField label="Invoice number" name="invoice_number" value={form.invoice_number}
          onChange={set("invoice_number")} autoComplete="off" />
        <TextField label="Invoice date" name="invoice_date" type="date" value={form.invoice_date}
          onChange={set("invoice_date")} error={issueText(issues, "invoice_date")} />
        <TextField label="Due date" name="due_date" type="date" value={form.due_date} onChange={set("due_date")}
          error={issueText(issues, "due_date")} />
        <TextField label="Currency" name="currency" value={form.currency} maxLength={3} autoComplete="off"
          onChange={set("currency")} error={issueText(issues, "currency")} spellCheck={false} />
        <TextField label="Vendor tax ID or BIN" name="vendor_tax_id" value={form.vendor_tax_id}
          onChange={set("vendor_tax_id")} autoComplete="off" />
        <SelectField label="Document type" name="document_type" value={form.document_type}
          onChange={set("document_type")} options={DOCUMENT_TYPE_OPTIONS} />
        <SelectField label="Payment method" name="payment_method" value={form.payment_method}
          onChange={set("payment_method")} options={PAYMENT_METHOD_OPTIONS} />
      </div>
    </Card>
  );
}

function AmountFields({ form, issues, set }: FieldsProps) {
  return (
    <Card>
      <h2 className="card-title">Amounts</h2>
      <div className="form-grid">
        {(Object.keys(AMOUNT_LABELS) as AmountField[]).map((field) => (
          <TextField key={field} label={AMOUNT_LABELS[field]} name={field} value={form[field]} inputMode="decimal"
            onChange={set(field)} error={issueText(issues, field)} autoComplete="off" />
        ))}
      </div>
    </Card>
  );
}

interface LineItemsProps {
  lines: LineItemForm[];
  issues: Issue[];
  onChange: (lines: LineItemForm[]) => void;
}

function LineItemsEditor({ lines, issues, onChange }: LineItemsProps) {
  const update = (index: number, field: keyof LineItemForm, value: string) =>
    onChange(lines.map((line, position) => (position === index ? { ...line, [field]: value } : line)));
  const invalid = (index: number, field: string) => (issueText(issues, `line_items.${index}.${field}`) ? true : undefined);

  return (
    <Card>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <h2 className="card-title">Line items</h2>
        <Button size="sm" variant="tinted" icon="plus"
          onClick={() => onChange([...lines, { description: "", quantity: "1", unit_price: "", line_total: "", category: null }])}>
          Add line
        </Button>
      </div>
      {lines.length === 0 ? (
        <p className="muted" style={{ marginTop: 12 }}>No line items. Add them if the document lists items.</p>
      ) : (
        <div className="lines" role="group" aria-label="Line items">
          <div className="lines-head" aria-hidden="true">
            <span>Description</span><span>Qty</span><span>Unit price</span><span>Line total</span><span />
          </div>
          {lines.map((line, index) => (
            <div className="lines-row" key={index}>
              <input className="input" aria-label={`Line ${index + 1} description`} name={`line_items.${index}.description`}
                value={line.description} aria-invalid={invalid(index, "description")}
                onChange={(event) => update(index, "description", event.target.value)} />
              <input className="input" aria-label={`Line ${index + 1} quantity`} name={`line_items.${index}.quantity`}
                inputMode="decimal" value={line.quantity} aria-invalid={invalid(index, "quantity")}
                onChange={(event) => update(index, "quantity", event.target.value)} />
              <input className="input" aria-label={`Line ${index + 1} unit price`} name={`line_items.${index}.unit_price`}
                inputMode="decimal" value={line.unit_price} aria-invalid={invalid(index, "unit_price")}
                onChange={(event) => update(index, "unit_price", event.target.value)} />
              <input className="input" aria-label={`Line ${index + 1} total`} name={`line_items.${index}.line_total`}
                inputMode="decimal" value={line.line_total} aria-invalid={invalid(index, "line_total")}
                onChange={(event) => update(index, "line_total", event.target.value)} />
              <Button size="sm" variant="ghost" className="btn-icon" icon="close" aria-label={`Remove line ${index + 1}`}
                onClick={() => onChange(lines.filter((_, position) => position !== index))} />
            </div>
          ))}
        </div>
      )}
      {issues
        .filter((issue) => issue.blocking && issue.field.startsWith("line_items."))
        .map((issue) => (
          <p key={issue.field} className="field-error" style={{ marginTop: 8 }}>{issue.message}</p>
        ))}
    </Card>
  );
}

export function ReviewWorkspacePage() {
  const { documentId = "" } = useParams();
  const navigate = useNavigate();
  const toast = useToast();
  const { refresh } = useSession();
  const [queue, setQueue] = useState<string[] | null>(null);
  const [detail, setDetail] = useState<ReviewDetail | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [form, setForm] = useState<ReviewForm | null>(null);
  const [busy, setBusy] = useState(false);
  const [dismissal, setDismissal] = useState<Dismissal | null>(null);
  const [note, setNote] = useState("");
  const heading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    api.reviewQueue().then(
      (response) => setQueue(response.items.map((item) => item.document_id)),
      () => setQueue([]),
    );
  }, []);

  useEffect(() => {
    setDetail(null);
    setForm(null);
    setLoadError(null);
    api.reviewItem(documentId).then(
      (loaded) => {
        setDetail(loaded);
        setForm(formFromRecord(loaded.record));
        window.setTimeout(() => heading.current?.focus(), 0);
      },
      (error) =>
        setLoadError(error instanceof ApiError && error.status === 404 ? "This document is not in your review queue." : "The document could not be loaded."),
    );
  }, [documentId]);

  const issues = form ? checkForm(form) : [];
  const warnings = issues.filter((issue) => !issue.blocking);
  const position = queue ? queue.indexOf(documentId) : -1;
  const active = detail?.status === "REVIEW_REQUIRED";

  const set = (field: keyof ReviewForm) => (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm((current) => (current ? { ...current, [field]: event.target.value } : current));

  function open(id: string | null) {
    navigate(id ? `/review/${encodeURIComponent(id)}` : "/review", { replace: true });
  }

  async function decide(action: "approve" | Dismissal, extra: { corrected_record?: Record<string, unknown>; note?: string }) {
    setBusy(true);
    try {
      await api.resolveReview(documentId, { action, ...extra });
      toast({ tone: "success", title: action === "approve" ? "Approved and stored" : DISMISSALS[action].done });
      void refresh();
      const next = queue ? nextAfter(queue, documentId) : null;
      setQueue((current) => current?.filter((id) => id !== documentId) ?? current);
      open(next);
    } catch (error) {
      toast({ tone: "danger", title: "Not saved", description: error instanceof Error ? error.message : undefined });
    } finally {
      setBusy(false);
    }
  }

  function approve(event?: FormEvent) {
    event?.preventDefault();
    if (!form || !detail || !active || busy) return;
    const firstProblem = issues.find((issue) => issue.blocking);
    if (firstProblem) {
      document.querySelector<HTMLElement>(`[name="${firstProblem.field}"]`)?.focus();
      toast({ tone: "danger", title: "Fix the highlighted fields first", description: firstProblem.message });
      return;
    }
    void decide("approve", { corrected_record: recordFromForm(form, detail.record) });
  }

  function confirmDismissal() {
    if (!dismissal) return;
    const action = dismissal;
    setDismissal(null);
    void decide(action, { note: note.trim() || undefined });
    setNote("");
  }

  useShortcuts(
    {
      a: () => approve(),
      r: () => active && setDismissal("reject"),
      d: () => active && setDismissal("duplicate"),
      j: () => queue && position >= 0 && position < queue.length - 1 && open(queue[position + 1]),
      k: () => queue && position > 0 && open(queue[position - 1]),
    },
    Boolean(detail) && !busy,
  );

  if (loadError) {
    return (
      <div className="page">
        <EmptyState icon="alert" title={loadError} headingLevel="h1"
          action={<Link to="/review" className={buttonClass("tinted")}>Back to the review queue</Link>} />
      </div>
    );
  }

  return (
    <div className="page page-wide stack">
      <div className="review-header">
        <div>
          <Link to="/review" className="back-link">
            <Icon name="arrowLeft" size={16} /> Review queue
          </Link>
          <h1 className="page-title" ref={heading} tabIndex={-1}>
            {detail ? form?.vendor_name.trim() || "Vendor not found" : <Skeleton width={260} height={34} />}
          </h1>
        </div>
        {queue && position >= 0 && (
          <div className="row review-position">
            <span className="muted">
              {position + 1} of {queue.length}
            </span>
            <Button size="sm" variant="ghost" icon="arrowLeft" disabled={position === 0}
              onClick={() => open(queue[position - 1])} aria-label="Previous document (K)">
              Previous
            </Button>
            <Button size="sm" variant="ghost" disabled={position === queue.length - 1}
              onClick={() => open(queue[position + 1])} aria-label="Next document (J)">
              Next <Icon name="arrowRight" size={15} />
            </Button>
          </div>
        )}
      </div>

      {!detail || !form ? (
        <div className="review-layout">
          <Skeleton height={480} />
          <Skeleton height={480} />
        </div>
      ) : (
        <div className="review-layout">
          <DocumentViewer key={detail.document_id} document={detail.document} />
          <form className="review-form stack" onSubmit={approve} noValidate>
            {active ? (
              <ReasonsPanel detail={detail} />
            ) : (
              <div className="callout tone-info" role="status">
                This document was already handled: {statusLabel(detail.status).label}.
              </div>
            )}
            <DetailFields form={form} issues={issues} set={set} />
            <AmountFields form={form} issues={issues} set={set} />
            <LineItemsEditor lines={form.line_items} issues={issues}
              onChange={(lines) => setForm((current) => (current ? { ...current, line_items: lines } : current))} />
            <div className="review-actions">
              {warnings.length > 0 && (
                <ul className="review-warnings" aria-live="polite">
                  {warnings.map((warning) => (
                    <li key={warning.field}>{warning.message}</li>
                  ))}
                </ul>
              )}
              <div className="row">
                <Button type="submit" disabled={!active || busy} icon="check">
                  Approve <kbd>A</kbd>
                </Button>
                <Button variant="danger" disabled={!active || busy} onClick={() => setDismissal("reject")}>
                  Reject <kbd>R</kbd>
                </Button>
                <Button variant="ghost" disabled={!active || busy} onClick={() => setDismissal("duplicate")}>
                  Duplicate <kbd>D</kbd>
                </Button>
                {detail.record.total_amount != null && (
                  <span className="muted review-total">
                    Total {formatMoney(Number(form.total_amount.replace(/,/g, "")) || 0, form.currency || null)}
                  </span>
                )}
              </div>
            </div>
          </form>
        </div>
      )}

      <Dialog open={dismissal !== null} onClose={() => setDismissal(null)} title={dismissal ? DISMISSALS[dismissal].title : ""}>
        {dismissal && (
          <div className="stack" style={{ gap: 16 }}>
            <p className="muted">{DISMISSALS[dismissal].text}</p>
            <div className="field">
              <label className="field-label" htmlFor="dismiss-note">Note (optional)</label>
              <textarea id="dismiss-note" className="input" rows={3} value={note} onChange={(event) => setNote(event.target.value)} />
            </div>
            <div className="row" style={{ justifyContent: "flex-end" }}>
              <Button variant="ghost" onClick={() => setDismissal(null)}>Cancel</Button>
              <Button variant={dismissal === "reject" ? "danger" : "primary"} onClick={confirmDismissal}>
                {DISMISSALS[dismissal].action}
              </Button>
            </div>
          </div>
        )}
      </Dialog>
    </div>
  );
}
