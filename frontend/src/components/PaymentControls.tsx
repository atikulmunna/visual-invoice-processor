import { useState } from "react";

import { api } from "../lib/api";
import { localToday, paymentState, type Payment, type PaymentStatus } from "../lib/payables";
import { useSession } from "../shell/session";
import { useToast } from "./Toast";
import { Badge, Button } from "./ui";

interface PaymentControlsProps {
  recordId: number;
  payment: Payment;
  onChange: (payment: Payment) => void;
}

const DONE: Record<PaymentStatus, string> = {
  paid: "Marked as paid",
  scheduled: "Payment scheduled",
  unpaid: "Marked as unpaid",
};

/** Where a bill stands for payment, and the buttons to mark it paid, schedule it, or reopen it. */
export function PaymentControls({ recordId, payment, onChange }: PaymentControlsProps) {
  const toast = useToast();
  const { refresh } = useSession();
  const [busy, setBusy] = useState(false);
  const [planning, setPlanning] = useState(false);
  const today = localToday();
  const [planned, setPlanned] = useState(payment.date ?? today);
  const state = paymentState(payment, today);

  async function change(status: PaymentStatus, paymentDate?: string) {
    setBusy(true);
    try {
      const updated = await api.setPayment(recordId, { status, payment_date: paymentDate });
      toast({ tone: "success", title: DONE[status] });
      setPlanning(false);
      onChange(updated);
      void refresh(); // The overdue count in the sidebar may have changed.
    } catch (error) {
      toast({ tone: "danger", title: "Not saved", description: error instanceof Error ? error.message : undefined });
    } finally {
      setBusy(false);
    }
  }

  if (planning) {
    return (
      <form
        className="row payment-controls"
        onSubmit={(event) => {
          event.preventDefault();
          void change("scheduled", planned);
        }}
      >
        <label className="visually-hidden" htmlFor={`plan-${recordId}`}>Planned payment date</label>
        <input id={`plan-${recordId}`} className="input input-date" type="date" required min={today} value={planned}
          onChange={(event) => setPlanned(event.target.value)} />
        <Button size="sm" type="submit" disabled={busy || !planned}>Save</Button>
        <Button size="sm" variant="ghost" onClick={() => setPlanning(false)}>Cancel</Button>
      </form>
    );
  }

  return (
    <div className="row payment-controls">
      <Badge tone={state.tone}>{state.label}</Badge>
      {payment.status === "paid" ? (
        <Button size="sm" variant="ghost" disabled={busy} onClick={() => void change("unpaid")}>Mark unpaid</Button>
      ) : (
        <>
          <Button size="sm" variant="tinted" icon="check" disabled={busy} onClick={() => void change("paid")}>
            Mark paid
          </Button>
          <Button size="sm" variant="ghost" icon="calendar" disabled={busy} onClick={() => setPlanning(true)}>
            {payment.status === "scheduled" ? "Reschedule" : "Schedule"}
          </Button>
          {payment.status === "scheduled" && (
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => void change("unpaid")}>Unschedule</Button>
          )}
        </>
      )}
    </div>
  );
}
