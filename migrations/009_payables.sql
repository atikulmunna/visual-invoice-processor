-- Payables: every stored bill is unpaid, scheduled, or paid. Overdue is never stored; it is
-- an unpaid bill past its due date. payment_date is the planned date while scheduled and
-- the day it was paid once paid. Apply after 008_drop_placeholder_roles.sql.

alter table public.ledger_records
  add column if not exists payment_status text not null default 'unpaid'
    check (payment_status in ('unpaid', 'scheduled', 'paid')),
  add column if not exists payment_date date;

alter table public.ledger_records drop constraint if exists ledger_records_payment_date_check;
alter table public.ledger_records add constraint ledger_records_payment_date_check
  check ((payment_status = 'unpaid') = (payment_date is null));

-- A receipt is proof of payment, so existing receipts count as paid on their own date.
update public.ledger_records
set payment_status = 'paid', payment_date = (record_json ->> 'invoice_date')::date
where payment_status = 'unpaid'
  and record_json ->> 'document_type' = 'receipt'
  and record_json ->> 'invoice_date' ~ '^\d{4}-\d{2}-\d{2}$';

create index if not exists ledger_records_org_payment_idx on public.ledger_records(org_id, payment_status);
