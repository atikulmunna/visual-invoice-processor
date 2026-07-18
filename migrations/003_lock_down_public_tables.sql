-- The backend connects as the database owner. Browser-facing Supabase roles do not
-- need direct table or view access for this private-alpha application.

alter table public.ledger_records enable row level security;
alter table public.review_queue_items enable row level security;
alter table public.alpha_users enable row level security;
alter table public.processing_jobs enable row level security;
alter table public.document_claims enable row level security;
alter table public.processing_events enable row level security;
alter table public.alpha_budget enable row level security;

alter view public.ledger_records_flat set (security_invoker = true);
alter view public.ledger_line_items_flat set (security_invoker = true);
alter view public.ledger_daily_summary set (security_invoker = true);

revoke all privileges on table
  public.ledger_records,
  public.review_queue_items,
  public.alpha_users,
  public.processing_jobs,
  public.document_claims,
  public.processing_events,
  public.alpha_budget,
  public.schema_migrations,
  public.ledger_records_flat,
  public.ledger_line_items_flat,
  public.ledger_daily_summary
from anon, authenticated;

revoke all privileges on sequence
  public.ledger_records_id_seq,
  public.processing_events_id_seq
from anon, authenticated;
