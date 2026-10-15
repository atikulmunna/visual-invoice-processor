-- The backend connects as the database owner. Row-level security with no policies means
-- no other role can read or change these tables, and the views run with the caller's rights.

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
