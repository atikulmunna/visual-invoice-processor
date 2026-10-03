-- Organizations own every job, ledger record, review item, and dedupe claim.
-- Apply after 004_alpha_sessions.sql.

create table if not exists public.organizations (
  id uuid primary key,
  name text not null check (length(btrim(name)) between 1 and 120),
  created_at_utc timestamptz not null default now()
);

create table if not exists public.memberships (
  org_id uuid not null references public.organizations(id) on delete cascade,
  user_id uuid not null references public.alpha_users(id) on delete cascade,
  role text not null default 'member' check (role in ('owner', 'member')),
  created_at_utc timestamptz not null default now(),
  primary key (org_id, user_id)
);

create index if not exists memberships_user_id_idx
  on public.memberships(user_id, created_at_utc);

-- Every existing tester gets a personal organization. Reusing the user id as the
-- organization id keeps the backfill below deterministic.
insert into public.organizations (id, name)
select u.id, u.username
from public.alpha_users u
where not exists (select 1 from public.memberships m where m.user_id = u.id)
on conflict (id) do nothing;

insert into public.memberships (org_id, user_id, role)
select u.id, u.id, 'owner'
from public.alpha_users u
where not exists (select 1 from public.memberships m where m.user_id = u.id)
  and exists (select 1 from public.organizations o where o.id = u.id)
on conflict (org_id, user_id) do nothing;

-- Sessions remember the active organization. Null falls back to the user's first membership.
alter table public.alpha_sessions
  add column if not exists org_id uuid references public.organizations(id) on delete cascade;

-- Jobs always belong to an organization.
alter table public.processing_jobs
  add column if not exists org_id uuid references public.organizations(id);

update public.processing_jobs j
set org_id = j.user_id
where j.org_id is null
  and exists (select 1 from public.organizations o where o.id = j.user_id);

alter table public.processing_jobs alter column org_id set not null;

create index if not exists processing_jobs_org_id_idx
  on public.processing_jobs(org_id, authorized_at_utc desc);

-- Ledger records and review items link to their organization through the job that
-- produced them. Rows without a job (from retired ingestion paths) stay unowned, so
-- no organization can see them until an administrator assigns them.
alter table public.ledger_records
  add column if not exists org_id uuid references public.organizations(id);

update public.ledger_records r
set org_id = j.org_id
from public.processing_jobs j
where r.org_id is null
  and j.object_key = r.drive_file_id;

create index if not exists ledger_records_org_id_idx
  on public.ledger_records(org_id, processed_at_utc desc);

alter table public.review_queue_items
  add column if not exists org_id uuid references public.organizations(id);

update public.review_queue_items r
set org_id = j.org_id
from public.processing_jobs j
where r.org_id is null
  and j.object_key = coalesce(r.metadata_json ->> 'source_file_id', r.metadata_json ->> 'drive_file_id');

create index if not exists review_queue_items_org_id_idx
  on public.review_queue_items(org_id, status, created_at_utc);

-- Deduplication is per organization, so one tenant's uploads never reveal another's.
alter table public.document_claims
  add column if not exists org_id uuid references public.organizations(id);

update public.document_claims d
set org_id = j.org_id
from public.processing_jobs j
where d.org_id is null
  and j.object_key = d.source_id;

-- Claims without a job only block reprocessing; they carry no record data.
delete from public.document_claims where org_id is null;

alter table public.document_claims alter column org_id set not null;
alter table public.document_claims drop constraint if exists document_claims_pkey;
alter table public.document_claims add primary key (org_id, file_hash);

-- Expose org_id on the analytics views. New columns must come last.
create or replace view public.ledger_records_flat as
select
  lr.id,
  lr.processed_at_utc,
  lr.status as row_status,
  lr.drive_file_id,
  lr.file_hash,
  lr.record_json ->> 'document_type' as document_type,
  lr.record_json ->> 'vendor_name' as vendor_name,
  lr.record_json ->> 'vendor_tax_id' as vendor_tax_id,
  lr.record_json ->> 'invoice_number' as invoice_number,
  lr.record_json ->> 'invoice_date' as invoice_date,
  lr.record_json ->> 'due_date' as due_date,
  lr.record_json ->> 'currency' as currency,
  (lr.record_json ->> 'subtotal')::numeric as subtotal,
  (lr.record_json ->> 'tax_amount')::numeric as tax_amount,
  (lr.record_json ->> 'total_amount')::numeric as total_amount,
  lr.record_json ->> 'payment_method' as payment_method,
  (lr.record_json ->> 'model_confidence')::numeric as model_confidence,
  (lr.record_json ->> 'validation_score')::numeric as validation_score,
  coalesce((lr.record_json ->> 'needs_review')::boolean, false) as needs_review,
  lr.metadata_json ->> 'document_id' as document_id,
  lr.metadata_json ->> 'used_provider' as used_provider,
  lr.org_id
from public.ledger_records lr;

create or replace view public.ledger_line_items_flat as
select
  lr.id as ledger_id,
  lr.metadata_json ->> 'document_id' as document_id,
  lr.drive_file_id,
  lr.record_json ->> 'vendor_name' as vendor_name,
  lr.record_json ->> 'invoice_number' as invoice_number,
  lr.record_json ->> 'invoice_date' as invoice_date,
  lr.record_json ->> 'currency' as currency,
  li.ordinality as line_no,
  li.item ->> 'description' as description,
  (li.item ->> 'quantity')::numeric as quantity,
  (li.item ->> 'unit_price')::numeric as unit_price,
  (li.item ->> 'line_total')::numeric as line_total,
  li.item ->> 'category' as category,
  lr.org_id
from public.ledger_records lr
cross join lateral jsonb_array_elements(lr.record_json -> 'line_items') with ordinality as li(item, ordinality);

alter view public.ledger_records_flat set (security_invoker = true);
alter view public.ledger_line_items_flat set (security_invoker = true);

-- Same lockdown as 003 for the new tables.
alter table public.organizations enable row level security;
alter table public.memberships enable row level security;

revoke all privileges on table public.organizations, public.memberships from anon, authenticated;
