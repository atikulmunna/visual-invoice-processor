-- Durable state for the private-alpha serverless deployment.
-- Apply after 001_analytics_views.sql.

create table if not exists public.alpha_users (
  id uuid primary key,
  username text not null unique check (username = lower(username)),
  password_hash text not null,
  is_active boolean not null default true,
  document_limit integer not null default 20 check (document_limit > 0),
  documents_used integer not null default 0 check (documents_used >= 0),
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create table if not exists public.processing_jobs (
  id uuid primary key,
  user_id uuid not null references public.alpha_users(id),
  object_key text not null unique,
  original_name text not null,
  content_type text not null,
  declared_size bigint not null check (declared_size > 0),
  status text not null default 'AUTHORIZED' check (
    status in ('AUTHORIZED', 'PROCESSING', 'STORED', 'REVIEW_REQUIRED', 'DUPLICATE', 'REJECTED', 'FAILED')
  ),
  attempts integer not null default 0,
  page_count integer,
  document_id text,
  result_json jsonb,
  error_code text,
  error_message text,
  authorized_at_utc timestamptz not null default now(),
  started_at_utc timestamptz,
  completed_at_utc timestamptz,
  updated_at_utc timestamptz not null default now()
);

create index if not exists processing_jobs_user_id_idx
  on public.processing_jobs(user_id, authorized_at_utc desc);

create table if not exists public.document_claims (
  file_hash text primary key,
  source_id text not null,
  status text not null,
  owner_id text,
  claimed_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create table if not exists public.processing_events (
  id bigserial primary key,
  job_id uuid,
  event_type text not null,
  payload_json jsonb not null,
  recorded_at_utc timestamptz not null default now()
);

create table if not exists public.alpha_budget (
  id smallint primary key check (id = 1),
  page_attempts integer not null default 0 check (page_attempts >= 0),
  updated_at_utc timestamptz not null default now()
);

insert into public.alpha_budget(id, page_attempts)
values (1, 0)
on conflict (id) do nothing;

