-- Base durable tables required by both the legacy worker and private-alpha deployment.

create table if not exists public.ledger_records (
  id bigserial primary key,
  drive_file_id text not null,
  file_hash text not null,
  status text not null,
  record_json jsonb not null,
  metadata_json jsonb not null,
  processed_at_utc timestamptz not null default now(),
  unique (drive_file_id, file_hash)
);

create table if not exists public.review_queue_items (
  document_id text primary key,
  status text not null,
  reason_codes jsonb not null default '[]'::jsonb,
  metadata_json jsonb,
  source_file_moved_to text,
  created_at_utc timestamptz not null default now(),
  resolved_at_utc timestamptz,
  resolved_record jsonb,
  storage_result jsonb,
  resolution_note text
);
