-- Opaque, server-side sessions for the private-alpha browser login.

create table if not exists public.alpha_sessions (
  token_hash text primary key check (length(token_hash) = 64),
  user_id uuid not null references public.alpha_users(id) on delete cascade,
  created_at_utc timestamptz not null default now(),
  expires_at_utc timestamptz not null
);

create index if not exists alpha_sessions_user_expiry_idx
  on public.alpha_sessions(user_id, expires_at_utc desc);

alter table public.alpha_sessions enable row level security;

revoke all privileges on table public.alpha_sessions from anon, authenticated;
