-- Vendor master: one row per supplier of an organization, so every spelling of a vendor's
-- name adds up to the same vendor. Records link to a vendor when they are stored.
-- Apply after 006_org_base_currency.sql. Existing records are linked afterwards with
-- `python -m app.alpha_admin vendors-link`.

create table if not exists public.vendors (
  id bigserial primary key,
  org_id uuid not null references public.organizations(id) on delete cascade,
  name text not null,
  tax_id text,
  default_currency text check (default_currency ~ '^[A-Z]{3}$'),
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now()
);

create index if not exists vendors_org_id_idx on public.vendors(org_id);
create index if not exists vendors_org_tax_id_idx on public.vendors(org_id, tax_id) where tax_id is not null;

-- Every name a vendor has appeared under, normalized for matching. One vendor per normalized
-- name per organization, so two spellings can never point at different vendors.
create table if not exists public.vendor_aliases (
  org_id uuid not null references public.organizations(id) on delete cascade,
  normalized text not null,
  vendor_id bigint not null references public.vendors(id) on delete cascade,
  alias text not null,
  primary key (org_id, normalized)
);

create index if not exists vendor_aliases_vendor_id_idx on public.vendor_aliases(vendor_id);

alter table public.ledger_records
  add column if not exists vendor_id bigint references public.vendors(id) on delete set null;

create index if not exists ledger_records_org_vendor_idx on public.ledger_records(org_id, vendor_id);

-- Same lockdown as 003 for the new tables.
alter table public.vendors enable row level security;
alter table public.vendor_aliases enable row level security;

revoke all privileges on table public.vendors, public.vendor_aliases from anon, authenticated;
revoke all privileges on sequence public.vendors_id_seq from anon, authenticated;
