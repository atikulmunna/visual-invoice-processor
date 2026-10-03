-- Each organization keeps its books in a base currency. Documents that show no readable
-- currency fall back to it, and the stored record is flagged with currency_assumed.
-- Apply after 005_organizations.sql.

alter table public.organizations
  add column if not exists base_currency text not null default 'BDT'
  check (base_currency ~ '^[A-Z]{3}$');
