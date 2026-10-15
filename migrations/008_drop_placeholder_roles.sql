-- The lockdown migrations once revoked access from Supabase's browser roles, anon and
-- authenticated. Production now runs on Neon, where those roles were created only so the
-- revokes could run. They have no login and no privileges; remove them where they exist.

do $$
begin
  if exists (select 1 from pg_roles where rolname = 'anon') then
    drop role anon;
  end if;
  if exists (select 1 from pg_roles where rolname = 'authenticated') then
    drop role authenticated;
  end if;
end
$$;
