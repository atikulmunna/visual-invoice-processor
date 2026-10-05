import { useState, type FormEvent } from "react";

import { PageHeader } from "../components/PageHeader";
import { useToast } from "../components/Toast";
import { Button, Card, TextField } from "../components/ui";
import { api } from "../lib/api";
import { COMMON_CURRENCIES } from "../lib/format";
import { useSession } from "../shell/session";

function OrganizationSettings() {
  const { me, refresh } = useSession();
  const toast = useToast();
  const organization = me.organization!;
  const isOwner = organization.role === "owner";
  const [currency, setCurrency] = useState(organization.base_currency);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const code = currency.trim().toUpperCase();

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!/^[A-Z]{3}$/.test(code)) {
      setError("Use a three-letter currency code, such as BDT or USD.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await api.updateOrganization({ base_currency: code });
      await refresh();
      toast({
        tone: "success",
        title: "Base currency saved",
        description: `Documents that show no currency will now use ${code}.`,
      });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The change could not be saved.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card>
      <h2 className="card-title">{organization.name}</h2>
      <p className="muted" style={{ marginTop: 4 }}>
        You are {isOwner ? "an owner" : "a member"} of this organization.
      </p>
      <form onSubmit={save} className="stack" style={{ marginTop: 24, maxWidth: 420, gap: 16 }}>
        <TextField
          label="Base currency"
          value={currency}
          onChange={(event) => setCurrency(event.target.value.toUpperCase())}
          list="currency-codes"
          maxLength={3}
          autoComplete="off"
          spellCheck={false}
          disabled={!isOwner}
          error={error}
          hint={
            isOwner
              ? "Used when a document shows no readable currency. Those records are marked as assumed."
              : "Only owners can change this."
          }
        />
        <datalist id="currency-codes">
          {COMMON_CURRENCIES.map((option) => (
            <option key={option} value={option} />
          ))}
        </datalist>
        {isOwner && (
          <div>
            <Button type="submit" disabled={saving || code === organization.base_currency}>
              {saving ? "Saving" : "Save changes"}
            </Button>
          </div>
        )}
      </form>
    </Card>
  );
}

export function SettingsPage() {
  const { me } = useSession();

  return (
    <div className="page stack">
      <PageHeader eyebrow="Settings" title="Settings" subtitle="Your organization and account." />
      {me.organization ? (
        <OrganizationSettings />
      ) : (
        <Card>
          <p className="muted">This account is not part of an organization, so there is nothing to configure.</p>
        </Card>
      )}
      <Card>
        <h2 className="card-title">Account</h2>
        <dl className="row" style={{ marginTop: 16, gap: 32 }}>
          <div>
            <dt className="stat-label">Username</dt>
            <dd style={{ margin: "6px 0 0", fontWeight: 700 }}>{me.username}</dd>
          </div>
          {me.documents_remaining !== null && (
            <div>
              <dt className="stat-label">Uploads left</dt>
              <dd style={{ margin: "6px 0 0", fontWeight: 700 }}>
                {me.documents_remaining} of {me.document_limit}
              </dd>
            </div>
          )}
        </dl>
      </Card>
    </div>
  );
}
