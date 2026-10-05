import { useCallback, useEffect, useState, type FormEvent } from "react";
import { Link, useParams } from "react-router";

import { Dialog } from "../components/Dialog";
import { Icon } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { SpendChart } from "../components/SpendChart";
import { useToast } from "../components/Toast";
import { Button, buttonClass, Card, EmptyState, SelectField, Skeleton, Stat, TextField } from "../components/ui";
import { api } from "../lib/api";
import { COMMON_CURRENCIES, formatDate } from "../lib/format";
import { monthLabel, plural } from "../lib/overview";
import { totalsLabel, type Vendor, type VendorDetail } from "../lib/vendors";

function EditVendor({ vendor, onSaved, onClose }: { vendor: VendorDetail; onSaved: (next: VendorDetail) => void; onClose: () => void }) {
  const [name, setName] = useState(vendor.name);
  const [taxId, setTaxId] = useState(vendor.tax_id ?? "");
  const [currency, setCurrency] = useState(vendor.default_currency ?? "");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const code = currency.trim().toUpperCase();
    if (code && !/^[A-Z]{3}$/.test(code)) {
      setError("Use a three-letter currency code, such as BDT or USD, or leave it empty.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      onSaved(await api.updateVendor(vendor.id, { name: name.trim(), tax_id: taxId.trim() || null, default_currency: code || null }));
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : "The change could not be saved.");
    } finally {
      setSaving(false);
    }
  }

  return (
    <form className="stack" style={{ gap: 16 }} onSubmit={save}>
      <TextField label="Name" value={name} required maxLength={200} onChange={(event) => setName(event.target.value)}
        hint="Shown everywhere. The old name stays as an earlier spelling, so its documents still link here." />
      <TextField label="Tax ID or BIN" value={taxId} maxLength={40} onChange={(event) => setTaxId(event.target.value)} />
      <TextField label="Usual currency" value={currency} maxLength={3} list="vendor-currency-codes" autoComplete="off"
        spellCheck={false} onChange={(event) => setCurrency(event.target.value.toUpperCase())} />
      <datalist id="vendor-currency-codes">
        {COMMON_CURRENCIES.map((option) => (
          <option key={option} value={option} />
        ))}
      </datalist>
      {error && (
        <p className="field-error" role="alert">
          {error}
        </p>
      )}
      <div className="row" style={{ justifyContent: "flex-end" }}>
        <Button variant="ghost" onClick={onClose}>
          Cancel
        </Button>
        <Button type="submit" disabled={saving || !name.trim()}>
          {saving ? "Saving" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}

function MergeInto({ vendor, onMerged }: { vendor: VendorDetail; onMerged: (next: VendorDetail) => void }) {
  const toast = useToast();
  const [others, setOthers] = useState<Vendor[]>([]);
  const [chosen, setChosen] = useState("");
  const [confirming, setConfirming] = useState(false);
  const [merging, setMerging] = useState(false);
  const other = others.find((candidate) => String(candidate.id) === chosen);

  useEffect(() => {
    api.vendors().then(
      (response) => setOthers(response.vendors.filter((candidate) => candidate.id !== vendor.id)),
      () => setOthers([]),
    );
  }, [vendor.id]);

  async function merge() {
    if (!other) {
      return;
    }
    setMerging(true);
    try {
      onMerged(await api.mergeVendors(vendor.id, [other.id]));
      toast({ tone: "success", title: `${other.name} merged into ${vendor.name}` });
      setOthers((current) => current.filter((candidate) => candidate.id !== other.id));
      setChosen("");
      setConfirming(false);
    } catch (failure) {
      toast({ tone: "danger", title: "Not merged", description: failure instanceof Error ? failure.message : undefined });
    } finally {
      setMerging(false);
    }
  }

  if (others.length === 0) {
    return null;
  }
  return (
    <Card>
      <h2 className="card-title">Merge another vendor into this one</h2>
      <p className="muted chart-subtitle">Its records and spellings move to {vendor.name}, which keeps its name.</p>
      <div className="row merge-picker">
        <SelectField
          label="Vendor to merge"
          value={chosen}
          onChange={(event) => setChosen(event.target.value)}
          options={[
            { value: "", label: "Choose a vendor" },
            ...others.map((candidate) => ({ value: String(candidate.id), label: `${candidate.name} (${plural(candidate.records, "record")})` })),
          ]}
        />
        <Button variant="tinted" disabled={!other} onClick={() => setConfirming(true)}>
          Merge
        </Button>
      </div>
      <Dialog open={confirming && other !== undefined} onClose={() => setConfirming(false)} title="Merge these vendors?">
        {other && (
          <div className="stack" style={{ gap: 16 }}>
            <p>
              {other.name} ({plural(other.records, "record")}) will be merged into <strong>{vendor.name}</strong>. This
              cannot be split apart again automatically.
            </p>
            <div className="row" style={{ justifyContent: "flex-end" }}>
              <Button variant="ghost" onClick={() => setConfirming(false)}>
                Cancel
              </Button>
              <Button onClick={() => void merge()} disabled={merging}>
                {merging ? "Merging" : `Merge into ${vendor.name}`}
              </Button>
            </div>
          </div>
        )}
      </Dialog>
    </Card>
  );
}

export function VendorPage() {
  const { vendorId } = useParams();
  const toast = useToast();
  const [vendor, setVendor] = useState<VendorDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);

  const load = useCallback(() => {
    api.vendor(Number(vendorId)).then(
      (detail) => {
        setVendor(detail);
        setError(null);
      },
      (failure: unknown) => setError(failure instanceof Error ? failure.message : "Unknown error"),
    );
  }, [vendorId]);

  useEffect(load, [load]);

  if (error && !vendor) {
    return (
      <div className="page">
        <EmptyState icon="alert" title="This vendor could not be opened" headingLevel="h1"
          action={<Link to="/vendors" className={buttonClass("tinted")}>All vendors</Link>}>
          {error}
        </EmptyState>
      </div>
    );
  }
  if (!vendor) {
    return (
      <div className="page stack" aria-busy="true" aria-label="Loading vendor">
        <Skeleton width={320} height={36} />
        <Skeleton height={240} />
      </div>
    );
  }

  const details = [vendor.tax_id ? `Tax ID or BIN ${vendor.tax_id}` : null, vendor.default_currency ? `Usually ${vendor.default_currency}` : null]
    .filter(Boolean)
    .join(". ");

  return (
    <div className="page stack">
      <Link to="/vendors" className="back-link">
        <Icon name="arrowLeft" size={16} /> Vendors
      </Link>
      <PageHeader
        eyebrow="Vendor"
        title={vendor.name}
        subtitle={details || "No tax ID or usual currency recorded yet."}
        actions={
          <>
            <Button variant="tinted" icon="edit" onClick={() => setEditing(true)}>
              Edit
            </Button>
            <Link to={`/records?vendor=${encodeURIComponent(vendor.name)}`} className={buttonClass("primary")}>
              See records <Icon name="arrowRight" size={16} />
            </Link>
          </>
        }
      />
      <div className="grid-cards">
        <Stat label="Records" value={vendor.records} hint={vendor.aliases.length ? `Also printed as ${vendor.aliases.join(", ")}` : "One spelling so far"} />
        <Stat label="Total spend" value={<span className="vendor-spend">{totalsLabel(vendor.totals)}</span>} hint="Every currency kept separate" />
        <Stat label="First invoice" value={<span className="vendor-date">{formatDate(vendor.first_invoice_date) ?? "None"}</span>} />
        <Stat label="Last invoice" value={<span className="vendor-date">{formatDate(vendor.last_invoice_date) ?? "None"}</span>} />
      </div>
      {vendor.currency ? (
        <Card className="chart-card">
          <h2 className="card-title">Spending by month</h2>
          <p className="muted chart-subtitle">
            {vendor.currency}, by invoice date, {monthLabel(vendor.months[0].month, true)} to {monthLabel(vendor.months.at(-1)!.month, true)}
          </p>
          <SpendChart months={vendor.months} currency={vendor.currency} />
        </Card>
      ) : null}
      <MergeInto vendor={vendor} onMerged={setVendor} />
      <Dialog open={editing} onClose={() => setEditing(false)} title={`Edit ${vendor.name}`}>
        {editing && (
          <EditVendor
            vendor={vendor}
            onClose={() => setEditing(false)}
            onSaved={(next) => {
              setVendor(next);
              setEditing(false);
              toast({ tone: "success", title: "Vendor saved" });
            }}
          />
        )}
      </Dialog>
    </div>
  );
}
