import { useEffect, useState, type ReactNode } from "react";
import { Link, useSearchParams } from "react-router";

import { Icon, type IconName } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { SpendChart } from "../components/SpendChart";
import { Button, buttonClass, Card, Chip, EmptyState, Skeleton, Stat } from "../components/ui";
import { api } from "../lib/api";
import { formatAmount, formatMoney } from "../lib/format";
import {
  monthLabel,
  plural,
  spendSummary,
  spotlightCopy,
  type Overview,
  type Spotlight,
  type VendorTotal,
} from "../lib/overview";
import { useSession } from "../shell/session";

function Money({ amount, currency }: { amount: number; currency: string | null }) {
  return (
    <span className="money">
      {currency && <span className="money-code">{currency}</span>}
      {formatAmount(amount, currency)}
    </span>
  );
}

const SPOTLIGHT_ICONS: Record<Spotlight["kind"], IconName> = {
  files_expiring: "clock",
  uploads_failed: "alert",
  review_waiting: "review",
  records_flagged: "records",
};

function SpotlightPanel({ overview }: { overview: Overview }) {
  const copy = spotlightCopy(overview.spotlight);
  return (
    <section className="spotlight" data-urgent={copy.urgent || undefined} aria-labelledby="spotlight-title">
      <div className="spotlight-art" aria-hidden="true">
        <Icon name={overview.spotlight ? SPOTLIGHT_ICONS[overview.spotlight.kind] : "check"} size={40} />
      </div>
      <div className="spotlight-copy">
        <div className="eyebrow">{copy.eyebrow}</div>
        <h2 id="spotlight-title" className="spotlight-title">
          {copy.title}
        </h2>
        <p className="spotlight-body">{copy.body}</p>
        <Link to={copy.action.to} className={buttonClass("glass")}>
          {copy.action.label} <Icon name="arrowRight" size={16} />
        </Link>
      </div>
    </section>
  );
}

function VendorBars({ vendors, currency }: { vendors: VendorTotal[]; currency: string }) {
  const largest = Math.max(0, ...vendors.map((vendor) => vendor.total));
  return (
    <ol className="vendor-bars">
      {vendors.map((vendor) => (
        <li key={vendor.vendor_name}>
          <Link
            className="vendor-bar"
            to={`/records?vendor=${encodeURIComponent(vendor.vendor_name)}&currency=${encodeURIComponent(currency)}`}
          >
            <span className="vendor-bar-head">
              <span className="vendor-name">{vendor.vendor_name}</span>
              <span className="vendor-total">{formatMoney(vendor.total, currency)}</span>
            </span>
            <span className="vendor-fill" style={{ width: `${largest ? (vendor.total / largest) * 100 : 0}%` }} aria-hidden="true" />
            <span className="muted vendor-count">{plural(vendor.count, "record")}</span>
          </Link>
        </li>
      ))}
    </ol>
  );
}

function ChartCard({ title, subtitle, children }: { title: string; subtitle: string; children: ReactNode }) {
  return (
    <Card className="chart-card">
      <h2 className="card-title">{title}</h2>
      <p className="muted chart-subtitle">{subtitle}</p>
      {children}
    </Card>
  );
}

function Stats({ overview, reviewCount }: { overview: Overview; reviewCount: number | null }) {
  const { thisMonth, lastMonth, yearTotal, yearCount } = spendSummary(overview.months);
  const currency = overview.currency;
  return (
    <div className="grid-cards">
      <Stat
        label="Spent this month"
        value={<Money amount={thisMonth?.total ?? 0} currency={currency} />}
        hint={
          lastMonth
            ? `${monthLabel(lastMonth.month, true)}: ${formatMoney(lastMonth.total, currency)}`
            : "By invoice date"
        }
      />
      <Stat
        label="Last 12 months"
        value={<Money amount={yearTotal} currency={currency} />}
        hint={`${plural(yearCount, "record")} by invoice date`}
      />
      <Stat
        label="Records stored"
        value={overview.records_total.toLocaleString("en-US")}
        hint={
          overview.flagged_total ? (
            <Link to="/records?flagged=1">{plural(overview.flagged_total, "flagged record")}</Link>
          ) : (
            <Link to="/records">None flagged by checks</Link>
          )
        }
      />
      <Stat
        label="Needs review"
        value={reviewCount ?? "?"}
        hint={reviewCount ? <Link to="/review">See what needs attention</Link> : "Nothing is waiting on you."}
      />
    </div>
  );
}

function Loading() {
  return (
    <div className="stack" aria-busy="true" aria-label="Loading overview">
      <Skeleton height={180} />
      <div className="grid-cards">
        {[0, 1, 2, 3].map((index) => (
          <Skeleton key={index} height={120} />
        ))}
      </div>
    </div>
  );
}

export function OverviewPage() {
  const { me, reviewCount } = useSession();
  const [params, setParams] = useSearchParams();
  const requested = params.get("currency");
  const [loaded, setLoaded] = useState<{ currency: string | null; overview: Overview } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    api.overview(requested).then(
      (overview) => !cancelled && setLoaded({ currency: requested, overview }),
      (failure: unknown) => !cancelled && setError(failure instanceof Error ? failure.message : "Unknown error"),
    );
    return () => {
      cancelled = true;
    };
  }, [requested, attempt]);

  const overview = loaded?.overview ?? null;
  const currency = overview?.currency ?? null;

  function chooseCurrency(code: string) {
    setParams(code === me.organization?.base_currency ? {} : { currency: code }, { replace: true });
  }

  return (
    <div className="page stack">
      <PageHeader
        eyebrow={me.organization?.name ?? "Workspace"}
        title="Overview"
        subtitle="Where your documents and spending stand today."
        actions={
          <Link to="/upload" className={buttonClass("primary")}>
            <Icon name="upload" size={16} /> Upload documents
          </Link>
        }
      />
      {error && !overview && (
        <EmptyState icon="alert" title="The overview could not load"
          action={<Button onClick={() => setAttempt((n) => n + 1)}>Try again</Button>}>
          {error}
        </EmptyState>
      )}
      {!error && !overview && <Loading />}
      {error && overview && (
        <div className="callout tone-danger row" role="alert">
          <Icon name="alert" size={18} />
          <span>The overview could not be updated: {error}</span>
          <Button size="sm" variant="ghost" onClick={() => setAttempt((n) => n + 1)}>
            Try again
          </Button>
        </div>
      )}
      {overview && (
        <div className="stack" data-refreshing={loaded?.currency !== requested || undefined}>
          <SpotlightPanel overview={overview} />
          {overview.currencies.length > 1 && (
            <div className="row" role="group" aria-label="Currency for totals">
              <span className="muted">Totals in</span>
              {overview.currencies.map((code) => (
                <Chip key={code} pressed={code === currency} onToggle={() => chooseCurrency(code)}>
                  {code}
                </Chip>
              ))}
            </div>
          )}
          <Stats overview={overview} reviewCount={reviewCount} />
          {overview.records_total === 0 || !currency ? (
            <Card>
              <EmptyState icon="overview" title="No spending to show yet"
                action={<Link to="/upload" className={buttonClass("tinted")}>Upload documents</Link>}>
                Monthly totals and top vendors appear here once invoices are stored.
              </EmptyState>
            </Card>
          ) : (
            <div className="overview-grid">
              <ChartCard
                title="Spending by month"
                subtitle={`${currency}, by invoice date, ${monthLabel(overview.months[0].month, true)} to ${monthLabel(overview.months.at(-1)!.month, true)}`}
              >
                <SpendChart months={overview.months} currency={currency} />
              </ChartCard>
              <ChartCard title="Top vendors" subtitle={`${currency}, last 12 months`}>
                {overview.top_vendors.length ? (
                  <VendorBars vendors={overview.top_vendors} currency={currency} />
                ) : (
                  <p className="muted">No {currency} invoices in the last 12 months.</p>
                )}
              </ChartCard>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
