import { Link } from "react-router";

import { Icon } from "../components/Icon";
import { PageHeader, RebuildNotice } from "../components/PageHeader";
import { buttonClass, Stat } from "../components/ui";
import { useSession } from "../shell/session";

export function OverviewPage() {
  const { me, reviewCount } = useSession();
  const organization = me.organization;

  return (
    <div className="page stack">
      <PageHeader
        eyebrow={organization?.name ?? "Workspace"}
        title="Overview"
        subtitle="Where your documents stand today."
        actions={
          <Link to="/upload" className={buttonClass("primary")}>
            <Icon name="upload" size={16} /> Upload documents
          </Link>
        }
      />
      <div className="grid-cards">
        <Stat
          label="Needs review"
          value={reviewCount ?? "?"}
          hint={reviewCount ? <Link to="/review">See what needs attention</Link> : "Nothing is waiting on you."}
        />
        {me.documents_remaining !== null && (
          <Stat
            label="Uploads left"
            value={me.documents_remaining}
            hint={`of ${me.document_limit} in this private alpha`}
          />
        )}
        {organization && (
          <Stat label="Base currency" value={organization.base_currency} hint={<Link to="/settings">Used when a document shows none</Link>} />
        )}
      </div>
      <RebuildNotice what="overview with spending trends and vendor totals" />
    </div>
  );
}
