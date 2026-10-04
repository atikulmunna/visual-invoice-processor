import type { ReactNode } from "react";

import { Icon } from "./Icon";
import { buttonClass, Card } from "./ui";

interface PageHeaderProps {
  eyebrow: string;
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
}

export function PageHeader({ eyebrow, title, subtitle, actions }: PageHeaderProps) {
  return (
    <div className="page-header">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1 className="page-title">{title}</h1>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      {actions && <div className="row">{actions}</div>}
    </div>
  );
}

/** Shown on screens that are still being rebuilt, pointing to the working classic screen. */
export function RebuildNotice({ what }: { what: string }) {
  return (
    <Card dark>
      <div className="row" style={{ justifyContent: "space-between", gap: 20 }}>
        <div style={{ maxWidth: 560 }}>
          <div className="eyebrow" style={{ color: "#9db0ff" }}>
            Being rebuilt
          </div>
          <p className="card-title" style={{ marginTop: 8 }}>
            The new {what} lands in an upcoming release.
          </p>
          <p className="muted" style={{ marginTop: 6 }}>
            Everything still works in the classic workspace, and your data is the same in both.
          </p>
        </div>
        <a className={buttonClass("light")} href="/dashboard">
          Open classic workspace <Icon name="arrowRight" size={16} />
        </a>
      </div>
    </Card>
  );
}
