import { useState } from "react";

import { downloadFile } from "../lib/api";
import { useMenu } from "../lib/menu";
import { plural } from "../lib/overview";
import { recordsExportPath, type ExportFormat, type RecordQuery } from "../lib/records";
import { Icon } from "./Icon";
import { useToast } from "./Toast";
import { Button } from "./ui";

const CHOICES: { format: ExportFormat; label: string; hint: string }[] = [
  { format: "xlsx", label: "Excel workbook", hint: "Records and line items on two sheets" },
  { format: "records-csv", label: "Records (CSV)", hint: "One row per invoice or receipt" },
  { format: "line-items-csv", label: "Line items (CSV)", hint: "One row per line, with its invoice" },
];

/** Downloads the records list as it is filtered and sorted now, every page of it. */
export function ExportMenu({ query, total }: { query: RecordQuery; total: number | null }) {
  const { open, setOpen, containerRef } = useMenu();
  const [busy, setBusy] = useState(false);
  const toast = useToast();

  async function download(format: ExportFormat) {
    setOpen(false);
    setBusy(true);
    try {
      await downloadFile(recordsExportPath(query, format));
    } catch (error) {
      toast({ tone: "danger", title: "Export failed", description: error instanceof Error ? error.message : undefined });
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="menu" ref={containerRef}>
      <Button
        variant="tinted"
        icon="download"
        aria-expanded={open}
        aria-controls="export-menu"
        disabled={busy || !total}
        onClick={() => setOpen((value) => !value)}
      >
        {busy ? "Exporting" : "Export"}
      </Button>
      {open && total ? (
        <div className="menu-panel export-panel" id="export-menu">
          <p className="menu-note">All {plural(total, "record")} in this view, not only this page.</p>
          {CHOICES.map((choice) => (
            <button key={choice.format} type="button" className="menu-item" onClick={() => void download(choice.format)}>
              <Icon name="download" />
              <span className="menu-item-text">
                {choice.label}
                <small>{choice.hint}</small>
              </span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
