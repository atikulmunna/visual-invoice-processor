import { useEffect, useRef, useState, type DragEvent } from "react";
import { Link } from "react-router";

import { DataTable, type Column } from "../components/DataTable";
import { Icon } from "../components/Icon";
import { PageHeader } from "../components/PageHeader";
import { Button, buttonClass, EmptyState, StatusBadge, Stepper } from "../components/ui";
import { api } from "../lib/api";
import { formatMoney, formatRelativeTime } from "../lib/format";
import { processingErrorLabel, reasonLabel } from "../lib/labels";
import { formatBytes, stepStates, type JobView, type UploadLimits } from "../lib/uploads";
import { useSession } from "../shell/session";
import { useUploadQueue, type UploadItem } from "../shell/uploadQueue";

const STEP_LABELS = ["Authorize", "Upload", "Extract", "Validate"];

/** One line describing a finished job: what was read, why it needs review, or why it failed. */
function JobOutcome({ job }: { job: JobView }) {
  if (job.status === "DUPLICATE") {
    return <span className="muted">Already processed in your organization.</span>;
  }
  if (job.status === "REJECTED" || job.status === "FAILED") {
    return <span className="outcome-error">{processingErrorLabel(job.error_code, job.error_message)}</span>;
  }
  const summary = job.summary;
  const vendor = summary?.vendor_name ?? "Vendor not found";
  const total =
    summary?.total_amount != null ? formatMoney(summary.total_amount, summary.currency) : null;
  return (
    <span>
      <strong>{vendor}</strong>
      {total && <> · {total}</>}
      {summary?.currency_assumed && <span className="muted"> (currency assumed)</span>}
      {job.status === "REVIEW_REQUIRED" && job.reason_codes.length > 0 && (
        <span className="outcome-reasons">{job.reason_codes.map(reasonLabel).join(". ")}.</span>
      )}
    </span>
  );
}

function progressText(item: UploadItem): string {
  switch (item.phase) {
    case "queued":
      return "Waiting to start";
    case "authorizing":
      return "Requesting a private upload";
    case "uploading":
      return "Uploading to private storage";
    case "processing":
      return item.job?.status === "PROCESSING" ? "Reading the document" : "Waiting for processing to start";
    case "stalled":
      return "Still processing. It will appear under recent uploads when done.";
    default:
      return "";
  }
}

function BatchRow({ item }: { item: UploadItem }) {
  const { retry, dismiss } = useUploadQueue();
  const states = stepStates(item.phase, item.job?.status ?? null, item.failedStep);
  const settled = item.phase === "finished" || item.phase === "failed" || item.phase === "stalled";

  return (
    <li className="batch-row">
      <div className="batch-file">
        <span className="file-tile">
          <Icon name="file" size={20} />
        </span>
        <div className="batch-name">
          <strong title={item.name}>{item.name}</strong>
          <span className="muted">{formatBytes(item.size)}</span>
        </div>
        {item.phase === "finished" && item.job && <StatusBadge status={item.job.status} />}
      </div>
      <Stepper steps={STEP_LABELS.map((label, index) => ({ label, state: states[index] }))} />
      <div className="batch-outcome" aria-live="polite">
        {item.phase === "finished" && item.job ? (
          <JobOutcome job={item.job} />
        ) : item.phase === "failed" ? (
          <span className="outcome-error">{item.error}</span>
        ) : (
          <span className="muted">{progressText(item)}</span>
        )}
      </div>
      {settled && (
        <div className="row batch-actions">
          {item.canRetry && (
            <Button size="sm" variant="tinted" onClick={() => retry(item)}>
              Try again
            </Button>
          )}
          <Button size="sm" variant="ghost" onClick={() => dismiss(item.id)} aria-label={`Remove ${item.name} from this list`}>
            Remove
          </Button>
        </div>
      )}
    </li>
  );
}

function limitsText(limits: UploadLimits | null): string {
  if (!limits) {
    return "PDF, PNG, or JPEG. Files go straight to private storage.";
  }
  return `PDF, PNG, or JPEG, up to ${formatBytes(limits.max_upload_bytes)} and ${limits.max_pdf_pages} pages each. Files go straight to private storage.`;
}

export function UploadPage() {
  const { me } = useSession();
  const queue = useUploadQueue();
  const [limits, setLimits] = useState<UploadLimits | null>(null);
  const [recent, setRecent] = useState<JobView[] | null>(null);
  const [dragging, setDragging] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    api.uploadLimits().then(setLimits, () => setLimits(null));
  }, []);

  useEffect(() => {
    api.recentJobs(15).then(
      (response) => setRecent(response.jobs),
      () => setRecent([]),
    );
  }, [queue.finishedCount]);

  // A file dropped outside the drop zone must not make the browser open it.
  useEffect(() => {
    const block = (event: globalThis.DragEvent) => event.preventDefault();
    window.addEventListener("dragover", block);
    window.addEventListener("drop", block);
    return () => {
      window.removeEventListener("dragover", block);
      window.removeEventListener("drop", block);
    };
  }, []);

  const remaining = me.documents_remaining;
  const outOfUploads = remaining !== null && remaining <= 0;
  const ready = limits !== null && !outOfUploads;

  function accept(list: FileList | null) {
    if (list && list.length > 0 && limits && !outOfUploads) {
      queue.addFiles(Array.from(list), limits);
    }
  }

  function onDrop(event: DragEvent<HTMLElement>) {
    event.preventDefault();
    setDragging(false);
    accept(event.dataTransfer.files);
  }

  const counts = {
    active: queue.items.filter((item) => !["finished", "failed"].includes(item.phase)).length,
    stored: queue.items.filter((item) => item.job?.status === "STORED" && item.phase === "finished").length,
    review: queue.items.filter((item) => item.job?.status === "REVIEW_REQUIRED" && item.phase === "finished").length,
    failed: queue.items.filter(
      (item) => item.phase === "failed" || (item.phase === "finished" && ["FAILED", "REJECTED"].includes(item.job?.status ?? "")),
    ).length,
  };
  const used = me.document_limit !== null && remaining !== null ? me.document_limit - remaining : null;

  const recentColumns: Column<JobView>[] = [
    {
      key: "document",
      header: "Document",
      render: (job) => (
        <div className="batch-name">
          <strong title={job.name}>{job.name}</strong>
          <span className="muted">{formatBytes(job.size)}</span>
        </div>
      ),
    },
    { key: "status", header: "Status", render: (job) => <StatusBadge status={job.status} /> },
    {
      key: "result",
      header: "Result",
      render: (job) =>
        ["AUTHORIZED", "PROCESSING"].includes(job.status) ? <span className="muted">In progress</span> : <JobOutcome job={job} />,
    },
    { key: "when", header: "Uploaded", render: (job) => formatRelativeTime(job.authorized_at) ?? "" },
    {
      key: "action",
      header: "Actions",
      render: (job) =>
        job.retryable ? (
          <Button size="sm" variant="tinted" onClick={() => queue.retryJob(job)}>
            Try again
          </Button>
        ) : null,
    },
  ];

  return (
    <div className="page stack">
      <PageHeader eyebrow="Upload" title="Process documents" subtitle={limitsText(limits)} />
      <div className="upload-layout">
        <div className="stack">
          <section
            className="dropzone"
            data-dragging={dragging || undefined}
            data-disabled={!ready || undefined}
            aria-labelledby="dropzone-title"
            onDragEnter={(event) => {
              event.preventDefault();
              setDragging(true);
            }}
            onDragOver={(event) => event.preventDefault()}
            onDragLeave={(event) => {
              if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                setDragging(false);
              }
            }}
            onDrop={onDrop}
          >
            <span className="dropzone-icon">
              <Icon name="upload" size={26} />
            </span>
            <h2 id="dropzone-title" className="dropzone-title">
              {outOfUploads ? "Your upload allowance is used up" : "Drop invoices and receipts here"}
            </h2>
            <p className="muted">
              {outOfUploads
                ? "Ask the alpha administrator for more uploads."
                : "Or choose files from your computer. You can add several at once."}
            </p>
            <Button icon="file" onClick={() => input.current?.click()} disabled={!ready}>
              Choose files
            </Button>
            <input
              ref={input}
              type="file"
              multiple
              className="visually-hidden"
              tabIndex={-1}
              aria-hidden="true"
              accept={limits?.allowed_types.join(",")}
              onChange={(event) => {
                accept(event.target.files);
                event.target.value = "";
              }}
            />
          </section>

          {queue.items.length > 0 && (
            <section className="stack" style={{ gap: 14 }} aria-labelledby="batch-title">
              <h2 id="batch-title" className="section-title">
                This batch
              </h2>
              <ul className="batch">
                {queue.items.map((item) => (
                  <BatchRow key={item.id} item={item} />
                ))}
              </ul>
            </section>
          )}
        </div>

        <aside className="card-dark upload-summary" aria-label="Upload summary">
          {remaining !== null && me.document_limit !== null && used !== null && (
            <>
              <div className="eyebrow" style={{ color: "#9db0ff" }}>
                Uploads left
              </div>
              <div className="meter-value">
                {remaining} <span>of {me.document_limit}</span>
              </div>
              <div
                className="meter"
                role="progressbar"
                aria-label="Uploads used"
                aria-valuemin={0}
                aria-valuemax={me.document_limit}
                aria-valuenow={used}
              >
                <span style={{ width: `${Math.min((used / Math.max(me.document_limit, 1)) * 100, 100)}%` }} />
              </div>
            </>
          )}
          {queue.items.length > 0 && (
            <dl className="summary-counts" aria-label="This batch">
              <div>
                <dt>In progress</dt>
                <dd>{counts.active}</dd>
              </div>
              <div>
                <dt>Stored</dt>
                <dd>{counts.stored}</dd>
              </div>
              <div>
                <dt>Needs review</dt>
                <dd>{counts.review}</dd>
              </div>
              <div>
                <dt>Could not process</dt>
                <dd>{counts.failed}</dd>
              </div>
            </dl>
          )}
          {counts.review > 0 && (
            <Link to="/review" className={buttonClass("light")}>
              Review documents <Icon name="arrowRight" size={16} />
            </Link>
          )}
          <p className="muted summary-note">
            Files upload two at a time. You can open other pages while they process; keep this tab open until uploads
            finish.
          </p>
        </aside>
      </div>

      <section className="stack" style={{ gap: 14 }} aria-labelledby="recent-title">
        <h2 id="recent-title" className="section-title">
          Recent uploads
        </h2>
        <DataTable
          caption="Recent uploads in your organization"
          columns={recentColumns}
          rows={recent ?? []}
          rowKey={(job) => job.id}
          loading={recent === null}
          empty={<EmptyState title="No uploads yet">Documents you upload will appear here with their results.</EmptyState>}
        />
      </section>
    </div>
  );
}
