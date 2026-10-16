import type { StepState } from "../components/ui";

export interface UploadLimits {
  max_upload_bytes: number;
  max_pdf_pages: number;
  allowed_types: string[];
}

export interface JobSummary {
  vendor_name: string | null;
  invoice_date: string | null;
  total_amount: number | null;
  currency: string | null;
  currency_assumed: boolean;
}

export interface JobView {
  id: string;
  name: string;
  content_type: string;
  size: number;
  status: string;
  page_count: number | null;
  reason_codes: string[];
  error_code: string | null;
  error_message: string | null;
  retryable: boolean;
  authorized_at: string | null;
  completed_at: string | null;
  summary: JobSummary | null;
}

/** Where a file is in this browser's upload flow. */
export type UploadPhase = "queued" | "authorizing" | "uploading" | "processing" | "finished" | "failed" | "stalled";

export const TERMINAL_STATUSES = new Set(["STORED", "REVIEW_REQUIRED", "DUPLICATE", "REJECTED", "FAILED"]);

const EXTENSION_TYPES: Record<string, string> = {
  pdf: "application/pdf",
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
};

/** Some systems report no type for dropped files, so fall back to the extension. */
export function contentTypeFor(file: { name: string; type: string }): string {
  if (file.type) {
    return file.type;
  }
  const extension = file.name.split(".").pop()?.toLowerCase() ?? "";
  return EXTENSION_TYPES[extension] ?? "";
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${Math.round(bytes / 1024)} KB`;
  }
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** A reason the file cannot be uploaded, checked before spending an upload credit. */
export function fileProblem(file: { name: string; type: string; size: number }, limits: UploadLimits): string | null {
  if (!limits.allowed_types.includes(contentTypeFor(file))) {
    return "Only PDF, PNG, and JPEG files can be processed.";
  }
  if (file.size === 0) {
    return "The file is empty.";
  }
  if (file.size > limits.max_upload_bytes) {
    return `The file is larger than ${formatBytes(limits.max_upload_bytes)}.`;
  }
  return null;
}

const FIRST_POLL_MS = 1500;
const MAX_POLL_MS = 10000;
export const POLL_GIVE_UP_MS = 6 * 60 * 1000;

/** Check quickly at first, then back off: most documents finish in seconds, a few take minutes. */
export function pollDelay(attempt: number): number {
  return Math.min(Math.round(FIRST_POLL_MS * 1.5 ** attempt), MAX_POLL_MS);
}

const STEP_COUNT = 4;

function steps(done: number, current: StepState | null): StepState[] {
  return Array.from({ length: STEP_COUNT }, (_, index) => {
    if (index < done) return "done";
    if (index === done && current) return current;
    return "pending";
  });
}

/** Progress for the Authorize, Upload, Extract, Validate steps. */
export function stepStates(phase: UploadPhase, status: string | null, failedStep = 0): StepState[] {
  switch (phase) {
    case "queued":
      return steps(0, null);
    case "authorizing":
      return steps(0, "active");
    case "uploading":
      return steps(1, "active");
    case "processing":
    case "stalled":
      return steps(2, "active");
    case "failed":
      return steps(failedStep, "error");
    case "finished":
      return status === "REJECTED" || status === "FAILED" ? steps(2, "error") : steps(STEP_COUNT, null);
  }
}

/** One file in the upload batch, from queued to finished or failed. */
export interface UploadItem {
  id: string;
  name: string;
  size: number;
  phase: UploadPhase;
  /** Which step failed when phase is "failed": 0 authorize, 1 upload, 2 processing. */
  failedStep: number;
  job: JobView | null;
  error: string | null;
  canRetry: boolean;
}

const TRANSFERRING: UploadPhase[] = ["authorizing", "uploading"];

/** A new batch item: queued, or failed at once when the file breaks the limits. */
export function queuedItem(id: string, file: { name: string; type: string; size: number }, limits: UploadLimits): UploadItem {
  const problem = fileProblem(file, limits);
  return {
    id,
    name: file.name,
    size: file.size,
    phase: problem ? "failed" : "queued",
    failedStep: 0,
    job: null,
    error: problem,
    canRetry: false,
  };
}

/** Queued items to start now, so that at most maxParallel transfers run at once. */
export function nextToStart(items: UploadItem[], started: Set<string>, maxParallel: number): UploadItem[] {
  const transferring = items.filter((item) => TRANSFERRING.includes(item.phase)).length;
  const waiting = items.filter((item) => item.phase === "queued" && !started.has(item.id));
  return waiting.slice(0, Math.max(maxParallel - transferring, 0));
}

/** True while leaving the page would abandon an upload that has not reached storage. */
export function hasUnsentUploads(items: UploadItem[]): boolean {
  return items.some((item) => item.phase === "queued" || TRANSFERRING.includes(item.phase));
}

export interface BatchCounts {
  active: number;
  stored: number;
  review: number;
  failed: number;
}

/** How this batch stands: still moving, stored, waiting for review, or not processed. */
export function batchCounts(items: UploadItem[]): BatchCounts {
  const finished = (item: UploadItem, statuses: string[]) => item.phase === "finished" && statuses.includes(item.job?.status ?? "");
  return {
    active: items.filter((item) => !["finished", "failed"].includes(item.phase)).length,
    stored: items.filter((item) => finished(item, ["STORED"])).length,
    review: items.filter((item) => finished(item, ["REVIEW_REQUIRED"])).length,
    failed: items.filter((item) => item.phase === "failed" || finished(item, ["FAILED", "REJECTED"])).length,
  };
}
