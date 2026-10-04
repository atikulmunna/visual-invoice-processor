import { createContext, useCallback, useContext, useEffect, useRef, useState, type ReactNode } from "react";

import { api, ApiError, uploadToStorage } from "../lib/api";
import {
  contentTypeFor,
  fileProblem,
  POLL_GIVE_UP_MS,
  pollDelay,
  TERMINAL_STATUSES,
  type JobView,
  type UploadLimits,
  type UploadPhase,
} from "../lib/uploads";
import { useSession } from "./session";

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

interface UploadQueue {
  items: UploadItem[];
  /** Increments whenever a document finishes, so lists can reload. */
  finishedCount: number;
  addFiles: (files: File[], limits: UploadLimits) => void;
  retry: (item: UploadItem) => void;
  retryJob: (job: JobView) => void;
  dismiss: (id: string) => void;
}

// Two transfers at a time keeps a large batch from starving the connection.
const MAX_PARALLEL = 2;
const TRANSFERRING: UploadPhase[] = ["authorizing", "uploading"];

const UploadQueueContext = createContext<UploadQueue | null>(null);

export function useUploadQueue(): UploadQueue {
  const queue = useContext(UploadQueueContext);
  if (!queue) {
    throw new Error("useUploadQueue must be used inside UploadQueueProvider");
  }
  return queue;
}

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

function messageOf(error: unknown): string {
  if (error instanceof ApiError && error.status === 429) {
    return "Your upload allowance is used up.";
  }
  return error instanceof ApiError ? error.message : "Something went wrong. Try again.";
}

export function UploadQueueProvider({ children }: { children: ReactNode }) {
  const { refresh } = useSession();
  const [items, setItems] = useState<UploadItem[]>([]);
  const [finishedCount, setFinishedCount] = useState(0);
  const files = useRef(new Map<string, File>());
  const started = useRef(new Set<string>());
  const active = useRef(true);

  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);

  const update = useCallback((id: string, changes: Partial<UploadItem>) => {
    setItems((list) => list.map((item) => (item.id === id ? { ...item, ...changes } : item)));
  }, []);

  const watch = useCallback(
    async (id: string, jobId: string) => {
      const begun = Date.now();
      for (let attempt = 0; active.current; attempt += 1) {
        if (Date.now() - begun > POLL_GIVE_UP_MS) {
          update(id, { phase: "stalled" });
          return;
        }
        await sleep(pollDelay(attempt));
        let job: JobView;
        try {
          job = await api.job(jobId);
        } catch {
          continue; // A dropped connection should not end the watch; the deadline does.
        }
        if (TERMINAL_STATUSES.has(job.status)) {
          update(id, { phase: "finished", job, canRetry: job.retryable });
          setFinishedCount((count) => count + 1);
          void refresh();
          return;
        }
        update(id, { job });
      }
    },
    [refresh, update],
  );

  const transfer = useCallback(
    async (id: string) => {
      const file = files.current.get(id);
      if (!file) {
        return;
      }
      update(id, { phase: "authorizing", error: null });
      let presigned;
      try {
        presigned = await api.presign({ filename: file.name, content_type: contentTypeFor(file), size: file.size });
      } catch (error) {
        update(id, { phase: "failed", failedStep: 0, error: messageOf(error), canRetry: true });
        return;
      }
      void refresh(); // One upload credit was just used.
      update(id, { phase: "uploading" });
      try {
        await uploadToStorage(presigned.upload, file);
      } catch (error) {
        update(id, { phase: "failed", failedStep: 1, error: messageOf(error), canRetry: true });
        return;
      }
      files.current.delete(id);
      update(id, { phase: "processing" });
      await watch(id, presigned.job_id);
    },
    [refresh, update, watch],
  );

  // Start queued files while fewer than MAX_PARALLEL are transferring.
  useEffect(() => {
    const transferring = items.filter((item) => TRANSFERRING.includes(item.phase)).length;
    const waiting = items.filter((item) => item.phase === "queued" && !started.current.has(item.id));
    for (const item of waiting.slice(0, Math.max(MAX_PARALLEL - transferring, 0))) {
      started.current.add(item.id);
      void transfer(item.id);
    }
  }, [items, transfer]);

  // Leaving mid-transfer would abandon the upload, so ask first.
  useEffect(() => {
    const busy = items.some((item) => item.phase === "queued" || TRANSFERRING.includes(item.phase));
    if (!busy) {
      return;
    }
    const warn = (event: BeforeUnloadEvent) => event.preventDefault();
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [items]);

  const addFiles = useCallback((incoming: File[], limits: UploadLimits) => {
    const added = incoming.map((file): UploadItem => {
      const id = crypto.randomUUID();
      const problem = fileProblem(file, limits);
      if (!problem) {
        files.current.set(id, file);
      }
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
    });
    setItems((list) => [...added, ...list]);
  }, []);

  const resumeJob = useCallback(
    async (id: string, jobId: string) => {
      try {
        await api.retryJob(jobId);
      } catch (error) {
        update(id, { phase: "failed", failedStep: 2, error: messageOf(error), canRetry: false });
        return;
      }
      update(id, { phase: "processing", error: null, canRetry: false });
      await watch(id, jobId);
    },
    [update, watch],
  );

  const retry = useCallback(
    (item: UploadItem) => {
      if (!item.canRetry) {
        return;
      }
      if (item.job?.retryable) {
        update(item.id, { phase: "processing", error: null, canRetry: false });
        void resumeJob(item.id, item.job.id);
        return;
      }
      started.current.delete(item.id);
      update(item.id, { phase: "queued", error: null, job: null, canRetry: false });
    },
    [resumeJob, update],
  );

  const retryJob = useCallback(
    (job: JobView) => {
      const id = `job-${job.id}`;
      setItems((list) => [
        { id, name: job.name, size: job.size, phase: "processing", failedStep: 2, job, error: null, canRetry: false },
        ...list.filter((item) => item.id !== id),
      ]);
      void resumeJob(id, job.id);
    },
    [resumeJob],
  );

  const dismiss = useCallback((id: string) => {
    files.current.delete(id);
    setItems((list) => list.filter((item) => item.id !== id));
  }, []);

  return (
    <UploadQueueContext.Provider value={{ items, finishedCount, addFiles, retry, retryJob, dismiss }}>
      {children}
    </UploadQueueContext.Provider>
  );
}
