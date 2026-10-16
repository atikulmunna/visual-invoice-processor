import { describe, expect, it } from "vitest";

import {
  batchCounts,
  contentTypeFor,
  fileProblem,
  formatBytes,
  hasUnsentUploads,
  nextToStart,
  pollDelay,
  queuedItem,
  stepStates,
  type UploadItem,
  type UploadLimits,
  type UploadPhase,
} from "./uploads";

const LIMITS: UploadLimits = {
  max_upload_bytes: 5 * 1024 * 1024,
  max_pdf_pages: 5,
  allowed_types: ["application/pdf", "image/png", "image/jpeg"],
};

describe("contentTypeFor", () => {
  it("prefers the browser's type and falls back to the extension", () => {
    expect(contentTypeFor({ name: "a.pdf", type: "application/pdf" })).toBe("application/pdf");
    expect(contentTypeFor({ name: "scan.JPG", type: "" })).toBe("image/jpeg");
    expect(contentTypeFor({ name: "notes.txt", type: "" })).toBe("");
  });
});

describe("fileProblem", () => {
  it("accepts supported files within the limit", () => {
    expect(fileProblem({ name: "a.pdf", type: "application/pdf", size: 1000 }, LIMITS)).toBeNull();
  });

  it("rejects unsupported, empty, and oversized files with a reason", () => {
    expect(fileProblem({ name: "a.docx", type: "application/msword", size: 10 }, LIMITS)).toMatch(/PDF, PNG, and JPEG/);
    expect(fileProblem({ name: "a.png", type: "image/png", size: 0 }, LIMITS)).toBe("The file is empty.");
    expect(fileProblem({ name: "a.png", type: "image/png", size: 6 * 1024 * 1024 }, LIMITS)).toBe(
      "The file is larger than 5.0 MB.",
    );
  });
});

describe("formatBytes", () => {
  it("uses the largest sensible unit", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(416 * 1024)).toBe("416 KB");
    expect(formatBytes(1.25 * 1024 * 1024)).toBe("1.3 MB");
  });
});

describe("pollDelay", () => {
  it("starts quickly and backs off to a ceiling", () => {
    expect(pollDelay(0)).toBe(1500);
    expect(pollDelay(1)).toBe(2250);
    expect(pollDelay(3)).toBe(5063);
    expect(pollDelay(20)).toBe(10000);
  });
});

describe("stepStates", () => {
  it("walks through authorize, upload, and extract", () => {
    expect(stepStates("queued", null)).toEqual(["pending", "pending", "pending", "pending"]);
    expect(stepStates("authorizing", null)).toEqual(["active", "pending", "pending", "pending"]);
    expect(stepStates("uploading", null)).toEqual(["done", "active", "pending", "pending"]);
    expect(stepStates("processing", "PROCESSING")).toEqual(["done", "done", "active", "pending"]);
  });

  it("completes every step for stored, reviewed, and duplicate documents", () => {
    for (const status of ["STORED", "REVIEW_REQUIRED", "DUPLICATE"]) {
      expect(stepStates("finished", status)).toEqual(["done", "done", "done", "done"]);
    }
  });

  it("marks the failing step", () => {
    expect(stepStates("finished", "REJECTED")).toEqual(["done", "done", "error", "pending"]);
    expect(stepStates("failed", null, 0)).toEqual(["error", "pending", "pending", "pending"]);
    expect(stepStates("failed", null, 1)).toEqual(["done", "error", "pending", "pending"]);
  });
});

function item(id: string, phase: UploadPhase, status: string | null = null): UploadItem {
  const job = status ? ({ id, status } as UploadItem["job"]) : null;
  return { id, name: `${id}.pdf`, size: 10, phase, failedStep: 0, job, error: null, canRetry: false };
}

describe("queuedItem", () => {
  it("queues a file within the limits and fails one outside them at once", () => {
    const fine = queuedItem("a", { name: "a.pdf", type: "application/pdf", size: 1000 }, LIMITS);
    const huge = queuedItem("b", { name: "b.pdf", type: "application/pdf", size: 6 * 1024 * 1024 }, LIMITS);

    expect(fine).toMatchObject({ id: "a", phase: "queued", error: null });
    expect(huge.phase).toBe("failed");
    expect(huge.error).toBeTruthy();
  });
});

describe("nextToStart", () => {
  it("starts queued files only while fewer than the limit are transferring", () => {
    const items = [item("a", "uploading"), item("b", "queued"), item("c", "queued"), item("d", "queued")];

    expect(nextToStart(items, new Set(), 2).map((next) => next.id)).toEqual(["b"]);
    expect(nextToStart(items, new Set(["b"]), 2).map((next) => next.id)).toEqual(["c"]);
    expect(nextToStart([item("a", "uploading"), item("b", "authorizing"), item("c", "queued")], new Set(), 2)).toEqual([]);
  });
});

describe("hasUnsentUploads", () => {
  it("is true only while a file has not reached storage", () => {
    expect(hasUnsentUploads([item("a", "queued")])).toBe(true);
    expect(hasUnsentUploads([item("a", "uploading")])).toBe(true);
    expect(hasUnsentUploads([item("a", "processing"), item("b", "finished"), item("c", "failed")])).toBe(false);
  });
});

describe("batchCounts", () => {
  it("counts the batch by outcome", () => {
    const items = [
      item("a", "uploading"),
      item("b", "finished", "STORED"),
      item("c", "finished", "REVIEW_REQUIRED"),
      item("d", "finished", "REJECTED"),
      item("e", "failed"),
      item("f", "processing", "PROCESSING"),
    ];

    expect(batchCounts(items)).toEqual({ active: 2, stored: 1, review: 1, failed: 2 });
  });
});
