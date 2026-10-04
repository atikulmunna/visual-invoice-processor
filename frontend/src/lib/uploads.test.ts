import { describe, expect, it } from "vitest";

import { contentTypeFor, fileProblem, formatBytes, pollDelay, stepStates, type UploadLimits } from "./uploads";

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
