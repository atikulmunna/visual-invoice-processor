import { describe, expect, it } from "vitest";

import { DEFAULT_AFTER_SIGN_IN, safeNextPath, signInUrl } from "./navigation";

describe("safeNextPath", () => {
  it("keeps same-site paths with their query", () => {
    expect(safeNextPath("/app/review")).toBe("/app/review");
    expect(safeNextPath("/app/records?q=ryans")).toBe("/app/records?q=ryans");
    expect(safeNextPath("/dashboard")).toBe("/dashboard");
  });

  it("falls back to the default when there is nothing to return to", () => {
    expect(safeNextPath(null)).toBe(DEFAULT_AFTER_SIGN_IN);
    expect(safeNextPath(undefined)).toBe(DEFAULT_AFTER_SIGN_IN);
    expect(safeNextPath("")).toBe(DEFAULT_AFTER_SIGN_IN);
  });

  it("refuses anything that could leave the site", () => {
    for (const hostile of [
      "https://evil.example",
      "//evil.example",
      "/\\evil.example",
      "/\t/evil.example",
      "/\n/evil.example",
      "javascript:alert(1)",
      "app/review",
    ]) {
      expect(safeNextPath(hostile)).toBe(DEFAULT_AFTER_SIGN_IN);
    }
  });

  it("never returns to a sign-in page, which would loop", () => {
    expect(safeNextPath("/app/login")).toBe(DEFAULT_AFTER_SIGN_IN);
    expect(safeNextPath("/app/login/?next=/app/review")).toBe(DEFAULT_AFTER_SIGN_IN);
    expect(safeNextPath("/login")).toBe(DEFAULT_AFTER_SIGN_IN);
  });
});

describe("signInUrl", () => {
  it("encodes the return path", () => {
    expect(signInUrl("/app/records?q=a&b")).toBe("/app/login?next=%2Fapp%2Frecords%3Fq%3Da%26b");
  });

  it("round-trips through safeNextPath", () => {
    const next = new URL(signInUrl("/app/records?q=ryans"), "https://ledgerly.test").searchParams.get("next");
    expect(safeNextPath(next)).toBe("/app/records?q=ryans");
  });
});
