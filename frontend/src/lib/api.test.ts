import { describe, expect, it, vi } from "vitest";

import { ApiError, createApiClient } from "./api";

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("createApiClient", () => {
  it("returns parsed JSON and sends the session cookie with JSON headers", async () => {
    const fetchImpl = vi.fn(async () => jsonResponse(200, { username: "tester.one" }));
    const request = createApiClient({ fetchImpl, onUnauthorized: vi.fn() });

    const body = await request<{ username: string }>("/api/me", { method: "PUT", body: "{}" });

    expect(body.username).toBe("tester.one");
    const [, init] = fetchImpl.mock.calls[0] as unknown as [string, RequestInit];
    const headers = new Headers(init.headers);
    expect(init.credentials).toBe("same-origin");
    expect(headers.get("Accept")).toBe("application/json");
    expect(headers.get("Content-Type")).toBe("application/json");
  });

  it("redirects to sign-in and throws on 401", async () => {
    const onUnauthorized = vi.fn();
    const request = createApiClient({ fetchImpl: async () => jsonResponse(401, {}), onUnauthorized });

    await expect(request("/api/me")).rejects.toMatchObject({ status: 401 });
    expect(onUnauthorized).toHaveBeenCalledOnce();
  });

  it("surfaces FastAPI error details", async () => {
    const request = createApiClient({
      fetchImpl: async () => jsonResponse(403, { detail: "Only organization owners can change settings" }),
      onUnauthorized: vi.fn(),
    });

    const error = await request("/api/organization").catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(403);
    expect((error as ApiError).message).toBe("Only organization owners can change settings");
  });

  it("falls back to the status text when the error body is not JSON", async () => {
    const request = createApiClient({
      fetchImpl: async () => new Response("upstream crashed", { status: 502, statusText: "Bad Gateway" }),
      onUnauthorized: vi.fn(),
    });

    await expect(request("/backlog")).rejects.toThrow("Bad Gateway");
  });

  it("returns undefined for 204 responses", async () => {
    const request = createApiClient({ fetchImpl: async () => new Response(null, { status: 204 }), onUnauthorized: vi.fn() });

    await expect(request("/api/thing")).resolves.toBeUndefined();
  });
});
