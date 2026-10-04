import { signInUrl } from "./navigation";
import type { JobView, UploadLimits } from "./uploads";

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

interface ClientOptions {
  fetchImpl?: FetchLike;
  onUnauthorized?: () => void;
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const body = await response.json();
    if (typeof body?.detail === "string" && body.detail.trim()) {
      return body.detail;
    }
  } catch {
    // Not JSON; fall through to the status text.
  }
  return response.statusText || `Request failed with status ${response.status}`;
}

export function createApiClient({
  fetchImpl = (input, init) => fetch(input, init),
  onUnauthorized = () => window.location.assign(signInUrl(window.location.pathname + window.location.search)),
}: ClientOptions = {}) {
  return async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    headers.set("Accept", "application/json");
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    const response = await fetchImpl(path, { credentials: "same-origin", ...init, headers });
    if (response.status === 401) {
      onUnauthorized();
      throw new ApiError(401, await errorMessage(response));
    }
    if (!response.ok) {
      throw new ApiError(response.status, await errorMessage(response));
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return (await response.json()) as T;
  };
}

export interface Organization {
  id: string;
  name: string;
  role: "owner" | "member";
  base_currency: string;
}

export interface Me {
  username: string;
  organization: Organization | null;
  documents_remaining: number | null;
  document_limit: number | null;
}

export interface PresignedUpload {
  job_id: string;
  upload: { url: string; fields: Record<string, string> };
  documents_remaining: number;
}

export interface Backlog {
  review_queue_total: number;
  dead_letter_total: number;
  attention_total: number;
}

const request = createApiClient();
// The sign-in page handles 401s itself; redirecting there from there would loop.
const publicRequest = createApiClient({ onUnauthorized: () => undefined });

export const publicApi = {
  me: () => publicRequest<Me>("/api/me"),
  signIn: (username: string, password: string) =>
    publicRequest<{ username: string }>("/api/session", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
};

export const api = {
  me: () => request<Me>("/api/me"),
  backlog: () => request<Backlog>("/backlog"),
  updateOrganization: (changes: { base_currency: string }) =>
    request<Organization>("/api/organization", { method: "PUT", body: JSON.stringify(changes) }),
  uploadLimits: () => request<UploadLimits>("/api/upload-limits"),
  recentJobs: (limit = 20) => request<{ jobs: JobView[] }>(`/api/jobs?limit=${limit}`),
  job: (jobId: string) => request<JobView>(`/api/jobs/${encodeURIComponent(jobId)}`),
  presign: (file: { filename: string; content_type: string; size: number }) =>
    request<PresignedUpload>("/uploads/presign", { method: "POST", body: JSON.stringify(file) }),
  retryJob: (jobId: string) =>
    request<{ job_id: string; status: string }>(`/uploads/${encodeURIComponent(jobId)}/retry`, { method: "POST" }),
};

/** Send the file straight to S3 with the presigned form; the file field must come last. */
export async function uploadToStorage(upload: PresignedUpload["upload"], file: File): Promise<void> {
  const form = new FormData();
  for (const [key, value] of Object.entries(upload.fields)) {
    form.append(key, value);
  }
  form.append("file", file);
  const response = await fetch(upload.url, { method: "POST", body: form });
  if (!response.ok) {
    throw new ApiError(response.status, "The file could not be uploaded to storage.");
  }
}
