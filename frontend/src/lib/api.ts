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
  onUnauthorized = () => window.location.assign("/login"),
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
      throw new ApiError(401, "Your session has ended. Sign in again.");
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

export interface Backlog {
  review_queue_total: number;
  dead_letter_total: number;
  attention_total: number;
}

const request = createApiClient();

export const api = {
  me: () => request<Me>("/api/me"),
  backlog: () => request<Backlog>("/backlog"),
  updateOrganization: (changes: { base_currency: string }) =>
    request<Organization>("/api/organization", { method: "PUT", body: JSON.stringify(changes) }),
};
