import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";

import { Button, EmptyState, Skeleton } from "../components/ui";
import { api, ApiError, type Me } from "../lib/api";

interface Session {
  me: Me;
  reviewCount: number | null;
  refresh: () => Promise<void>;
}

const SessionContext = createContext<Session | null>(null);

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (!session) {
    throw new Error("useSession must be used inside SessionProvider");
  }
  return session;
}

type LoadState = { status: "loading" } | { status: "ready"; me: Me; reviewCount: number | null } | { status: "error"; message: string };

export function SessionProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<LoadState>({ status: "loading" });

  const refresh = useCallback(async () => {
    try {
      const me = await api.me();
      // The review count is a nicety; the workspace still loads without it.
      const reviewCount = await api.backlog().then(
        (backlog) => backlog.review_queue_total,
        () => null,
      );
      setState({ status: "ready", me, reviewCount });
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        return; // The API client is already redirecting to sign-in.
      }
      // Only the first load may fail visibly. A failed background refresh keeps the
      // workspace as it was, so in-flight uploads and their status checks survive.
      const message = error instanceof Error ? error.message : "Unknown error";
      setState((current) => (current.status === "ready" ? current : { status: "error", message }));
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  if (state.status === "loading") {
    return (
      <main className="page" aria-busy="true" aria-label="Loading workspace">
        <Skeleton width={120} />
        <div style={{ height: 14 }} />
        <Skeleton width={320} height={36} />
      </main>
    );
  }
  if (state.status === "error") {
    return (
      <main className="page">
        <EmptyState
          icon="alert"
          title="The workspace could not load"
          headingLevel="h1"
          action={<Button onClick={() => void refresh()}>Try again</Button>}
        >
          {state.message}
        </EmptyState>
      </main>
    );
  }
  return (
    <SessionContext.Provider value={{ me: state.me, reviewCount: state.reviewCount, refresh }}>
      {children}
    </SessionContext.Provider>
  );
}
