import { createContext, useCallback, useContext, useState, type ReactNode } from "react";

import { Icon } from "./Icon";

type ToastTone = "success" | "danger" | "info";

interface ToastInput {
  tone?: ToastTone;
  title: string;
  description?: string;
}

interface ToastItem extends ToastInput {
  id: number;
}

const DISMISS_AFTER_MS = 4500;
const ToastContext = createContext<(toast: ToastInput) => void>(() => undefined);
let nextId = 0;

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const push = useCallback((toast: ToastInput) => {
    const id = ++nextId;
    setToasts((current) => [...current, { tone: "info", ...toast, id }]);
    window.setTimeout(() => setToasts((current) => current.filter((item) => item.id !== id)), DISMISS_AFTER_MS);
  }, []);

  return (
    <ToastContext.Provider value={push}>
      {children}
      <div className="toast-region" role="status" aria-live="polite">
        {toasts.map((toast) => (
          <div key={toast.id} className="toast" data-tone={toast.tone}>
            <span className="toast-icon">
              <Icon name={toast.tone === "danger" ? "alert" : "check"} />
            </span>
            <div>
              <div className="toast-title">{toast.title}</div>
              {toast.description && <div className="toast-text">{toast.description}</div>}
            </div>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  return useContext(ToastContext);
}
