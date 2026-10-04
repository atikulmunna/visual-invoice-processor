import { useEffect, useId, useRef, type MouseEvent, type ReactNode } from "react";

import { Button } from "./ui";

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title: string;
  variant?: "modal" | "drawer";
  className?: string;
  children: ReactNode;
}

/** Modal or side drawer. The native dialog element traps focus, closes on Escape,
 * and returns focus to the trigger when it closes. */
export function Dialog({ open, onClose, title, variant = "modal", className = "", children }: DialogProps) {
  const ref = useRef<HTMLDialogElement>(null);
  const titleId = useId();

  useEffect(() => {
    const dialog = ref.current;
    if (!dialog) {
      return;
    }
    if (open && !dialog.open) {
      dialog.showModal();
    } else if (!open && dialog.open) {
      dialog.close();
    }
  }, [open]);

  function closeOnBackdrop(event: MouseEvent<HTMLDialogElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <dialog
      ref={ref}
      className={["dialog", variant === "drawer" ? "dialog-drawer" : "", className].filter(Boolean).join(" ")}
      aria-labelledby={titleId}
      onClose={onClose}
      onClick={closeOnBackdrop}
    >
      <div className="dialog-header">
        <h2 id={titleId} className="dialog-title">
          {title}
        </h2>
        <Button variant="ghost" className="btn-icon" icon="close" aria-label="Close" onClick={onClose} />
      </div>
      <div className="dialog-body">{children}</div>
    </dialog>
  );
}
