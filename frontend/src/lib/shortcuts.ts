import { useEffect } from "react";

/** Single-key shortcuts that stay out of the way while typing or while a dialog is open. */
export function useShortcuts(bindings: Record<string, () => void>, enabled = true): void {
  useEffect(() => {
    if (!enabled) {
      return;
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.altKey || event.ctrlKey || event.metaKey || event.repeat) return;
      const target = event.target as HTMLElement | null;
      if (target?.closest("input, textarea, select, [contenteditable='true']")) return;
      if (document.querySelector("dialog[open]")) return;
      const action = bindings[event.key.toLowerCase()];
      if (action) {
        event.preventDefault();
        action();
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [bindings, enabled]);
}
