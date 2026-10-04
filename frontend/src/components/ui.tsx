import { useId, type ButtonHTMLAttributes, type InputHTMLAttributes, type ReactNode } from "react";

import { statusLabel, type Tone } from "../lib/labels";
import { Icon, type IconName } from "./Icon";

type ButtonVariant = "primary" | "light" | "glass" | "tinted" | "ghost" | "danger";
type ButtonSize = "sm" | "md" | "lg";

export function buttonClass(variant: ButtonVariant = "primary", size: ButtonSize = "md", extra = ""): string {
  return ["btn", `btn-${variant}`, size === "md" ? "" : `btn-${size}`, extra].filter(Boolean).join(" ");
}

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  icon?: IconName;
}

export function Button({ variant, size, icon, className = "", children, type = "button", ...rest }: ButtonProps) {
  return (
    <button type={type} className={buttonClass(variant, size, className)} {...rest}>
      {icon && <Icon name={icon} size={size === "sm" ? 15 : 17} />}
      {children}
    </button>
  );
}

export function Badge({ tone, children }: { tone: Tone; children: ReactNode }) {
  return <span className={`badge tone-${tone}`}>{children}</span>;
}

export function StatusBadge({ status }: { status: string }) {
  const { label, tone } = statusLabel(status);
  return <Badge tone={tone}>{label}</Badge>;
}

interface ChipProps {
  pressed: boolean;
  onToggle: () => void;
  children: ReactNode;
}

export function Chip({ pressed, onToggle, children }: ChipProps) {
  return (
    <button type="button" className="chip" aria-pressed={pressed} onClick={onToggle}>
      {children}
    </button>
  );
}

export function Card({ dark = false, className = "", children }: { dark?: boolean; className?: string; children: ReactNode }) {
  return <section className={`${dark ? "card-dark" : "card"} ${className}`.trim()}>{children}</section>;
}

export function Stat({ label, value, hint }: { label: string; value: ReactNode; hint?: ReactNode }) {
  return (
    <Card>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
      {hint && <p className="muted" style={{ marginTop: 6, fontSize: 13.5 }}>{hint}</p>}
    </Card>
  );
}

export function Skeleton({ width = "100%", height = 14 }: { width?: number | string; height?: number }) {
  return <span className="skeleton" style={{ width, height }} aria-hidden="true" />;
}

interface EmptyStateProps {
  icon?: IconName;
  title: string;
  /** Use h1 when the empty state is the whole page. */
  headingLevel?: "h1" | "h2";
  children?: ReactNode;
  action?: ReactNode;
}

export function EmptyState({ icon = "file", title, headingLevel: Heading = "h2", children, action }: EmptyStateProps) {
  return (
    <div className="empty">
      <div className="empty-icon">
        <Icon name={icon} size={26} />
      </div>
      <Heading className="empty-title">{title}</Heading>
      {children && <p className="empty-text">{children}</p>}
      {action}
    </div>
  );
}

export type StepState = "pending" | "active" | "done" | "error";

export function Stepper({ steps }: { steps: { label: string; state: StepState }[] }) {
  return (
    <ol className="stepper">
      {steps.map((step, index) => (
        <li key={step.label} className="step" data-state={step.state}>
          <span className="step-dot" aria-hidden="true">
            {step.state === "done" ? <Icon name="check" size={13} /> : step.state === "error" ? "!" : index + 1}
          </span>
          {step.label}
          <span className="visually-hidden">
            {step.state === "done" ? " complete" : step.state === "active" ? " in progress" : step.state === "error" ? " failed" : ""}
          </span>
        </li>
      ))}
    </ol>
  );
}

interface TextFieldProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  hint?: ReactNode;
  error?: string | null;
}

export function TextField({ label, hint, error, className = "", ...rest }: TextFieldProps) {
  const id = useId();
  const describedBy = [hint ? `${id}-hint` : "", error ? `${id}-error` : ""].filter(Boolean).join(" ") || undefined;
  return (
    <div className="field">
      <label className="field-label" htmlFor={id}>
        {label}
      </label>
      <input
        id={id}
        className={`input ${className}`.trim()}
        aria-invalid={error ? true : undefined}
        aria-describedby={describedBy}
        {...rest}
      />
      {hint && (
        <span id={`${id}-hint`} className="field-hint">
          {hint}
        </span>
      )}
      {error && (
        <span id={`${id}-error`} className="field-error" role="alert">
          {error}
        </span>
      )}
    </div>
  );
}
