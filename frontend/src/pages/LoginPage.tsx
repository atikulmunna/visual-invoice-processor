import { useEffect, useState, type FormEvent } from "react";
import { useSearchParams } from "react-router";

import { Icon, type IconName } from "../components/Icon";
import { Button, TextField } from "../components/ui";
import { ApiError, publicApi } from "../lib/api";
import { safeNextPath } from "../lib/navigation";

const TRUST: { icon: IconName; title: string; text: string }[] = [
  { icon: "lock", title: "Private storage", text: "Files go straight to an encrypted, private bucket." },
  { icon: "users", title: "Separate workspaces", text: "Each organization sees only its own documents." },
  { icon: "shield", title: "Nothing invented", text: "Missing details are flagged for review, never guessed." },
  { icon: "clock", title: "Short retention", text: "Source files are deleted after 30 days." },
];

export function LoginPage() {
  const [params] = useSearchParams();
  const next = safeNextPath(params.get("next"));
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    document.title = "Sign in · Ledgerly";
    // Someone who is already signed in goes straight on.
    publicApi.me().then(
      () => window.location.replace(next),
      () => undefined,
    );
  }, [next]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setSubmitting(true);
    setError(null);
    try {
      await publicApi.signIn(String(form.get("username") ?? ""), String(form.get("password") ?? ""));
      window.location.assign(next);
    } catch (caught) {
      setError(
        caught instanceof ApiError && caught.status === 401
          ? caught.message
          : "Sign-in is unavailable right now. Try again in a moment.",
      );
      setSubmitting(false);
    }
  }

  return (
    <div className="signin">
      <div className="signin-orbs" aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
      <header className="signin-top">
        <span className="brand">
          <img src="/assets/icon.png" alt="" />
          Ledgerly
        </span>
      </header>
      <main className="signin-main" id="main">
        <section>
          <span className="glass-pill">Private alpha</span>
          <h1 className="signin-title">Every bill read, checked, and ready for your books.</h1>
          <p className="signin-lead">
            Upload invoices and receipts. Ledgerly pulls out the numbers, checks that they add up, and flags anything that
            needs a second look.
          </p>
        </section>
        <section className="signin-card" aria-labelledby="signin-heading">
          <h2 id="signin-heading">Sign in</h2>
          <p className="signin-hint">Use the account details you were given for the private alpha.</p>
          <form className="signin-form" onSubmit={submit} noValidate>
            <TextField label="Username" name="username" autoComplete="username" autoFocus required maxLength={64} />
            <TextField
              label="Password"
              name="password"
              type="password"
              autoComplete="current-password"
              required
              maxLength={256}
            />
            {error && (
              <div className="signin-error" role="alert">
                {error}
              </div>
            )}
            <Button type="submit" variant="light" size="lg" disabled={submitting}>
              {submitting ? "Signing in" : "Sign in"}
            </Button>
          </form>
          <p className="signin-fineprint">Accounts are by invitation during the private alpha.</p>
        </section>
      </main>
      <footer className="signin-trust">
        <h2 className="visually-hidden">How your documents are handled</h2>
        <ul className="trust-list">
          {TRUST.map((item) => (
            <li key={item.title}>
              <span className="trust-icon">
                <Icon name={item.icon} size={18} />
              </span>
              <strong>{item.title}</strong>
              <span className="trust-text">{item.text}</span>
            </li>
          ))}
        </ul>
      </footer>
    </div>
  );
}
