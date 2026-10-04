// Where sign-in sends people when they did not come from a specific page.
export const DEFAULT_AFTER_SIGN_IN = "/app/overview";

const SIGN_IN_PATHS = ["/login", "/app/login"];

/**
 * Accept only a same-site path, so a crafted sign-in link cannot send someone to
 * another site afterwards. Browsers drop tabs and newlines inside URLs and treat
 * backslashes as slashes, so "/\t/evil.example" would otherwise become "//evil.example".
 */
export function safeNextPath(raw: string | null | undefined): string {
  if (!raw || !raw.startsWith("/") || raw.startsWith("//") || /[\s\\]/.test(raw)) {
    return DEFAULT_AFTER_SIGN_IN;
  }
  const path = raw.split(/[?#]/)[0].replace(/\/+$/, "");
  return SIGN_IN_PATHS.includes(path) ? DEFAULT_AFTER_SIGN_IN : raw;
}

export function signInUrl(returnTo: string): string {
  return `/app/login?next=${encodeURIComponent(returnTo)}`;
}
