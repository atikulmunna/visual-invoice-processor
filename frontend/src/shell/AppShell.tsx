import { useEffect, useState, type FormEvent } from "react";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router";

import { Icon, type IconName } from "../components/Icon";
import { ToastProvider } from "../components/Toast";
import { buttonClass } from "../components/ui";
import { initials } from "../lib/format";
import { useMenu } from "../lib/menu";
import { SessionProvider, useSession } from "./session";
import { UploadQueueProvider, useUploadQueue } from "./uploadQueue";

const NAV: { to: string; label: string; icon: IconName }[] = [
  { to: "/overview", label: "Overview", icon: "overview" },
  { to: "/records", label: "Records", icon: "records" },
  { to: "/review", label: "Review", icon: "review" },
];

function NavLinks({ reviewCount }: { reviewCount: number | null }) {
  return (
    <>
      {NAV.map((item) => (
        <NavLink key={item.to} to={item.to} className="nav-link">
          <Icon name={item.icon} size={16} />
          {item.label}
          {item.to === "/review" && reviewCount ? (
            <span className="count-badge" aria-label={`${reviewCount} waiting`}>
              {reviewCount}
            </span>
          ) : null}
        </NavLink>
      ))}
    </>
  );
}

function SearchForm({ id }: { id: string }) {
  const navigate = useNavigate();

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const query = String(new FormData(event.currentTarget).get("q") ?? "").trim();
    navigate(query ? `/records?q=${encodeURIComponent(query)}` : "/records");
  }

  return (
    <form className="search" role="search" onSubmit={submit}>
      <Icon name="search" size={16} />
      <label className="visually-hidden" htmlFor={id}>
        Search records
      </label>
      <input id={id} name="q" type="search" placeholder="Search records" autoComplete="off" />
    </form>
  );
}

function AccountMenu() {
  const { me } = useSession();
  const { open, setOpen, containerRef } = useMenu();

  return (
    <div className="menu" ref={containerRef}>
      <button
        type="button"
        className="avatar"
        aria-expanded={open}
        aria-controls="account-menu"
        aria-label={`Account menu for ${me.username}`}
        onClick={() => setOpen((value) => !value)}
      >
        {initials(me.username)}
      </button>
      {open && (
        <div className="menu-panel" id="account-menu">
          <div className="menu-identity">
            <strong>{me.username}</strong>
            {me.organization && <span className="muted">{me.organization.name}</span>}
          </div>
          <Link className="menu-item" to="/settings" onClick={() => setOpen(false)}>
            <Icon name="settings" /> Settings
          </Link>
          <form method="post" action="/logout">
            <button type="submit" className="menu-item">
              <Icon name="logout" /> Sign out
            </button>
          </form>
        </div>
      )}
    </div>
  );
}

function TopBar() {
  const { reviewCount } = useSession();
  const { items } = useUploadQueue();
  const uploading = items.filter((item) => !["finished", "failed", "stalled"].includes(item.phase)).length;
  const [mobileOpen, setMobileOpen] = useState(false);
  const location = useLocation();

  useEffect(() => setMobileOpen(false), [location.pathname, location.search]);

  return (
    <>
      <header className="topbar">
        <div className="topbar-inner">
          <Link to="/overview" className="brand">
            <img src="/assets/icon.png" alt="" />
            Ledgerly
          </Link>
          <nav className="nav" aria-label="Main">
            <NavLinks reviewCount={reviewCount} />
          </nav>
          <div className="topbar-actions">
            <SearchForm id="search-desktop" />
            <Link
              to="/upload"
              className={buttonClass("light", "sm")}
              aria-label={uploading ? `Upload documents, ${uploading} in progress` : "Upload documents"}
            >
              <Icon name="upload" size={15} />
              <span className="upload-label">Upload</span>
              {uploading > 0 && <span className="count-badge">{uploading}</span>}
            </Link>
            <AccountMenu />
            <button
              type="button"
              className={buttonClass("glass", "sm", "btn-icon menu-toggle")}
              aria-expanded={mobileOpen}
              aria-controls="mobile-nav"
              aria-label={mobileOpen ? "Close navigation" : "Open navigation"}
              onClick={() => setMobileOpen((value) => !value)}
            >
              <Icon name={mobileOpen ? "close" : "menu"} />
            </button>
          </div>
        </div>
      </header>
      <div className="mobile-sheet" id="mobile-nav" hidden={!mobileOpen}>
        <SearchForm id="search-mobile" />
        <nav aria-label="Main mobile">
          <NavLinks reviewCount={reviewCount} />
        </nav>
      </div>
    </>
  );
}

export function AppShell() {
  return (
    <ToastProvider>
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <div className="ambient" aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
      <SessionProvider>
        <UploadQueueProvider>
          <TopBar />
          <main id="main" tabIndex={-1}>
            <Outlet />
          </main>
        </UploadQueueProvider>
      </SessionProvider>
    </ToastProvider>
  );
}
