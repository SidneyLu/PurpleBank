"use client";

/**
 * Top navigation that adapts links by role while keeping guest browsing open.
 */

import Link from "next/link";
import { useRouter } from "next/navigation";

import { clearAuthSession } from "@/lib/auth-storage";
import { useAuthedProfile } from "@/lib/hooks";

export function NavBar(): JSX.Element {
  const router = useRouter();
  const { profile } = useAuthedProfile();

  const logout = (): void => {
    clearAuthSession();
    router.push("/");
    router.refresh();
  };

  return (
    <header className="top-nav">
      <div className="nav-brand">
        <Link href="/" className="nav-brand-link">
          <img className="nav-brand-mark" src="/purplebank-mark.svg" alt="PurpleBank logo mark" width={36} height={36} />
          <span className="nav-brand-text">PurpleBank</span>
        </Link>
      </div>
      <nav className="nav-links">
        <Link href="/">Search</Link>
        {profile ? <Link href="/requests/new">New Request</Link> : null}
        {profile ? <Link href="/requests/mine">My Requests</Link> : null}
        {profile?.role === "admin" ? <Link href="/admin/reviews/sequences">Sequence Reviews</Link> : null}
        {profile?.role === "admin" ? <Link href="/admin/reviews/users">User Reviews</Link> : null}
        {profile?.role === "admin" ? <Link href="/admin/users">Users</Link> : null}
      </nav>
      <div className="nav-actions">
        {profile ? (
          <>
            <span className="chip">{profile.username} ({profile.role})</span>
            <button className="button ghost" onClick={logout} type="button">
              Logout
            </button>
          </>
        ) : (
          <>
            <Link className="button ghost" href="/login">
              Login
            </Link>
            <Link className="button" href="/register">
              Register
            </Link>
          </>
        )}
      </div>
    </header>
  );
}
