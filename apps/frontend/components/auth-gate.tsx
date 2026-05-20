"use client";

/**
 * Reusable route guard for authenticated and role-restricted pages.
 */

import Link from "next/link";
import type { ReactNode } from "react";

import { useAuthedProfile } from "@/lib/hooks";
import type { UserRole } from "@/lib/types";

interface AuthGateProps {
  children: ReactNode;
  allowRoles?: UserRole[];
}

export function AuthGate({ children, allowRoles }: AuthGateProps): JSX.Element {
  const { profile, isLoading } = useAuthedProfile();

  if (isLoading) {
    return <div className="card">Checking session...</div>;
  }

  if (!profile) {
    return (
      <section className="card notice">
        <h2>Authentication required</h2>
        <p>This page is available after login.</p>
        <Link className="button" href="/login">
          Go to login
        </Link>
      </section>
    );
  }

  if (allowRoles && !allowRoles.includes(profile.role)) {
    return (
      <section className="card notice">
        <h2>Permission denied</h2>
        <p>Your role does not have access to this section.</p>
      </section>
    );
  }

  return <>{children}</>;
}
