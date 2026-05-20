"use client";

/**
 * Reusable auth/session hooks.
 *
 * We intentionally use SWR to deduplicate `/auth/me` calls across pages
 * and keep profile state fresh after login/logout transitions.
 */

import { useEffect, useMemo, useState } from "react";
import useSWR from "swr";

import { fetchMe } from "@/lib/api";
import { getAccessToken, getCachedUser, subscribeAuthChange } from "@/lib/auth-storage";
import type { UserProfile } from "@/lib/types";

export function useAuthSession(): { token: string | null; cachedUser: UserProfile | null } {
  const [token, setToken] = useState<string | null>(null);
  const [cachedUser, setCachedUser] = useState<UserProfile | null>(null);

  useEffect(() => {
    const sync = (): void => {
      setToken(getAccessToken());
      setCachedUser(getCachedUser());
    };
    sync();
    return subscribeAuthChange(sync);
  }, []);

  return { token, cachedUser };
}

export function useAuthedProfile() {
  const { token, cachedUser } = useAuthSession();
  const swr = useSWR(
    token ? ["auth/me", token] : null,
    async ([, authToken]) => fetchMe(authToken),
    {
      revalidateOnFocus: false,
      dedupingInterval: 5_000,
      fallbackData: cachedUser ?? undefined,
    },
  );

  const profile = useMemo(() => swr.data ?? cachedUser ?? null, [swr.data, cachedUser]);
  return {
    token,
    profile,
    isLoading: swr.isLoading,
    error: swr.error as Error | undefined,
    mutate: swr.mutate,
  };
}
