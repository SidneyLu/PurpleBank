/**
 * Thin API client utilities for public and authenticated calls.
 *
 * The backend URL is environment-driven so local/dev/prod can share code.
 */

import type {
  AuthTokenResponse,
  SequenceDetail,
  SequenceRequestItem,
  SequenceSearchResponse,
  UserProfile,
  UserRequestItem,
} from "@/lib/types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8001/api/v1";

async function requestJson<T>(
  path: string,
  init: RequestInit = {},
  token?: string,
): Promise<T> {
  const headers = new Headers(init.headers ?? {});
  if (!headers.has("Content-Type") && init.body) {
    headers.set("Content-Type", "application/json");
  }
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers,
    cache: "no-store",
  });

  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const data = (await response.json()) as { detail?: string };
      if (data.detail) detail = data.detail;
    } catch {
      // Keep generic message when backend payload is not JSON.
    }
    throw new Error(detail);
  }

  return (await response.json()) as T;
}

export interface SearchParams {
  keyword?: string;
  status?: string;
  genus?: string;
  page?: number;
  page_size?: number;
  sort?: string;
  order?: string;
}

export async function fetchSequenceSearch(params: SearchParams): Promise<SequenceSearchResponse> {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      search.set(key, String(value));
    }
  });
  return requestJson<SequenceSearchResponse>(`/sequences/search?${search.toString()}`);
}

export async function fetchSequenceDetail(accession: string): Promise<SequenceDetail> {
  return requestJson<SequenceDetail>(`/sequences/${encodeURIComponent(accession)}`);
}

export async function login(username: string, password: string): Promise<AuthTokenResponse> {
  return requestJson<AuthTokenResponse>("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export async function register(username: string, password: string): Promise<AuthTokenResponse> {
  return requestJson<AuthTokenResponse>("/auth/register", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export async function fetchMe(token: string): Promise<UserProfile> {
  return requestJson<UserProfile>("/auth/me", {}, token);
}

export async function createSequenceRequest(
  token: string,
  body: {
    action_type: "CREATE" | "UPDATE" | "DELETE";
    target_accession: string;
    payload_json?: Record<string, unknown>;
    reason?: string;
  },
): Promise<SequenceRequestItem> {
  return requestJson<SequenceRequestItem>("/sequence-requests", {
    method: "POST",
    body: JSON.stringify(body),
  }, token);
}

export async function fetchMySequenceRequests(token: string): Promise<SequenceRequestItem[]> {
  return requestJson<SequenceRequestItem[]>("/sequence-requests/my", {}, token);
}

export async function fetchAdminSequenceRequests(token: string): Promise<SequenceRequestItem[]> {
  return requestJson<SequenceRequestItem[]>("/admin/sequence-requests", {}, token);
}

export async function reviewAdminSequenceRequest(
  token: string,
  requestId: number,
  decision: "APPROVE" | "REJECT",
  review_comment?: string,
): Promise<{ request_id: number; status: string; execution_summary: string }> {
  return requestJson<{ request_id: number; status: string; execution_summary: string }>(
    `/admin/sequence-requests/${requestId}/review`,
    {
      method: "POST",
      body: JSON.stringify({ decision, review_comment }),
    },
    token,
  );
}

export async function fetchAdminUsers(token: string): Promise<UserProfile[]> {
  return requestJson<UserProfile[]>("/admin/users", {}, token);
}

export async function createAdminUserRequest(
  token: string,
  body: {
    action_type: "CREATE" | "DELETE";
    payload_json: Record<string, unknown>;
    reason?: string;
  },
): Promise<UserRequestItem> {
  return requestJson<UserRequestItem>(
    "/admin/user-requests",
    {
      method: "POST",
      body: JSON.stringify(body),
    },
    token,
  );
}

export async function fetchAdminUserRequests(token: string): Promise<UserRequestItem[]> {
  return requestJson<UserRequestItem[]>("/admin/user-requests", {}, token);
}

export async function reviewAdminUserRequest(
  token: string,
  requestId: number,
  decision: "APPROVE" | "REJECT",
  review_comment?: string,
): Promise<{ request_id: number; status: string; execution_summary: string }> {
  return requestJson<{ request_id: number; status: string; execution_summary: string }>(
    `/admin/user-requests/${requestId}/review`,
    {
      method: "POST",
      body: JSON.stringify({ decision, review_comment }),
    },
    token,
  );
}
