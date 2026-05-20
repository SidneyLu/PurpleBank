"use client";

import { FormEvent, useState } from "react";
import useSWR from "swr";

import { AuthGate } from "@/components/auth-gate";
import { fetchAdminUserRequests, reviewAdminUserRequest } from "@/lib/api";
import { useAuthedProfile } from "@/lib/hooks";

export default function AdminUserReviewPage(): JSX.Element {
  const { token } = useAuthedProfile();
  const [reviewComment, setReviewComment] = useState("");
  const [error, setError] = useState<string | null>(null);

  const { data, isLoading, mutate } = useSWR(
    token ? ["admin-user-requests", token] : null,
    async ([, authToken]) => fetchAdminUserRequests(authToken),
    {
      revalidateOnFocus: false,
      dedupingInterval: 4_000,
    },
  );

  const onReview = async (requestId: number, decision: "APPROVE" | "REJECT"): Promise<void> => {
    if (!token) return;

    setError(null);
    try {
      await reviewAdminUserRequest(token, requestId, decision, reviewComment || undefined);
      setReviewComment("");
      await mutate();
    } catch (reviewError) {
      setError(reviewError instanceof Error ? reviewError.message : "Review failed");
    }
  };

  return (
    <AuthGate allowRoles={["admin"]}>
      <section className="card">
        <h1>User Review Console</h1>
        <p>Admin user CREATE/DELETE requests are executed only after review.</p>
        {isLoading ? <p>Loading queue...</p> : null}
        {error ? <p className="error">{error}</p> : null}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Payload</th>
                <th>Status</th>
                <th>Requester</th>
                <th>Review</th>
              </tr>
            </thead>
            <tbody>
              {(data ?? []).map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{item.action_type}</td>
                  <td>
                    <pre className="seq">{JSON.stringify(item.payload_json ?? {}, null, 2)}</pre>
                  </td>
                  <td>
                    <span className={`status ${item.status.toLowerCase()}`}>{item.status}</span>
                  </td>
                  <td>{item.requester_id}</td>
                  <td>
                    {item.status === "PENDING" ? (
                      <form
                        className="inline-actions"
                        onSubmit={(event: FormEvent<HTMLFormElement>) => {
                          event.preventDefault();
                          void onReview(item.id, "APPROVE");
                        }}
                      >
                        <input
                          placeholder="comment"
                          value={reviewComment}
                          onChange={(event) => setReviewComment(event.target.value)}
                        />
                        <button className="button" type="submit">Approve</button>
                        <button
                          className="button ghost"
                          type="button"
                          onClick={() => void onReview(item.id, "REJECT")}
                        >
                          Reject
                        </button>
                      </form>
                    ) : (
                      <span>{item.review_comment ?? "Reviewed"}</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </AuthGate>
  );
}
