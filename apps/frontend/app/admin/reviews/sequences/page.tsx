"use client";

import { FormEvent, useState } from "react";
import useSWR from "swr";

import { AuthGate } from "@/components/auth-gate";
import { fetchAdminSequenceRequests, reviewAdminSequenceRequest } from "@/lib/api";
import { useAuthedProfile } from "@/lib/hooks";

export default function AdminSequenceReviewPage(): JSX.Element {
  const { token } = useAuthedProfile();
  const [reviewComment, setReviewComment] = useState("");
  const [error, setError] = useState<string | null>(null);

  const { data, isLoading, mutate } = useSWR(
    token ? ["admin-sequence-requests", token] : null,
    async ([, authToken]) => fetchAdminSequenceRequests(authToken),
    {
      revalidateOnFocus: false,
      dedupingInterval: 4_000,
    },
  );

  const onReview = async (requestId: number, decision: "APPROVE" | "REJECT"): Promise<void> => {
    setError(null);

    if (!token) return;
    try {
      await reviewAdminSequenceRequest(token, requestId, decision, reviewComment || undefined);
      setReviewComment("");
      await mutate();
    } catch (reviewError) {
      setError(reviewError instanceof Error ? reviewError.message : "Review failed");
    }
  };

  return (
    <AuthGate allowRoles={["admin"]}>
      <section className="card">
        <h1>Sequence Review Console</h1>
        <p>Admins approve/reject sequence requests. Approved requests execute transactions.</p>
        {isLoading ? <p>Loading review queue...</p> : null}
        {error ? <p className="error">{error}</p> : null}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Accession</th>
                <th>Status</th>
                <th>Requester</th>
                <th>Reason</th>
                <th>Review</th>
              </tr>
            </thead>
            <tbody>
              {(data ?? []).map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{item.action_type}</td>
                  <td>{item.target_accession}</td>
                  <td>
                    <span className={`status ${item.status.toLowerCase()}`}>{item.status}</span>
                  </td>
                  <td>{item.requester_id}</td>
                  <td>{item.reason ?? "-"}</td>
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
