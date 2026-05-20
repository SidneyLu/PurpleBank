"use client";

import useSWR from "swr";

import { AuthGate } from "@/components/auth-gate";
import { fetchMySequenceRequests } from "@/lib/api";
import { useAuthedProfile } from "@/lib/hooks";

export default function MyRequestsPage(): JSX.Element {
  const { token } = useAuthedProfile();

  const { data, isLoading, error, mutate } = useSWR(
    token ? ["my-sequence-requests", token] : null,
    async ([, authToken]) => fetchMySequenceRequests(authToken),
    {
      revalidateOnFocus: false,
      dedupingInterval: 4_000,
    },
  );

  return (
    <AuthGate allowRoles={["user", "admin"]}>
      <section className="card">
        <h1>My Sequence Requests</h1>
        <p>Track approval progress of your CREATE/UPDATE/DELETE requests.</p>
        <button className="button ghost" onClick={() => mutate()} type="button">
          Refresh
        </button>

        {isLoading ? <p>Loading requests...</p> : null}
        {error ? <p className="error">{error.message}</p> : null}

        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Accession</th>
                <th>Status</th>
                <th>Reason</th>
                <th>Created</th>
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
                  <td>{item.reason ?? "-"}</td>
                  <td>{new Date(item.created_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </AuthGate>
  );
}
