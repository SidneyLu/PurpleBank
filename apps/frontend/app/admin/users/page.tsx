"use client";

import { FormEvent, useState } from "react";
import useSWR from "swr";

import { AuthGate } from "@/components/auth-gate";
import { createAdminUserRequest, fetchAdminUsers } from "@/lib/api";
import { useAuthedProfile } from "@/lib/hooks";

export default function AdminUsersPage(): JSX.Element {
  const { token } = useAuthedProfile();
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newRole, setNewRole] = useState<"user" | "admin">("user");
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);

  const { data, isLoading, mutate } = useSWR(
    token ? ["admin-users", token] : null,
    async ([, authToken]) => fetchAdminUsers(authToken),
    {
      revalidateOnFocus: false,
      dedupingInterval: 4_000,
    },
  );

  const submitCreate = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    if (!token) return;

    setError(null);
    setMessage(null);
    try {
      const request = await createAdminUserRequest(token, {
        action_type: "CREATE",
        payload_json: {
          username: newUsername,
          password: newPassword,
          role: newRole,
        },
        reason,
      });
      setMessage(`Create request #${request.id} submitted.`);
      setNewUsername("");
      setNewPassword("");
      setReason("");
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Create request failed");
    }
  };

  const submitDelete = async (targetUserId: number): Promise<void> => {
    if (!token) return;

    setError(null);
    setMessage(null);
    try {
      const request = await createAdminUserRequest(token, {
        action_type: "DELETE",
        payload_json: { target_user_id: targetUserId },
        reason: "Admin requested user deactivation",
      });
      setMessage(`Delete request #${request.id} submitted.`);
      await mutate();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Delete request failed");
    }
  };

  return (
    <AuthGate allowRoles={["admin"]}>
      <section className="card">
        <h1>User Management</h1>
        <p>Admins cannot directly mutate users. Actions are submitted as review requests.</p>
      </section>

      <form className="card" onSubmit={submitCreate}>
        <h2>Create User Request</h2>
        <label>
          Username
          <input required value={newUsername} onChange={(event) => setNewUsername(event.target.value)} />
        </label>
        <label>
          Password
          <input required value={newPassword} onChange={(event) => setNewPassword(event.target.value)} />
        </label>
        <label>
          Role
          <select value={newRole} onChange={(event) => setNewRole(event.target.value as "user" | "admin")}>
            <option value="user">user</option>
            <option value="admin">admin</option>
          </select>
        </label>
        <label>
          Reason
          <input value={reason} onChange={(event) => setReason(event.target.value)} />
        </label>
        <button className="button" type="submit">Submit create request</button>
      </form>

      <section className="card">
        <h2>Current Users</h2>
        <button className="button ghost" type="button" onClick={() => mutate()}>
          Refresh users
        </button>
        {isLoading ? <p>Loading users...</p> : null}
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Username</th>
                <th>Role</th>
                <th>Active</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {(data ?? []).map((user) => (
                <tr key={user.id}>
                  <td>{user.id}</td>
                  <td>{user.username}</td>
                  <td>{user.role}</td>
                  <td>{user.is_active ? "Yes" : "No"}</td>
                  <td>
                    {user.is_active ? (
                      <button className="button ghost" type="button" onClick={() => void submitDelete(user.id)}>
                        Submit delete request
                      </button>
                    ) : (
                      <span className="chip">inactive</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {message ? <p className="success">{message}</p> : null}
        {error ? <p className="error">{error}</p> : null}
      </section>
    </AuthGate>
  );
}
