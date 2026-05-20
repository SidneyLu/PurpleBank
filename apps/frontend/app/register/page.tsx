"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";

import { register } from "@/lib/api";
import { saveAuthSession } from "@/lib/auth-storage";

export default function RegisterPage(): JSX.Element {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    setError(null);
    setLoading(true);

    try {
      const response = await register(username, password);
      saveAuthSession(response.access_token, response.user);
      router.push("/");
      router.refresh();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="card auth-card" onSubmit={submit}>
      <h1>Register</h1>
      <p>Registration creates a standard user account.</p>
      <label>
        Username
        <input required minLength={3} value={username} onChange={(event) => setUsername(event.target.value)} />
      </label>
      <label>
        Password
        <input
          required
          minLength={8}
          type="password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
        />
      </label>
      <button className="button" type="submit" disabled={loading}>
        {loading ? "Registering..." : "Create account"}
      </button>
      {error ? <p className="error">{error}</p> : null}
    </form>
  );
}
