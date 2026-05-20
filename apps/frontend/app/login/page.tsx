"use client";

import { FormEvent, useState } from "react";
import { useRouter } from "next/navigation";

import { login } from "@/lib/api";
import { saveAuthSession } from "@/lib/auth-storage";

export default function LoginPage(): JSX.Element {
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
      const response = await login(username, password);
      saveAuthSession(response.access_token, response.user);
      router.push("/");
      router.refresh();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Login failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="card auth-card" onSubmit={submit}>
      <h1>Login</h1>
      <p>Access request submission and admin workflows.</p>
      <label>
        Username
        <input required value={username} onChange={(event) => setUsername(event.target.value)} />
      </label>
      <label>
        Password
        <input required type="password" value={password} onChange={(event) => setPassword(event.target.value)} />
      </label>
      <button className="button" type="submit" disabled={loading}>
        {loading ? "Signing in..." : "Sign in"}
      </button>
      {error ? <p className="error">{error}</p> : null}
    </form>
  );
}
