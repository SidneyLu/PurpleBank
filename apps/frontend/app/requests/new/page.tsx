"use client";

import dynamic from "next/dynamic";

import { AuthGate } from "@/components/auth-gate";
import { useAuthedProfile } from "@/lib/hooks";

const SequenceRequestForm = dynamic(
  () => import("@/components/sequence-request-form").then((module) => module.SequenceRequestForm),
  {
    loading: () => <div className="card">Loading request form...</div>,
  },
);

export default function NewRequestPage(): JSX.Element {
  const { token } = useAuthedProfile();

  return (
    <AuthGate allowRoles={["user", "admin"]}>
      {token ? <SequenceRequestForm token={token} /> : <div className="card">Loading token...</div>}
    </AuthGate>
  );
}
