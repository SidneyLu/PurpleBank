"use client";

/**
 * Unified form for creating sequence mutation requests.
 *
 * Payload is edited as JSON to keep this tool flexible for CREATE/UPDATE/DELETE
 * without locking the UI to one schema version.
 */

import { FormEvent, useMemo, useState } from "react";

import { createSequenceRequest } from "@/lib/api";

interface SequenceRequestFormProps {
  token: string;
}

const DEFAULT_PAYLOADS: Record<string, string> = {
  CREATE: JSON.stringify(
    {
      accession: "NC_NEW_000001",
      version: "1.0",
      locus: "new_locus",
      definition: "new nucleotide sequence",
      organism_id: 1,
      mol_type: "DNA",
      sequence: "ATCGATCGATCGATCGATCGATCG",
    },
    null,
    2,
  ),
  UPDATE: JSON.stringify(
    {
      version: "2.0",
      definition: "updated definition",
      sequence: "ATCGATCGATCGATCGATCGATCGAT",
      feature_gene: "rbcL",
      feature_product: "updated product",
      feature_location: "chr1:1-26(plus)",
      feature_note: "updated by review workflow",
    },
    null,
    2,
  ),
  DELETE: JSON.stringify({}, null, 2),
};

export function SequenceRequestForm({ token }: SequenceRequestFormProps): JSX.Element {
  const [actionType, setActionType] = useState<"CREATE" | "UPDATE" | "DELETE">("CREATE");
  const [targetAccession, setTargetAccession] = useState("");
  const [reason, setReason] = useState("");
  const [payloadRaw, setPayloadRaw] = useState(DEFAULT_PAYLOADS.CREATE);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const payloadHint = useMemo(() => DEFAULT_PAYLOADS[actionType], [actionType]);

  const handleActionChange = (next: "CREATE" | "UPDATE" | "DELETE"): void => {
    setActionType(next);
    setPayloadRaw(DEFAULT_PAYLOADS[next]);
  };

  const submit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    setError(null);
    setMessage(null);

    let parsedPayload: Record<string, unknown>;
    try {
      parsedPayload = JSON.parse(payloadRaw || "{}");
    } catch {
      setError("Payload JSON is invalid.");
      return;
    }

    setLoading(true);
    try {
      const created = await createSequenceRequest(token, {
        action_type: actionType,
        target_accession: targetAccession,
        payload_json: parsedPayload,
        reason,
      });
      setMessage(`Request #${created.id} submitted with status ${created.status}.`);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Submit failed.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="card request-form" onSubmit={submit}>
      <h2>Submit Sequence Change Request</h2>
      <p>All CREATE/UPDATE/DELETE actions must be reviewed by admin before execution.</p>

      <label>
        Action Type
        <select value={actionType} onChange={(event) => handleActionChange(event.target.value as "CREATE" | "UPDATE" | "DELETE")}>
          <option value="CREATE">CREATE</option>
          <option value="UPDATE">UPDATE</option>
          <option value="DELETE">DELETE</option>
        </select>
      </label>

      <label>
        Target Accession
        <input
          required
          placeholder="e.g. NC_085725"
          value={targetAccession}
          onChange={(event) => setTargetAccession(event.target.value)}
        />
      </label>

      <label>
        Reason
        <input
          placeholder="Why should this change be approved?"
          value={reason}
          onChange={(event) => setReason(event.target.value)}
        />
      </label>

      <label>
        Payload JSON
        <textarea
          rows={14}
          value={payloadRaw}
          onChange={(event) => setPayloadRaw(event.target.value)}
          placeholder={payloadHint}
        />
      </label>

      <button className="button" type="submit" disabled={loading}>
        {loading ? "Submitting..." : "Submit request"}
      </button>

      {message ? <p className="success">{message}</p> : null}
      {error ? <p className="error">{error}</p> : null}
    </form>
  );
}
