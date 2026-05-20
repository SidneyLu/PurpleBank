"use client";

/**
 * Thin shell to lazy-load the table component and keep initial JS payload lower.
 */

import dynamic from "next/dynamic";

import type { SequenceSearchResponse } from "@/lib/types";

const SequenceTable = dynamic(() => import("@/components/sequence-table"), {
  loading: () => <div className="card">Loading sequence table...</div>,
});

interface SequenceResultsShellProps {
  data: SequenceSearchResponse;
}

export function SequenceResultsShell({ data }: SequenceResultsShellProps): JSX.Element {
  return (
    <SequenceTable items={data.items} page={data.page} pageSize={data.page_size} total={data.total} />
  );
}
