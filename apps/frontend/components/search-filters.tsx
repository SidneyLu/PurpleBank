"use client";

/**
 * Search filter form that writes parameters to URL so server components can
 * fetch with statically analyzable query inputs.
 */

import { FormEvent, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

export function SearchFilters(): JSX.Element {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  const [keyword, setKeyword] = useState(params.get("keyword") ?? "");
  const [status, setStatus] = useState(params.get("status") ?? "");
  const [genus, setGenus] = useState(params.get("genus") ?? "");

  const onSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    const next = new URLSearchParams();
    if (keyword) next.set("keyword", keyword);
    if (status) next.set("status", status);
    if (genus) next.set("genus", genus);
    next.set("page", "1");

    router.push(`${pathname}?${next.toString()}`);
  };

  return (
    <form className="search-filters" onSubmit={onSubmit}>
      <input
        aria-label="keyword"
        placeholder="Accession / locus / definition"
        value={keyword}
        onChange={(event) => setKeyword(event.target.value)}
      />
      <input
        aria-label="genus"
        placeholder="Genus"
        value={genus}
        onChange={(event) => setGenus(event.target.value)}
      />
      <select value={status} onChange={(event) => setStatus(event.target.value)}>
        <option value="">All statuses</option>
        <option value="pending">pending</option>
        <option value="approved">approved</option>
        <option value="rejected">rejected</option>
      </select>
      <button className="button" type="submit">
        Search
      </button>
    </form>
  );
}
