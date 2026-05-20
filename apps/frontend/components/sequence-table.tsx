"use client";

/**
 * Heavy results table extracted into a dedicated client component.
 * This component is dynamically imported to reduce base route bundle size.
 */

import Link from "next/link";
import { useMemo } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import type { SequenceListItem } from "@/lib/types";

interface SequenceTableProps {
  items: SequenceListItem[];
  page: number;
  pageSize: number;
  total: number;
}

function buildVisiblePages(currentPage: number, totalPages: number, windowSize: number): number[] {
  if (totalPages <= 0) return [];

  const halfWindow = Math.floor(windowSize / 2);
  let start = Math.max(1, currentPage - halfWindow);
  let end = Math.min(totalPages, start + windowSize - 1);
  start = Math.max(1, end - windowSize + 1);

  const pages: number[] = [];
  for (let value = start; value <= end; value += 1) {
    pages.push(value);
  }
  return pages;
}

export default function SequenceTable({ items, page, pageSize, total }: SequenceTableProps): JSX.Element {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(Math.max(page, 1), totalPages);
  const hasResults = total > 0;
  const visiblePages = useMemo(() => buildVisiblePages(safePage, totalPages, 5), [safePage, totalPages]);

  const buildPageHref = (targetPage: number): string => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("page", String(targetPage));

    // Keep explicit page_size in URL so pagination remains deterministic.
    if (!params.get("page_size")) {
      params.set("page_size", String(pageSize));
    }

    return `${pathname}?${params.toString()}`;
  };

  const goToPage = (targetPage: number): void => {
    const clampedTarget = Math.min(Math.max(targetPage, 1), totalPages);
    router.push(buildPageHref(clampedTarget));
  };

  return (
    <section className="card table-card">
      <div className="table-meta">
        <strong>{total}</strong> records found. Page {safePage}/{totalPages}
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Accession</th>
              <th>Name</th>
              <th>Genus</th>
              <th>Status</th>
              <th>Length</th>
              <th>Updated</th>
              <th>Ops</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.accession}>
                <td>
                  <Link href={`/sequence/${item.accession}`}>{item.accession}</Link>
                </td>
                <td>{item.scientific_name ?? "-"}</td>
                <td>{item.genus ?? "-"}</td>
                <td>
                  <span className={`status ${item.seq_status_desc}`}>{item.seq_status_desc}</span>
                </td>
                <td>{item.seq_length ?? "-"}</td>
                <td>{item.submit_time ? new Date(item.submit_time).toLocaleString() : "-"}</td>
                <td>{item.operate_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hasResults ? (
        <nav className="pagination" aria-label="Sequence result pages">
          <button
            className="pagination-button"
            type="button"
            onClick={() => goToPage(safePage - 1)}
            disabled={safePage <= 1}
          >
            Prev
          </button>
          {visiblePages.map((pageNumber) => (
            <button
              key={pageNumber}
              className={`pagination-button${pageNumber === safePage ? " active" : ""}`}
              type="button"
              aria-current={pageNumber === safePage ? "page" : undefined}
              onClick={() => goToPage(pageNumber)}
            >
              {pageNumber}
            </button>
          ))}
          <button
            className="pagination-button"
            type="button"
            onClick={() => goToPage(safePage + 1)}
            disabled={safePage >= totalPages}
          >
            Next
          </button>
        </nav>
      ) : null}
    </section>
  );
}
