import { SearchFilters } from "@/components/search-filters";
import { SequenceResultsShell } from "@/components/sequence-results-shell";
import { fetchSequenceSearch } from "@/lib/api";

interface HomePageProps {
  searchParams: {
    keyword?: string;
    status?: string;
    genus?: string;
    page?: string;
    page_size?: string;
    sort?: string;
    order?: string;
  };
}

export default async function HomePage({ searchParams }: HomePageProps): Promise<JSX.Element> {
  const page = Number(searchParams.page ?? "1");
  const pageSize = Number(searchParams.page_size ?? "10");

  // Kick off fetch early to avoid avoidable waterfall with downstream rendering.
  const searchPromise = fetchSequenceSearch({
    keyword: searchParams.keyword,
    status: searchParams.status,
    genus: searchParams.genus,
    page,
    page_size: pageSize,
    sort: searchParams.sort ?? "submit_time",
    order: searchParams.order ?? "desc",
  });

  let data;
  try {
    data = await searchPromise;
  } catch {
    // Keep guest page available even if backend is temporarily unavailable.
    data = { items: [], page, page_size: pageSize, total: 0 };
  }

  return (
    <>
      <section className="card hero">
        <h1>PurpleBank Sequence Explorer</h1>
        <p>Guest users can browse sequence data. Mutations require reviewed requests.</p>
      </section>
      <section className="card">
        <SearchFilters />
      </section>
      <SequenceResultsShell data={data} />
    </>
  );
}
