import Link from "next/link";
import { notFound } from "next/navigation";

import { fetchSequenceDetail } from "@/lib/api";

interface DetailPageProps {
  params: { accession: string };
}

export default async function SequenceDetailPage({ params }: DetailPageProps): Promise<JSX.Element> {
  let detail;
  try {
    detail = await fetchSequenceDetail(params.accession);
  } catch {
    notFound();
  }

  return (
    <>
      <section className="card">
        <h1>{detail.accession}</h1>
        <p>{detail.definition ?? "No definition"}</p>
        <div className="inline-actions">
          <span className={`chip status ${detail.seq_status_desc}`}>{detail.seq_status_desc ?? "unknown"}</span>
          <span className="chip">{detail.scientific_name ?? "Unknown organism"}</span>
          <span className="chip">Length: {detail.seq_length ?? "-"}</span>
        </div>
      </section>

      <section className="card">
        <h2>Sequence</h2>
        <pre className="seq">{detail.sequence ?? "No sequence content"}</pre>
      </section>

      <section className="card">
        <h2>Features ({detail.features.length})</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Key</th>
                <th>Location</th>
                <th>Gene</th>
                <th>Product</th>
              </tr>
            </thead>
            <tbody>
              {detail.features.map((feature, index) => (
                <tr key={String(feature.feature_id ?? `${detail.accession}-feature-${index}`)}>
                  <td>{String(feature.feature_id ?? "-")}</td>
                  <td>{String(feature.key ?? "-")}</td>
                  <td>{String(feature.location ?? "-")}</td>
                  <td>{String(feature.gene ?? "-")}</td>
                  <td>{String(feature.product ?? "-")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="card">
        <h2>References ({detail.references.length})</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Ref ID</th>
                <th>Title</th>
                <th>Journal</th>
                <th>Year</th>
                <th>PMID</th>
              </tr>
            </thead>
            <tbody>
              {detail.references.map((reference, index) => (
                <tr key={String(reference.ref_id ?? `${detail.accession}-ref-${index}`)}>
                  <td>{String(reference.ref_id ?? "-")}</td>
                  <td>{String(reference.title ?? "-")}</td>
                  <td>{String(reference.journal ?? "-")}</td>
                  <td>{String(reference.year ?? "-")}</td>
                  <td>{String(reference.pmid ?? "-")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <Link className="button ghost" href="/">
          Back to search
        </Link>
      </section>
    </>
  );
}
