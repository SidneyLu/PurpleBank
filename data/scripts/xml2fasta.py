#!/usr/bin/env python3
"""
Parse NCBI Entrezgene XML and download genomic gene sequences into FASTA.
Coordinates in Entrezgene XML are 0-based inclusive.
"""

import argparse
import sys
import time
from collections import OrderedDict
from pathlib import Path
import xml.etree.ElementTree as ET

import requests

EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"


def reverse_complement(seq: str) -> str:
    table = str.maketrans("ACGTURYSWKMBDHVNacgturyswkmbdhvn", "TGCAAYRSWMKVHDBNtgcaayrswmkvhdbn")
    return seq.translate(table)[::-1]


def wrap_fasta(seq: str, width: int = 70):
    for i in range(0, len(seq), width):
        yield seq[i:i + width]


def pick_genomic_commentary(entrezgene):
    for gc in entrezgene.findall("./Entrezgene_locus/Gene-commentary"):
        t = gc.find("Gene-commentary_type")
        if t is not None and t.attrib.get("value") == "genomic":
            return gc
    return None


def parse_records(xml_path: Path):
    root = ET.parse(xml_path).getroot()
    records = []

    for ent in root.findall("Entrezgene"):
        gene_id = (ent.findtext("./Entrezgene_track-info/Gene-track/Gene-track_geneid") or "").strip()
        locus = (ent.findtext("./Entrezgene_gene/Gene-ref/Gene-ref_locus") or "").strip()
        locus_tag = (ent.findtext("./Entrezgene_gene/Gene-ref/Gene-ref_locus-tag") or "").strip()
        organism = (ent.findtext("./Entrezgene_source/BioSource/BioSource_org/Org-ref/Org-ref_taxname") or "").strip()

        gc = pick_genomic_commentary(ent)
        if gc is None:
            continue

        accession = (gc.findtext("Gene-commentary_accession") or "").strip()
        if not accession:
            continue

        unique_intervals = OrderedDict()
        for iv in gc.findall(".//Seq-interval"):
            frm_text = iv.findtext("Seq-interval_from")
            to_text = iv.findtext("Seq-interval_to")
            if frm_text is None or to_text is None:
                continue

            try:
                frm = int(frm_text)
                to = int(to_text)
            except ValueError:
                continue

            strand_node = iv.find("Seq-interval_strand/Na-strand")
            strand = strand_node.attrib.get("value", "plus") if strand_node is not None else "plus"
            key = (frm, to, strand)
            unique_intervals[key] = None

        if not unique_intervals:
            continue

        # Prefer the longest unique interval if multiple exist.
        frm, to, strand = max(unique_intervals.keys(), key=lambda x: (abs(x[1] - x[0]) + 1, -min(x[0], x[1])))

        records.append(
            {
                "gene_id": gene_id,
                "locus": locus,
                "locus_tag": locus_tag,
                "organism": organism,
                "accession": accession,
                "from": frm,
                "to": to,
                "strand": strand,
            }
        )

    return records


def header_to_accession_base(header_line: str) -> str:
    token = header_line.split()[0]
    if "|" in token:
        parts = [p for p in token.split("|") if p]
        if parts:
            token = parts[-1]
    return token.split(".")[0]


def parse_fasta_text(text: str):
    seqs = {}
    cur_header = None
    cur_seq = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            if cur_header is not None:
                acc = header_to_accession_base(cur_header)
                seqs[acc] = "".join(cur_seq).upper()
            cur_header = line[1:]
            cur_seq = []
        else:
            cur_seq.append(line)

    if cur_header is not None:
        acc = header_to_accession_base(cur_header)
        seqs[acc] = "".join(cur_seq).upper()

    return seqs


def fetch_fasta_batch(ids, email=None, api_key=None, max_retries=6):
    payload = {
        "db": "nuccore",
        "id": ",".join(ids),
        "rettype": "fasta",
        "retmode": "text",
        "tool": "codex_xml_gene_fetch",
    }
    if email:
        payload["email"] = email
    if api_key:
        payload["api_key"] = api_key

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(EUTILS_URL, data=payload, timeout=240)
            if resp.status_code == 200 and resp.text and not resp.text.lstrip().startswith("Error"):
                return resp.text
            err = f"HTTP {resp.status_code}: {resp.text[:200]!r}"
        except Exception as exc:
            err = str(exc)

        sleep_s = min(60, 2 ** (attempt - 1))
        print(f"[WARN] Batch fetch failed (attempt {attempt}/{max_retries}): {err}. Retry in {sleep_s}s", file=sys.stderr)
        time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch batch after {max_retries} attempts; first id={ids[0]}")


def main():
    ap = argparse.ArgumentParser(description="Download gene sequences from Entrezgene XML to FASTA")
    ap.add_argument("--xml", required=True, help="Input Entrezgene XML path")
    ap.add_argument("--out", required=True, help="Output FASTA path")
    ap.add_argument("--batch-size", type=int, default=100, help="Accessions per efetch request (default: 100)")
    ap.add_argument("--email", default="", help="Email for NCBI E-utilities")
    ap.add_argument("--api-key", default="", help="NCBI API key (optional)")
    args = ap.parse_args()

    xml_path = Path(args.xml)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Parsing XML: {xml_path}")
    records = parse_records(xml_path)
    if not records:
        raise SystemExit("No valid gene records found in XML.")

    by_acc = OrderedDict()
    for rec in records:
        by_acc.setdefault(rec["accession"], []).append(rec)

    accessions = list(by_acc.keys())
    print(f"[INFO] Parsed {len(records)} gene records, {len(accessions)} unique genomic accessions")

    written = 0
    missing_acc = []
    clipped = 0

    with out_path.open("w", encoding="utf-8", newline="\n") as fout:
        total_batches = (len(accessions) + args.batch_size - 1) // args.batch_size
        for b, start in enumerate(range(0, len(accessions), args.batch_size), start=1):
            chunk = accessions[start:start + args.batch_size]
            fasta_text = fetch_fasta_batch(chunk, email=args.email or None, api_key=args.api_key or None)
            seq_map = parse_fasta_text(fasta_text)

            for acc in chunk:
                full_seq = seq_map.get(acc)
                if full_seq is None:
                    # Try base accession fallback (in case acc includes version unexpectedly)
                    full_seq = seq_map.get(acc.split(".")[0])

                if full_seq is None:
                    missing_acc.append(acc)
                    continue

                seq_len = len(full_seq)
                for rec in by_acc[acc]:
                    frm = rec["from"]
                    to = rec["to"]
                    strand = rec["strand"]
                    start0 = min(frm, to)
                    end0 = max(frm, to)

                    if start0 >= seq_len:
                        continue
                    if end0 >= seq_len:
                        end0 = seq_len - 1
                        clipped += 1

                    subseq = full_seq[start0:end0 + 1]
                    if strand == "minus":
                        subseq = reverse_complement(subseq)

                    if not subseq:
                        continue

                    header = (
                        f">geneid:{rec['gene_id']}|gene:{rec['locus'] or 'NA'}|locus_tag:{rec['locus_tag'] or 'NA'}|"
                        f"org:{rec['organism'] or 'NA'}|src:{acc}:{start0 + 1}-{end0 + 1}({strand})"
                    )
                    fout.write(header + "\n")
                    for line in wrap_fasta(subseq):
                        fout.write(line + "\n")
                    written += 1

            print(f"[INFO] Batch {b}/{total_batches} done - cumulative written: {written}")
            # no API key: be polite to NCBI
            if not args.api_key:
                time.sleep(0.34)

    print(f"[DONE] Wrote {written} sequences to: {out_path}")
    if missing_acc:
        print(f"[WARN] Missing fetched accessions: {len(missing_acc)} (examples: {missing_acc[:10]})", file=sys.stderr)
    if clipped:
        print(f"[WARN] Clipped intervals due to sequence boundary: {clipped}", file=sys.stderr)


if __name__ == "__main__":
    main()
