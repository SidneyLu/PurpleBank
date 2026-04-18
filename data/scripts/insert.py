import argparse
import os
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

import mysql.connector
from Bio import SeqIO


def clean(text):
    if text is None:
        return None
    text = text.strip()
    return text if text else None


def safe_int(text):
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def parse_fasta_records(fasta_path: Path):
    fasta_map = {}
    for rec in SeqIO.parse(str(fasta_path), "fasta"):
        meta = {}
        for part in rec.description.split("|"):
            if ":" in part:
                k, v = part.split(":", 1)
                meta[k.strip()] = v.strip()

        gene_id = clean(meta.get("geneid"))
        if not gene_id:
            continue

        fasta_map[gene_id] = {
            "sequence": str(rec.seq).upper(),
            "gene": clean(meta.get("gene")),
            "locus_tag": clean(meta.get("locus_tag")),
            "organism": clean(meta.get("org")),
            "src": clean(meta.get("src")),
        }
    return fasta_map


def infer_taxonomy(org_ref):
    lineage = clean(org_ref.findtext("Org-ref_orgname/OrgName/OrgName_lineage")) if org_ref is not None else None
    tokens = [x.strip() for x in lineage.split(";") if x.strip()] if lineage else []

    genus = clean(
        org_ref.findtext("Org-ref_orgname/OrgName/OrgName_name/OrgName_name_binomial/BinomialOrgName/BinomialOrgName_genus")
    ) if org_ref is not None else None
    if not genus and tokens:
        genus = tokens[-1]

    kingdom = tokens[0] if tokens else None

    phylum = None
    for t in tokens[1:]:
        low = t.lower()
        if low.endswith("phyta") or low.endswith("mycota") or low in {
            "chordata",
            "arthropoda",
            "mollusca",
            "nematoda",
            "annelida",
            "cnidaria",
            "porifera",
            "echinodermata",
            "platyhelminthes",
        }:
            phylum = t
            break

    order = None
    family = None
    for t in reversed(tokens):
        low = t.lower()
        if order is None and (low.endswith("ales") or low.endswith("iformes") or low.endswith("virales")):
            order = t
        if family is None and (
            low.endswith("aceae")
            or low.endswith("viridae")
            or (low.endswith("idae") and not low.endswith("oideae"))
        ):
            family = t
        if order and family:
            break

    class_name = None
    for t in tokens[1:]:
        low = t.lower()
        if low.endswith("opsida") or low.endswith("phyceae") or low.endswith("mycetes"):
            class_name = t
            break

    return kingdom, phylum, class_name, order, family, genus


def build_location_string(entrezgene, fallback_src):
    src_acc = clean(entrezgene.findtext("./Entrezgene_locus/Gene-commentary/Gene-commentary_accession"))
    start = safe_int(
        entrezgene.findtext(
            ".//Entrezgene_locus/Gene-commentary/Gene-commentary_seqs/Seq-loc/Seq-loc_int/Seq-interval/Seq-interval_from"
        )
    )
    end = safe_int(
        entrezgene.findtext(
            ".//Entrezgene_locus/Gene-commentary/Gene-commentary_seqs/Seq-loc/Seq-loc_int/Seq-interval/Seq-interval_to"
        )
    )
    strand_node = entrezgene.find(
        ".//Entrezgene_locus/Gene-commentary/Gene-commentary_seqs/Seq-loc/Seq-loc_int/Seq-interval/Seq-interval_strand/Na-strand"
    )
    strand = strand_node.attrib.get("value") if strand_node is not None else None

    if src_acc and start is not None and end is not None:
        start_1 = start + 1
        end_1 = end + 1
        strand = strand or "plus"
        return f"{src_acc}:{start_1}-{end_1}({strand})"
    return fallback_src


def accession_from_version(version: str | None, fallback: str | None = None):
    if version:
        return version.split(".", 1)[0]
    return fallback


def infer_na_subtype(mol_type, genome_type, gene_symbol, product, definition, seq):
    mt = (mol_type or "").lower()
    gt = (genome_type or "").lower()
    text = " ".join([x for x in [mt, gene_symbol, product, definition] if x]).lower()
    seq = (seq or "").upper()

    is_rna = False
    if any(x in text for x in [" mrna", "rrna", "trna"]):
        is_rna = True
    elif "rna" in mt and "dna" not in mt:
        is_rna = True
    elif "U" in seq and "T" not in seq:
        is_rna = True

    if is_rna:
        if "mrna" in text:
            return "RNA", None, "mrna", 1
        if "rrna" in text:
            return "RNA", None, "rrna", 0
        if "trna" in text:
            return "RNA", None, "trna", 0
        return "RNA", None, "others", 0

    if any(x in gt for x in ["plastid", "chloroplast", "cp"]):
        return "DNA", "cp", None, None
    if any(x in gt for x in ["mitochondr", "mt"]):
        return "DNA", "mt", None, None
    return "DNA", "genome", None, None


def table_map(cursor):
    cursor.execute("SHOW TABLES")
    rows = [r[0] for r in cursor.fetchall()]
    return {name.lower(): name for name in rows}


def table_exists(tmap, name):
    return name.lower() in tmap


def clear_tables(cursor, tmap):
    clear_order = [
        "ref_author",
        "ref_sequence",
        "reference",
        "feature",
        "dna",
        "rna",
        "sequence",
        "organism",
        "taxon",
    ]
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in clear_order:
        if table_exists(tmap, t):
            cursor.execute(f"DELETE FROM `{tmap[t]}`")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")


def get_or_create_taxon(cursor, cache, taxon_tuple, stats):
    if taxon_tuple in cache:
        return cache[taxon_tuple]
    cursor.execute(
        """
        INSERT INTO `taxon` (`kingdom`, `phylum`, `class`, `order`, `family`, `genus`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        taxon_tuple,
    )
    taxon_id = cursor.lastrowid
    cache[taxon_tuple] = taxon_id
    stats["taxon_inserted"] += 1
    return taxon_id


def get_or_create_organism(cursor, cache, scientific_name, taxon_id, stats):
    key = (scientific_name, taxon_id)
    if key in cache:
        return cache[key]
    cursor.execute(
        """
        INSERT INTO `organism` (`scientific_name`, `taxon_id`)
        VALUES (%s, %s)
        """,
        (scientific_name, taxon_id),
    )
    organism_id = cursor.lastrowid
    cache[key] = organism_id
    stats["organism_inserted"] += 1
    return organism_id


def main():
    parser = argparse.ArgumentParser(
        description="Import Entrezgene XML + FASTA data into purple_bank without changing schema."
    )
    parser.add_argument("--xml", default="gene_result.xml", help="Path to Entrezgene XML file")
    parser.add_argument("--fasta", default="gene_result_all_genes.fasta", help="Path to FASTA file")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "localhost"))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD", "Lxn20060822"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "purple_bank"))
    parser.add_argument("--batch-size", type=int, default=500, help="Commit every N records")
    parser.add_argument("--reset", action="store_true", help="Clear data tables before import")
    args = parser.parse_args()

    xml_path = Path(args.xml)
    fasta_path = Path(args.fasta)
    if not xml_path.exists():
        raise FileNotFoundError(f"找不到 XML 文件: {xml_path}")
    if not fasta_path.exists():
        raise FileNotFoundError(f"找不到 FASTA 文件: {fasta_path}")

    print(f"📥 正在读取 FASTA: {fasta_path}")
    fasta_records = parse_fasta_records(fasta_path)
    print(f"✅ FASTA 记录数: {len(fasta_records)}")

    db = mysql.connector.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    cursor = db.cursor()

    tmap = table_map(cursor)
    has_dna = table_exists(tmap, "dna")
    has_rna = table_exists(tmap, "rna")

    if args.reset:
        print("🧹 正在清空旧数据（仅清数据，不改 schema）...")
        clear_tables(cursor, tmap)
        db.commit()

    stats = Counter()

    taxon_cache = {}
    cursor.execute("SELECT `taxon_id`, `kingdom`, `phylum`, `class`, `order`, `family`, `genus` FROM `taxon`")
    for row in cursor.fetchall():
        key = tuple(row[1:])
        if key not in taxon_cache:
            taxon_cache[key] = row[0]

    organism_cache = {}
    cursor.execute("SELECT `organism_id`, `scientific_name`, `taxon_id` FROM `organism`")
    for row in cursor.fetchall():
        key = (row[1], row[2])
        if key not in organism_cache:
            organism_cache[key] = row[0]

    sequence_cache = {}
    cursor.execute("SELECT `accession`, `sequence` FROM `sequence`")
    for acc, seq in cursor.fetchall():
        sequence_cache[acc] = seq or ""

    feature_cache = set()
    cursor.execute("SELECT `accession`, `key`, `location`, COALESCE(`gene`, ''), COALESCE(`product`, '') FROM `feature`")
    for row in cursor.fetchall():
        feature_cache.add(row)

    reference_cache = set()
    cursor.execute("SELECT `accession`, `pmid` FROM `reference` WHERE `pmid` IS NOT NULL")
    for row in cursor.fetchall():
        reference_cache.add(row)

    print(f"📥 正在解析 XML: {xml_path}")
    for _, entrezgene in ET.iterparse(str(xml_path), events=("end",)):
        if entrezgene.tag != "Entrezgene":
            continue

        stats["xml_total"] += 1
        gene_id = clean(entrezgene.findtext("./Entrezgene_track-info/Gene-track/Gene-track_geneid"))
        if not gene_id:
            stats["skip_no_gene_id"] += 1
            entrezgene.clear()
            continue

        fasta_item = fasta_records.get(gene_id)
        if not fasta_item:
            stats["skip_missing_fasta"] += 1
            entrezgene.clear()
            continue

        org_ref = entrezgene.find("./Entrezgene_source/BioSource/BioSource_org/Org-ref")
        scientific_name = clean(org_ref.findtext("Org-ref_taxname")) if org_ref is not None else None
        scientific_name = scientific_name or fasta_item["organism"] or f"geneid:{gene_id}"

        taxon_tuple = infer_taxonomy(org_ref)
        taxon_id = get_or_create_taxon(cursor, taxon_cache, taxon_tuple, stats)
        organism_id = get_or_create_organism(cursor, organism_cache, scientific_name, taxon_id, stats)

        gene_symbol = clean(entrezgene.findtext("./Entrezgene_gene/Gene-ref/Gene-ref_locus")) or fasta_item["gene"]
        locus_tag = clean(entrezgene.findtext("./Entrezgene_gene/Gene-ref/Gene-ref_locus-tag")) or fasta_item["locus_tag"]

        genomic_acc = clean(entrezgene.findtext("./Entrezgene_locus/Gene-commentary/Gene-commentary_accession"))
        genomic_ver = clean(entrezgene.findtext("./Entrezgene_locus/Gene-commentary/Gene-commentary_version"))
        version = f"{genomic_acc}.{genomic_ver}" if genomic_acc and genomic_ver else genomic_acc
        accession = accession_from_version(version, fallback=gene_id)
        if not accession:
            stats["skip_no_accession"] += 1
            entrezgene.clear()
            continue

        prot_desc = clean(entrezgene.findtext("./Entrezgene_prot/Prot-ref/Prot-ref_desc"))
        prot_name = clean(entrezgene.findtext("./Entrezgene_prot/Prot-ref/Prot-ref_name/Prot-ref_name_E"))
        summary = clean(entrezgene.findtext("./Entrezgene_summary"))
        definition = prot_desc or prot_name or summary or gene_symbol or locus_tag or f"Gene {gene_id}"

        gene_type_node = entrezgene.find("./Entrezgene_type")
        mol_type = gene_type_node.attrib.get("value") if gene_type_node is not None else None
        genome_node = entrezgene.find("./Entrezgene_source/BioSource/BioSource_genome")
        genome_type = genome_node.attrib.get("value") if genome_node is not None else None
        if not mol_type:
            mol_type = genome_type

        sequence = fasta_item["sequence"]
        length = len(sequence)

        if accession not in sequence_cache:
            cursor.execute(
                """
                INSERT INTO `sequence` (`accession`, `version`, `locus`, `definition`, `organism_id`, `length`, `mol_type`, `sequence`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (accession, version, locus_tag, definition, organism_id, length, mol_type, sequence),
            )
            sequence_cache[accession] = sequence
            stats["sequence_inserted"] += 1
        else:
            old_seq = sequence_cache[accession] or ""
            if old_seq and old_seq != sequence:
                stats["sequence_conflict_same_accession"] += 1
                if len(sequence) > len(old_seq):
                    cursor.execute(
                        """
                        UPDATE `sequence`
                        SET `version`=%s, `locus`=%s, `definition`=%s, `organism_id`=%s, `length`=%s, `mol_type`=%s, `sequence`=%s
                        WHERE `accession`=%s
                        """,
                        (version, locus_tag, definition, organism_id, length, mol_type, sequence, accession),
                    )
                    sequence_cache[accession] = sequence
                    stats["sequence_replaced_by_longer"] += 1
            else:
                cursor.execute(
                    """
                    UPDATE `sequence`
                    SET `version`=%s, `locus`=%s, `definition`=%s, `organism_id`=%s, `length`=%s, `mol_type`=%s
                    WHERE `accession`=%s
                    """,
                    (version, locus_tag, definition, organism_id, length, mol_type, accession),
                )
                stats["sequence_updated"] += 1

        na_class, dna_type, rna_type, coding = infer_na_subtype(
            mol_type=mol_type,
            genome_type=genome_type,
            gene_symbol=gene_symbol,
            product=prot_desc or prot_name,
            definition=definition,
            seq=sequence,
        )

        if na_class == "DNA" and has_dna:
            cursor.execute(
                """
                INSERT INTO `dna` (`accession`, `dna_type`)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE `dna_type` = VALUES(`dna_type`)
                """,
                (accession, dna_type),
            )
            stats["dna_upserted"] += 1
            if has_rna:
                cursor.execute("DELETE FROM `rna` WHERE `accession` = %s", (accession,))

        if na_class == "RNA" and has_rna:
            cursor.execute(
                """
                INSERT INTO `rna` (`accession`, `rna_type`, `coding`)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    `rna_type` = VALUES(`rna_type`),
                    `coding` = VALUES(`coding`)
                """,
                (accession, rna_type, coding),
            )
            stats["rna_upserted"] += 1
            if has_dna:
                cursor.execute("DELETE FROM `dna` WHERE `accession` = %s", (accession,))

        location = build_location_string(entrezgene, fasta_item["src"])
        feature_key = (accession, "gene", location or "", gene_symbol or "", (prot_desc or prot_name or "") or "")
        if feature_key not in feature_cache:
            cursor.execute(
                """
                INSERT INTO `feature` (`accession`, `key`, `location`, `gene`, `product`, `translation`, `note`)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (accession, "gene", location, gene_symbol, prot_desc or prot_name, None, summary),
            )
            feature_cache.add(feature_key)
            stats["feature_inserted"] += 1
        else:
            stats["feature_skipped_duplicate"] += 1

        pmids = sorted({clean(x.text) for x in entrezgene.findall(".//Pub_pmid/PubMedId") if clean(x.text)})
        for pmid in pmids:
            ref_key = (accession, pmid)
            if ref_key in reference_cache:
                stats["reference_skipped_duplicate"] += 1
                continue
            cursor.execute(
                """
                INSERT INTO `reference` (`accession`, `title`, `journal`, `year`, `pmid`)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (accession, None, None, None, pmid),
            )
            reference_cache.add(ref_key)
            stats["reference_inserted"] += 1

        stats["processed"] += 1
        if stats["processed"] % args.batch_size == 0:
            db.commit()
            print(
                f"  · 已处理 {stats['processed']} 条；"
                f"Sequence 新增 {stats['sequence_inserted']} / 更新 {stats['sequence_updated']} / 冲突 {stats['sequence_conflict_same_accession']}；"
                f"DNA {stats['dna_upserted']} / RNA {stats['rna_upserted']}"
            )

        entrezgene.clear()

    db.commit()
    db.close()

    print("\n🎉 导入完成（未修改 schema，仅执行数据写入）")
    print(
        f"XML 总条目: {stats['xml_total']} | 成功处理: {stats['processed']} | "
        f"跳过(缺gene_id): {stats['skip_no_gene_id']} | 跳过(FASTA缺失): {stats['skip_missing_fasta']} | "
        f"跳过(缺accession): {stats['skip_no_accession']}"
    )
    print(
        f"Taxon 新增: {stats['taxon_inserted']} | Organism 新增: {stats['organism_inserted']} | "
        f"Sequence 新增: {stats['sequence_inserted']} | Sequence 更新: {stats['sequence_updated']} | "
        f"同 accession 冲突: {stats['sequence_conflict_same_accession']} | 替换为更长序列: {stats['sequence_replaced_by_longer']}"
    )
    print(
        f"DNA upsert: {stats['dna_upserted']} | RNA upsert: {stats['rna_upserted']} | "
        f"Feature 新增: {stats['feature_inserted']} | Feature 跳过重复: {stats['feature_skipped_duplicate']} | "
        f"Reference 新增: {stats['reference_inserted']} | Reference 跳过重复: {stats['reference_skipped_duplicate']}"
    )


if __name__ == "__main__":
    main()
