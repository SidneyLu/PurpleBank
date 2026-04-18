#!/usr/bin/env python3
"""
Enrich purple_bank reference metadata from PubMed by PMID.

This script:
1) fills reference.title / reference.journal / reference.year;
2) populates author and ref_author from PubMed author list;
3) only overwrites non-empty reference fields when they are placeholders/invalid.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import mysql.connector
import requests

EUTILS_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
PLACEHOLDER_TEXTS = {
    "unknown",
    "unk",
    "n-a",
    "na",
    "null",
    "none",
    "-",
    "tbd",
    "0",
}


def clean_text(text: str | None) -> str | None:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned or None


def normalize_text(text: str | None) -> str:
    return (clean_text(text) or "").lower()


def is_placeholder_text(text: str | None) -> bool:
    token = normalize_text(text)
    return token in PLACEHOLDER_TEXTS


def parse_year_from_text(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"(18|19|20)\d{2}", text)
    if not match:
        return None
    return int(match.group(0))


def to_int(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value = str(value).strip()
    if not value:
        return None
    if not re.fullmatch(r"-?\d+", value):
        return None
    return int(value)


def is_invalid_year(year_value, current_year: int) -> bool:
    year = to_int(year_value)
    if year is None:
        return True
    return year < 1800 or year > (current_year + 1)


def elem_text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    text = "".join(element.itertext())
    return clean_text(text)


def extract_article_year(article: ET.Element, pubmed_article: ET.Element) -> int | None:
    year_text = clean_text(article.findtext("Journal/JournalIssue/PubDate/Year"))
    year = to_int(year_text)
    if year is not None:
        return year

    medline_date = clean_text(article.findtext("Journal/JournalIssue/PubDate/MedlineDate"))
    year = parse_year_from_text(medline_date)
    if year is not None:
        return year

    for article_date in article.findall("ArticleDate"):
        year_text = clean_text(article_date.findtext("Year"))
        year = to_int(year_text)
        if year is not None:
            return year

    for pub_date in pubmed_article.findall(".//PubmedData/History/PubMedPubDate"):
        year_text = clean_text(pub_date.findtext("Year"))
        year = to_int(year_text)
        if year is not None:
            return year

    return None


def parse_authors(article: ET.Element) -> list[dict[str, str | None]]:
    authors = []
    seen = set()

    for node in article.findall("AuthorList/Author"):
        collective = elem_text(node.find("CollectiveName"))
        if collective:
            name = collective
        else:
            fore_name = elem_text(node.find("ForeName"))
            last_name = elem_text(node.find("LastName"))
            initials = elem_text(node.find("Initials"))
            if fore_name and last_name:
                name = clean_text(f"{fore_name} {last_name}")
            elif last_name and initials:
                name = clean_text(f"{initials} {last_name}")
            else:
                name = clean_text(last_name or fore_name or initials)

        if not name:
            continue

        aff_values = []
        aff_seen = set()
        for aff_node in node.findall("AffiliationInfo/Affiliation"):
            aff_text = elem_text(aff_node)
            if not aff_text:
                continue
            key = normalize_text(aff_text)
            if key in aff_seen:
                continue
            aff_seen.add(key)
            aff_values.append(aff_text)

        affiliation = "; ".join(aff_values) if aff_values else None
        dedupe_key = (normalize_text(name), normalize_text(affiliation))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        authors.append({"name": name, "affiliation": affiliation})

    return authors


def parse_pubmed_batch(xml_text: str) -> dict[str, dict]:
    root = ET.fromstring(xml_text)
    result = {}
    for pubmed_article in root.findall(".//PubmedArticle"):
        pmid = clean_text(pubmed_article.findtext("./MedlineCitation/PMID"))
        if not pmid:
            continue

        article = pubmed_article.find("./MedlineCitation/Article")
        if article is None:
            continue

        title = elem_text(article.find("ArticleTitle"))
        journal = elem_text(article.find("Journal/Title")) or elem_text(article.find("Journal/ISOAbbreviation"))
        year = extract_article_year(article, pubmed_article)
        authors = parse_authors(article)

        result[pmid] = {
            "title": title,
            "journal": journal,
            "year": year,
            "authors": authors,
        }
    return result


def fetch_pubmed_xml(
    pmids: list[str],
    email: str | None,
    api_key: str | None,
    timeout: int,
    max_retries: int,
    tool_name: str,
) -> str:
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "tool": tool_name,
    }
    if email:
        params["email"] = email
    if api_key:
        params["api_key"] = api_key

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(EUTILS_URL, params=params, timeout=timeout)
            if response.status_code == 200 and response.text.strip():
                return response.text
            err = f"HTTP {response.status_code}: {response.text[:180]!r}"
        except Exception as exc:  # noqa: BLE001
            err = str(exc)

        sleep_seconds = min(60, 2 ** (attempt - 1))
        print(
            f"[WARN] PubMed fetch failed for batch (attempt {attempt}/{max_retries}): {err}. "
            f"Retry in {sleep_seconds}s"
        )
        time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch PubMed batch after {max_retries} retries; first PMID={pmids[0]}")


def choose_text_value(existing: str | None, incoming: str | None) -> tuple[str | None, str]:
    current = clean_text(existing)
    new_value = clean_text(incoming)

    if not new_value:
        return current, "skip_no_incoming"
    if not current:
        return new_value, "fill_empty"
    if is_placeholder_text(current):
        if current != new_value:
            return new_value, "overwrite_placeholder"
        return current, "unchanged"
    if current != new_value:
        return current, "conflict"
    return current, "unchanged"


def choose_year_value(existing, incoming, current_year: int) -> tuple[int | None, str]:
    current = to_int(existing)
    new_value = to_int(incoming)

    if new_value is None:
        return current, "skip_no_incoming"
    if current is None:
        return new_value, "fill_empty"
    if is_invalid_year(current, current_year):
        if current != new_value:
            return new_value, "overwrite_invalid"
        return current, "unchanged"
    if current != new_value:
        return current, "conflict"
    return current, "unchanged"


def default_conflict_log_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(f"reference_conflicts_{stamp}.jsonl")


def ensure_affiliation_text(cursor, apply_change: bool):
    cursor.execute("SHOW COLUMNS FROM `author` LIKE 'affiliation'")
    row = cursor.fetchone()
    if not row:
        raise RuntimeError("Column author.affiliation does not exist.")
    current_type = str(row[1]).lower()
    if current_type != "text" and apply_change:
        cursor.execute("ALTER TABLE `author` MODIFY `affiliation` TEXT NULL")
    return current_type == "text", current_type


def load_reference_by_pmid(cursor) -> dict[str, list[dict]]:
    cursor.execute(
        """
        SELECT `ref_id`, `pmid`, `title`, `journal`, `year`
        FROM `reference`
        WHERE `pmid` IS NOT NULL AND TRIM(`pmid`) <> ''
        ORDER BY `ref_id`
        """
    )
    grouped = defaultdict(list)
    for ref_id, pmid, title, journal, year in cursor.fetchall():
        p = clean_text(pmid)
        if not p:
            continue
        grouped[p].append(
            {
                "ref_id": ref_id,
                "pmid": p,
                "title": title,
                "journal": journal,
                "year": year,
            }
        )
    return grouped


def load_author_cache(cursor) -> tuple[dict[tuple[str, str], int], int]:
    cursor.execute("SELECT `author_id`, `name`, `affiliation` FROM `author`")
    cache = {}
    max_id = 0
    for author_id, name, affiliation in cursor.fetchall():
        max_id = max(max_id, int(author_id))
        key = (normalize_text(name), normalize_text(affiliation))
        if key not in cache:
            cache[key] = int(author_id)
    return cache, max_id


def load_ref_author_links(cursor) -> set[tuple[int, int]]:
    cursor.execute("SELECT `ref_id`, `author_id` FROM `ref_author`")
    return {(int(ref_id), int(author_id)) for ref_id, author_id in cursor.fetchall()}


def main():
    parser = argparse.ArgumentParser(description="Enrich purple_bank reference/author metadata by PMID from PubMed.")
    parser.add_argument("--host", default=os.getenv("MYSQL_HOST", "localhost"))
    parser.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    parser.add_argument("--password", default=os.getenv("MYSQL_PASSWORD", "Lxn20060822"))
    parser.add_argument("--database", default=os.getenv("MYSQL_DATABASE", "purple_bank"))
    parser.add_argument("--batch-size", type=int, default=100, help="PMIDs per PubMed request")
    parser.add_argument("--batch-commit", type=int, default=500, help="Commit every N updated reference rows")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout (seconds) for PubMed requests")
    parser.add_argument("--max-retries", type=int, default=6, help="Max retries for each PubMed batch request")
    parser.add_argument("--email", default="", help="Email for NCBI E-utilities")
    parser.add_argument("--api-key", default="", help="NCBI API key")
    parser.add_argument("--tool-name", default="purple_bank_reference_enricher", help="NCBI E-utilities tool value")
    parser.add_argument("--dry-run", action="store_true", help="Run full logic but do not write any DB changes")
    parser.add_argument(
        "--conflict-log",
        default="",
        help="Path to conflict jsonl log file (default: auto-generated in current directory)",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("--batch-size must be > 0")
    if args.batch_commit <= 0:
        raise ValueError("--batch-commit must be > 0")

    db = mysql.connector.connect(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    cursor = db.cursor()

    stats = Counter()
    current_year = datetime.now().year
    conflict_path = Path(args.conflict_log) if args.conflict_log else default_conflict_log_path()
    conflict_fp = conflict_path.open("w", encoding="utf-8")

    try:
        if args.dry_run:
            print("[DRY-RUN] No database writes will be performed.")

        already_text, current_type = ensure_affiliation_text(cursor, apply_change=not args.dry_run)
        if already_text:
            print("[OK] Schema already compatible: author.affiliation is TEXT")
        else:
            if args.dry_run:
                print(
                    "[DRY-RUN] Pending schema migration: "
                    f"author.affiliation {current_type} -> TEXT"
                )
            else:
                db.commit()
                print("[OK] Schema migration applied: author.affiliation -> TEXT")

        refs_by_pmid = load_reference_by_pmid(cursor)
        if not refs_by_pmid:
            print("[INFO] No valid PMID records found in reference table.")
            return

        author_cache, max_author_id = load_author_cache(cursor)
        ref_author_links = load_ref_author_links(cursor)

        pmids = sorted(refs_by_pmid.keys(), key=lambda x: int(x))
        stats["reference_rows"] = sum(len(v) for v in refs_by_pmid.values())
        stats["unique_pmids"] = len(pmids)
        print(
            f"[INFO] Loaded {stats['reference_rows']} reference rows with {stats['unique_pmids']} unique PMIDs."
        )
        print(f"[INFO] Conflict log: {conflict_path.resolve()}")

        processed_ref_rows_since_commit = 0
        for batch_index, start in enumerate(range(0, len(pmids), args.batch_size), start=1):
            batch_pmids = pmids[start:start + args.batch_size]
            stats["batches_total"] += 1

            try:
                xml_text = fetch_pubmed_xml(
                    batch_pmids,
                    email=clean_text(args.email),
                    api_key=clean_text(args.api_key),
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    tool_name=args.tool_name,
                )
            except Exception as exc:  # noqa: BLE001
                stats["batches_failed"] += 1
                stats["pmids_failed"] += len(batch_pmids)
                for p in batch_pmids:
                    stats[f"pmid_failed::{p}"] += 1
                print(f"[WARN] Batch {batch_index} failed; PMIDs marked failed. Error: {exc}")
                continue

            parsed = parse_pubmed_batch(xml_text)
            found_pmids = set(parsed.keys())

            for pmid in batch_pmids:
                if pmid not in found_pmids:
                    stats["pmids_not_found"] += 1
                    stats[f"pmid_not_found::{pmid}"] += 1

            for pmid, payload in parsed.items():
                if pmid not in refs_by_pmid:
                    continue
                stats["pmids_parsed"] += 1
                title_in = payload["title"]
                journal_in = payload["journal"]
                year_in = payload["year"]
                authors_in = payload["authors"]

                for ref in refs_by_pmid[pmid]:
                    ref_id = ref["ref_id"]

                    title_new, title_action = choose_text_value(ref.get("title"), title_in)
                    journal_new, journal_action = choose_text_value(ref.get("journal"), journal_in)
                    year_new, year_action = choose_year_value(ref.get("year"), year_in, current_year)

                    changed = False

                    if title_action in {"fill_empty", "overwrite_placeholder"} and title_new != ref.get("title"):
                        ref["title"] = title_new
                        changed = True
                        stats["reference_title_updated"] += 1
                        stats[f"reference_title_action::{title_action}"] += 1
                    elif title_action == "conflict":
                        stats["reference_conflicts"] += 1
                        stats["reference_conflict_title"] += 1
                        conflict_fp.write(
                            json.dumps(
                                {
                                    "ref_id": ref_id,
                                    "pmid": pmid,
                                    "field": "title",
                                    "existing": clean_text(ref.get("title")),
                                    "incoming": clean_text(title_in),
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )

                    if journal_action in {"fill_empty", "overwrite_placeholder"} and journal_new != ref.get("journal"):
                        ref["journal"] = journal_new
                        changed = True
                        stats["reference_journal_updated"] += 1
                        stats[f"reference_journal_action::{journal_action}"] += 1
                    elif journal_action == "conflict":
                        stats["reference_conflicts"] += 1
                        stats["reference_conflict_journal"] += 1
                        conflict_fp.write(
                            json.dumps(
                                {
                                    "ref_id": ref_id,
                                    "pmid": pmid,
                                    "field": "journal",
                                    "existing": clean_text(ref.get("journal")),
                                    "incoming": clean_text(journal_in),
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )

                    if year_action in {"fill_empty", "overwrite_invalid"} and year_new != ref.get("year"):
                        ref["year"] = year_new
                        changed = True
                        stats["reference_year_updated"] += 1
                        stats[f"reference_year_action::{year_action}"] += 1
                    elif year_action == "conflict":
                        stats["reference_conflicts"] += 1
                        stats["reference_conflict_year"] += 1
                        conflict_fp.write(
                            json.dumps(
                                {
                                    "ref_id": ref_id,
                                    "pmid": pmid,
                                    "field": "year",
                                    "existing": to_int(ref.get("year")),
                                    "incoming": to_int(year_in),
                                },
                                ensure_ascii=False,
                            )
                            + "\n"
                        )

                    if changed:
                        stats["reference_rows_changed"] += 1
                        if not args.dry_run:
                            cursor.execute(
                                """
                                UPDATE `reference`
                                SET `title`=%s, `journal`=%s, `year`=%s
                                WHERE `ref_id`=%s
                                """,
                                (ref.get("title"), ref.get("journal"), ref.get("year"), ref_id),
                            )
                        processed_ref_rows_since_commit += 1
                    else:
                        stats["reference_rows_unchanged"] += 1

                    for author_item in authors_in:
                        author_name = clean_text(author_item.get("name"))
                        author_aff = clean_text(author_item.get("affiliation"))
                        if not author_name:
                            continue

                        author_key = (normalize_text(author_name), normalize_text(author_aff))
                        author_id = author_cache.get(author_key)
                        if author_id is None:
                            if args.dry_run:
                                max_author_id += 1
                                author_id = max_author_id
                            else:
                                cursor.execute(
                                    """
                                    INSERT INTO `author` (`name`, `affiliation`)
                                    VALUES (%s, %s)
                                    """,
                                    (author_name, author_aff),
                                )
                                author_id = int(cursor.lastrowid)
                            author_cache[author_key] = author_id
                            stats["author_created"] += 1
                        else:
                            stats["author_reused"] += 1

                        link_key = (int(ref_id), int(author_id))
                        if link_key in ref_author_links:
                            stats["ref_author_link_existing"] += 1
                            continue

                        if not args.dry_run:
                            cursor.execute(
                                """
                                INSERT IGNORE INTO `ref_author` (`ref_id`, `author_id`)
                                VALUES (%s, %s)
                                """,
                                (ref_id, author_id),
                            )
                        ref_author_links.add(link_key)
                        stats["ref_author_link_inserted"] += 1

                    if not args.dry_run and processed_ref_rows_since_commit >= args.batch_commit:
                        db.commit()
                        processed_ref_rows_since_commit = 0

            print(
                f"[INFO] Batch {batch_index}: parsed PMID in batch={len(found_pmids)}/{len(batch_pmids)}, "
                f"cumulative changed refs={stats['reference_rows_changed']}, "
                f"authors(new/reused)={stats['author_created']}/{stats['author_reused']}, "
                f"links(inserted/existing)={stats['ref_author_link_inserted']}/{stats['ref_author_link_existing']}"
            )
            if not args.api_key:
                time.sleep(0.34)

        if args.dry_run:
            db.rollback()
        else:
            db.commit()

    finally:
        conflict_fp.close()
        cursor.close()
        db.close()

    failed_pmids = sorted([k.split("::", 1)[1] for k in stats if k.startswith("pmid_failed::")], key=lambda x: int(x))
    not_found_pmids = sorted(
        [k.split("::", 1)[1] for k in stats if k.startswith("pmid_not_found::")],
        key=lambda x: int(x),
    )

    print("\n[DONE] PubMed enrichment finished.")
    print(f"dry_run: {args.dry_run}")
    print(
        f"reference_rows={stats['reference_rows']} unique_pmids={stats['unique_pmids']} "
        f"pmids_parsed={stats['pmids_parsed']} batches_failed={stats['batches_failed']}"
    )
    print(
        f"reference_changed={stats['reference_rows_changed']} unchanged={stats['reference_rows_unchanged']} "
        f"title_updates={stats['reference_title_updated']} journal_updates={stats['reference_journal_updated']} "
        f"year_updates={stats['reference_year_updated']} conflicts={stats['reference_conflicts']}"
    )
    print(
        f"authors_created={stats['author_created']} authors_reused={stats['author_reused']} "
        f"ref_author_inserted={stats['ref_author_link_inserted']} ref_author_existing={stats['ref_author_link_existing']}"
    )
    print(
        f"pmids_failed={len(failed_pmids)} pmids_not_found={len(not_found_pmids)} "
        f"conflict_log={conflict_path.resolve()}"
    )
    if failed_pmids:
        print(f"failed_pmids_sample={failed_pmids[:20]}")
    if not_found_pmids:
        print(f"not_found_pmids_sample={not_found_pmids[:20]}")


if __name__ == "__main__":
    main()
