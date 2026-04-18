-- ============================================================
-- Module 07: Views
-- Purpose: Create reporting view for nucleotide sequence info.
-- Prerequisite: Modules 01-06 are complete.
-- Suggested execution order: 07
-- ============================================================

USE purple_bank;

DROP VIEW IF EXISTS v_nucleotide_sequence_info;

CREATE VIEW v_nucleotide_sequence_info AS
SELECT
    s.accession,
    s.version,
    s.locus,
    s.definition,
    s.organism_id,
    o.scientific_name,
    t.genus,
    COALESCE(sr.seq_status, 1) AS seq_status,
    CASE COALESCE(sr.seq_status, 1)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'approved'
        WHEN 2 THEN 'rejected'
        ELSE 'unknown'
    END AS seq_status_desc,
    COALESCE(sr.submitter, 'seed_script') AS submitter,
    COALESCE(sr.submit_time, CURRENT_TIMESTAMP) AS submit_time,
    s.length AS seq_length,
    CONCAT(LEFT(COALESCE(s.sequence, ''), 20), '...') AS seq_content_simple,
    COALESCE(op.operate_count, 0) AS operate_count,
    op.last_operate_time
FROM Sequence AS s
LEFT JOIN Organism AS o ON o.organism_id = s.organism_id
LEFT JOIN Taxon AS t ON t.taxon_id = o.taxon_id
LEFT JOIN sequence_review AS sr ON sr.accession = s.accession
LEFT JOIN (
    SELECT accession,
           COUNT(*) AS operate_count,
           MAX(operate_time) AS last_operate_time
    FROM sequence_operation_log
    GROUP BY accession
) AS op ON op.accession = s.accession;
