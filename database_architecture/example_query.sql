-- 1) 单表查询
SELECT accession, version, locus, length
FROM sequence
WHERE length > 1400
ORDER BY length DESC
LIMIT 10;


-- 2) 多表连接查询
SELECT s.accession, s.version, o.scientific_name, t.family, d.dna_type, s.length
FROM sequence s
JOIN organism o ON s.organism_id = o.organism_id
LEFT JOIN taxon t ON o.taxon_id = t.taxon_id
JOIN dna d ON s.accession = d.accession
WHERE d.dna_type = 'cp'
ORDER BY s.length DESC
LIMIT 20;


-- 3) 多表嵌套查询
SELECT s.accession, s.version, s.length
FROM sequence s
WHERE s.accession IN (
    SELECT f.accession
    FROM feature f
    WHERE f.gene = 'rbcL'
      AND f.accession IN (
          SELECT d.accession
          FROM dna d
          WHERE d.dna_type = 'cp'
      )
)
ORDER BY s.length DESC
LIMIT 20;


-- 4) EXISTS 查询
SELECT s.accession, s.version, o.scientific_name
FROM sequence s
JOIN organism o ON s.organism_id = o.organism_id
WHERE EXISTS (
    SELECT 1
    FROM reference r
    WHERE r.accession = s.accession
)
AND EXISTS (
    SELECT 1
    FROM feature f
    WHERE f.accession = s.accession
      AND f.product IS NOT NULL
)
LIMIT 20;


-- 5) 聚合操作查询
SELECT t.family,
       COUNT(*) AS seq_count,
       ROUND(AVG(s.length), 2) AS avg_length,
       MIN(s.length) AS min_length,
       MAX(s.length) AS max_length
FROM sequence s
JOIN organism o ON s.organism_id = o.organism_id
LEFT JOIN taxon t ON o.taxon_id = t.taxon_id
GROUP BY t.family
HAVING COUNT(*) >= 5
ORDER BY seq_count DESC;
