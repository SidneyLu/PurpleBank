-- ============================================================
-- Module 05: Seed data
-- Purpose: Insert baseline users and approval status data.
-- Prerequisite: Modules 01-04 have created required tables/indexes.
-- Suggested execution order: 05
-- ============================================================

USE purple_bank;

INSERT INTO app_user (username, password_hash, role, is_active)
VALUES
    ('admin', 'pbkdf2_sha256$200000$cHVycGxlX2FkbWluX3NlZWQ$rV4rz8tToSLqMj9RHpiYvgfYiUXGYF76tG_gj_UHoaI', 'admin', 1),
    ('user', 'pbkdf2_sha256$200000$cHVycGxlX3VzZXJfc2VlZF8x$SgFOYQ0ILb8w9SQP2lLsaYCZBTaoMlrVx3F1BCza850', 'user', 1)
ON DUPLICATE KEY UPDATE
    password_hash = VALUES(password_hash),
    role = VALUES(role),
    is_active = VALUES(is_active);

-- Existing sequence rows are treated as approved baseline data.
INSERT INTO sequence_review(accession, seq_status, submitter)
SELECT s.accession, 1, 'seed_script'
FROM Sequence AS s
LEFT JOIN sequence_review AS sr ON sr.accession = s.accession
WHERE sr.accession IS NULL;
