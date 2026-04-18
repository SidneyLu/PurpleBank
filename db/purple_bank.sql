-- ============================================================
-- Module 01: Database bootstrap
-- Purpose: Create and select the purple_bank database.
-- Prerequisite: None.
-- Suggested execution order: 01
-- ============================================================

CREATE DATABASE IF NOT EXISTS purple_bank;
USE purple_bank;


-- ============================================================
-- Module 02: Core schema
-- Purpose: Create core biological data tables.
-- Prerequisite: Module 01 has created database purple_bank.
-- Suggested execution order: 02
-- ============================================================

USE purple_bank;

CREATE TABLE Taxon
(
    taxon_id INT PRIMARY KEY AUTO_INCREMENT,
    kingdom VARCHAR(100),
    phylum VARCHAR(100),
    class VARCHAR(100),
    `order` VARCHAR(100),
    family VARCHAR(100),
    genus VARCHAR(100)
);

CREATE TABLE Organism
(
    organism_id INT PRIMARY KEY AUTO_INCREMENT,
    scientific_name VARCHAR(255) NOT NULL,
    taxon_id INT,
    FOREIGN KEY (taxon_id) REFERENCES Taxon(taxon_id)
);

CREATE TABLE Sequence
(
    accession VARCHAR(50) PRIMARY KEY,
    version VARCHAR(50),
    locus VARCHAR(50),
    definition TEXT,
    organism_id INT,
    length INT,
    mol_type VARCHAR(50),
    sequence LONGTEXT,
    FOREIGN KEY (organism_id) REFERENCES Organism(organism_id)
);

CREATE TABLE DNA
(
    accession VARCHAR(50) PRIMARY KEY,
    dna_type VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE RNA
(
    accession VARCHAR(50) PRIMARY KEY,
    rna_type VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Feature
(
    feature_id INT PRIMARY KEY AUTO_INCREMENT,
    accession VARCHAR(50),
    `key` VARCHAR(50),
    location VARCHAR(100),
    gene VARCHAR(50),
    product TEXT,
    translation TEXT,
    note TEXT,
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Reference
(
    ref_id INT PRIMARY KEY AUTO_INCREMENT,
    accession VARCHAR(50),
    title TEXT,
    journal VARCHAR(255),
    year INT,
    pmid VARCHAR(50),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Author
(
    author_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    affiliation VARCHAR(255)
);

CREATE TABLE Ref_Sequence
(
    ref_id INT,
    accession VARCHAR(50),
    PRIMARY KEY (ref_id, accession),
    FOREIGN KEY (ref_id) REFERENCES Reference(ref_id),
    FOREIGN KEY (accession) REFERENCES Sequence(accession)
);

CREATE TABLE Ref_Author
(
    ref_id INT,
    author_id INT,
    PRIMARY KEY (ref_id, author_id),
    FOREIGN KEY (ref_id) REFERENCES Reference(ref_id),
    FOREIGN KEY (author_id) REFERENCES Author(author_id)
);


-- ============================================================
-- Module 03: Extension schema
-- Purpose: Create auth, review, audit, and workflow tables.
-- Prerequisite: Modules 01-02 have created core schema.
-- Suggested execution order: 03
-- ============================================================

USE purple_bank;

CREATE TABLE app_user
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    username VARCHAR(64) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('user', 'admin') NOT NULL DEFAULT 'user',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY UK_app_user_username (username)
);

CREATE TABLE sequence_review
(
    accession VARCHAR(50) NOT NULL,
    seq_status TINYINT NOT NULL DEFAULT 1 COMMENT '0=pending,1=approved,2=rejected',
    submitter VARCHAR(100) NOT NULL DEFAULT 'seed_script',
    submit_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (accession),
    CONSTRAINT FK_sequence_review_accession
        FOREIGN KEY (accession) REFERENCES Sequence(accession) ON DELETE CASCADE,
    CONSTRAINT CHK_sequence_review_status
        CHECK (seq_status IN (0, 1, 2))
);

CREATE TABLE sequence_operation_log
(
    log_id BIGINT NOT NULL AUTO_INCREMENT,
    accession VARCHAR(50) NOT NULL,
    operate_type VARCHAR(30) NOT NULL,
    operator_name VARCHAR(100) NOT NULL DEFAULT 'system',
    operate_desc VARCHAR(255),
    operate_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (log_id),
    CONSTRAINT FK_sequence_operation_log_accession
        FOREIGN KEY (accession) REFERENCES Sequence(accession) ON DELETE CASCADE
);

CREATE TABLE sequence_change_request
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    action_type ENUM('CREATE', 'UPDATE', 'DELETE') NOT NULL,
    target_accession VARCHAR(50) NOT NULL,
    payload_json JSON NULL,
    reason VARCHAR(255) NULL,
    status ENUM('PENDING', 'APPROVED', 'REJECTED') NOT NULL DEFAULT 'PENDING',
    requester_id BIGINT NOT NULL,
    reviewer_id BIGINT NULL,
    review_comment VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at DATETIME NULL,
    PRIMARY KEY (id),
    CONSTRAINT FK_sequence_change_request_requester
        FOREIGN KEY (requester_id) REFERENCES app_user(id),
    CONSTRAINT FK_sequence_change_request_reviewer
        FOREIGN KEY (reviewer_id) REFERENCES app_user(id)
);


-- ============================================================
-- Module 04: Indexes
-- Purpose: Create performance and search indexes.
-- Prerequisite: Modules 01-03 have created related tables.
-- Suggested execution order: 04
-- ============================================================

USE purple_bank;

CREATE INDEX IDX_sequence_review_status_time
    ON sequence_review(seq_status, submit_time);

CREATE INDEX IDX_sequence_operation_log_accession_time
    ON sequence_operation_log(accession, operate_time);

CREATE INDEX IDX_sequence_change_request_status_time
    ON sequence_change_request(status, created_at);

CREATE INDEX IDX_sequence_change_request_target_status
    ON sequence_change_request(target_accession, status);

CREATE INDEX IDX_sequence_change_request_requester_time
    ON sequence_change_request(requester_id, created_at);

CREATE FULLTEXT INDEX FT_sequence_definition
    ON Sequence(definition);


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


-- ============================================================
-- Module 06: Triggers
-- Purpose: Create validation and audit triggers for Sequence.
-- Prerequisite: Modules 01-05 are complete.
-- Suggested execution order: 06
-- ============================================================

USE purple_bank;

DROP TRIGGER IF EXISTS TRG_before_insert_sequence_validate;
DROP TRIGGER IF EXISTS TRG_before_update_sequence_validate;
DROP TRIGGER IF EXISTS TRG_after_insert_sequence;

DELIMITER //
CREATE TRIGGER TRG_before_insert_sequence_validate
BEFORE INSERT ON Sequence
FOR EACH ROW
BEGIN
    IF NEW.organism_id IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'organism_id is required';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM Organism WHERE organism_id = NEW.organism_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'organism_id does not exist';
    END IF;

    IF NEW.sequence IS NULL OR CHAR_LENGTH(TRIM(NEW.sequence)) < 10 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sequence content must be non-empty and length >= 10';
    END IF;

    IF REGEXP_LIKE(UPPER(NEW.sequence), '[^ATCGUN]') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sequence contains invalid nucleotide characters';
    END IF;

    SET NEW.length = CHAR_LENGTH(NEW.sequence);
END//

CREATE TRIGGER TRG_before_update_sequence_validate
BEFORE UPDATE ON Sequence
FOR EACH ROW
BEGIN
    IF NEW.sequence IS NULL OR CHAR_LENGTH(TRIM(NEW.sequence)) < 10 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sequence content must be non-empty and length >= 10';
    END IF;

    IF REGEXP_LIKE(UPPER(NEW.sequence), '[^ATCGUN]') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'sequence contains invalid nucleotide characters';
    END IF;

    SET NEW.length = CHAR_LENGTH(NEW.sequence);
END//

CREATE TRIGGER TRG_after_insert_sequence
AFTER INSERT ON Sequence
FOR EACH ROW
BEGIN
    INSERT INTO sequence_review(accession, seq_status, submitter)
    VALUES (NEW.accession, 1, 'approval_engine')
    ON DUPLICATE KEY UPDATE
        seq_status = VALUES(seq_status),
        submitter = VALUES(submitter),
        updated_time = CURRENT_TIMESTAMP;

    INSERT INTO sequence_operation_log(accession, operate_type, operator_name, operate_desc)
    VALUES (
        NEW.accession,
        'insert',
        'approval_engine',
        CONCAT('new sequence inserted (organism_id=', NEW.organism_id, ')')
    );
END//
DELIMITER ;


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


-- ============================================================
-- Module 08: Stored procedures
-- Purpose: Execute reviewed sequence update in one controlled routine.
-- Prerequisite: Modules 01-07 are complete.
-- Suggested execution order: 08
-- ============================================================

USE purple_bank;

DROP PROCEDURE IF EXISTS sp_update_sequence_reviewed;

DELIMITER //
CREATE PROCEDURE sp_update_sequence_reviewed(
    IN p_accession VARCHAR(50),
    IN p_version VARCHAR(50),
    IN p_locus VARCHAR(50),
    IN p_definition TEXT,
    IN p_organism_id INT,
    IN p_mol_type VARCHAR(50),
    IN p_sequence LONGTEXT,
    IN p_feature_gene VARCHAR(50),
    IN p_feature_product TEXT,
    IN p_feature_location VARCHAR(100),
    IN p_feature_note TEXT
)
BEGIN
    DECLARE v_seq_exists INT DEFAULT 0;
    DECLARE v_feature_exists INT DEFAULT 0;

    SELECT COUNT(*) INTO v_seq_exists
    FROM Sequence
    WHERE accession = p_accession;

    IF v_seq_exists = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'accession does not exist';
    END IF;

    UPDATE Sequence
    SET
        version = COALESCE(p_version, version),
        locus = COALESCE(p_locus, locus),
        definition = COALESCE(p_definition, definition),
        organism_id = COALESCE(p_organism_id, organism_id),
        mol_type = COALESCE(p_mol_type, mol_type),
        sequence = COALESCE(p_sequence, sequence)
    WHERE accession = p_accession;

    IF p_feature_gene IS NOT NULL
       OR p_feature_product IS NOT NULL
       OR p_feature_location IS NOT NULL
       OR p_feature_note IS NOT NULL THEN

        SELECT COUNT(*) INTO v_feature_exists
        FROM Feature
        WHERE accession = p_accession AND `key` = 'gene';

        IF v_feature_exists > 0 THEN
            UPDATE Feature
            SET
                gene = COALESCE(p_feature_gene, gene),
                product = COALESCE(p_feature_product, product),
                location = COALESCE(p_feature_location, location),
                note = COALESCE(p_feature_note, note)
            WHERE accession = p_accession AND `key` = 'gene';
        ELSE
            INSERT INTO Feature(accession, `key`, location, gene, product, note)
            VALUES(
                p_accession,
                'gene',
                COALESCE(p_feature_location, 'unknown'),
                COALESCE(p_feature_gene, 'unknown'),
                p_feature_product,
                p_feature_note
            );
        END IF;
    END IF;

    INSERT INTO sequence_operation_log(accession, operate_type, operator_name, operate_desc)
    VALUES (
        p_accession,
        'update_approved',
        'review_engine',
        'sequence updated by approved change request'
    );
END//
DELIMITER ;


-- ============================================================
-- Module 09: User change request workflow
-- Purpose: Support admin-submitted user create/delete requests with review.
-- Prerequisite: Modules 01-08 are complete.
-- Suggested execution order: 09
-- ============================================================

USE purple_bank;

CREATE TABLE IF NOT EXISTS user_change_request
(
    id BIGINT NOT NULL AUTO_INCREMENT,
    action_type ENUM('CREATE', 'DELETE') NOT NULL,
    payload_json JSON NOT NULL,
    reason VARCHAR(255) NULL,
    status ENUM('PENDING', 'APPROVED', 'REJECTED') NOT NULL DEFAULT 'PENDING',
    requester_id BIGINT NOT NULL,
    reviewer_id BIGINT NULL,
    review_comment VARCHAR(255) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at DATETIME NULL,
    PRIMARY KEY (id),
    CONSTRAINT FK_user_change_request_requester
        FOREIGN KEY (requester_id) REFERENCES app_user(id),
    CONSTRAINT FK_user_change_request_reviewer
        FOREIGN KEY (reviewer_id) REFERENCES app_user(id)
);

DROP INDEX IF EXISTS IDX_user_change_request_status_time ON user_change_request;
CREATE INDEX IDX_user_change_request_status_time
    ON user_change_request(status, created_at);

DROP INDEX IF EXISTS IDX_user_change_request_requester_time ON user_change_request;
CREATE INDEX IDX_user_change_request_requester_time
    ON user_change_request(requester_id, created_at);

