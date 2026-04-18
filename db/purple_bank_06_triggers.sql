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
