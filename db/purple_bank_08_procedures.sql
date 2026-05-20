-- ============================================================
-- Module 08: Stored procedures
-- Purpose: Execute reviewed sequence update in one controlled routine.
-- Prerequisite: Modules 01-07 are complete.
-- Suggested execution order: 08
--
-- Procedure: sp_update_sequence_reviewed
-- Inputs:
--   p_accession      target sequence accession
--   p_version        optional new version
--   p_locus          optional new locus
--   p_definition     optional new definition
--   p_organism_id    optional new organism id
--   p_mol_type       optional new molecular type
--   p_sequence       optional new nucleotide sequence
--   p_feature_gene   optional feature gene update
--   p_feature_product optional feature product update
--   p_feature_location optional feature location update
--   p_feature_note   optional feature note update
-- Failure behavior:
--   - raises SQL exception when accession does not exist
-- Affected tables:
--   Sequence, Feature, sequence_operation_log
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
