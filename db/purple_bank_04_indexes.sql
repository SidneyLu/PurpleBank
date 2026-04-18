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
