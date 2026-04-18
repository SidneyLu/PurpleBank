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
