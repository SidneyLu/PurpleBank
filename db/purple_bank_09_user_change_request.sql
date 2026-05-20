-- ============================================================
-- Module 09: User change request workflow
-- Purpose: Support admin-submitted user create/delete requests with review.
-- Prerequisite: Modules 01-08 are complete.
-- Suggested execution order: 09
--
-- Affected tables:
--   user_change_request (new)
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

SET @idx_status_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'user_change_request'
      AND index_name = 'IDX_user_change_request_status_time'
);
SET @sql_idx_status = IF(
    @idx_status_exists = 0,
    'CREATE INDEX IDX_user_change_request_status_time ON user_change_request(status, created_at)',
    'SELECT 1'
);
PREPARE stmt_idx_status FROM @sql_idx_status;
EXECUTE stmt_idx_status;
DEALLOCATE PREPARE stmt_idx_status;

SET @idx_requester_exists = (
    SELECT COUNT(*)
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'user_change_request'
      AND index_name = 'IDX_user_change_request_requester_time'
);
SET @sql_idx_requester = IF(
    @idx_requester_exists = 0,
    'CREATE INDEX IDX_user_change_request_requester_time ON user_change_request(requester_id, created_at)',
    'SELECT 1'
);
PREPARE stmt_idx_requester FROM @sql_idx_requester;
EXECUTE stmt_idx_requester;
DEALLOCATE PREPARE stmt_idx_requester;
