PurpleBank demo screenshots
Generated at: 2026-05-20T12:57:24.861945

ORGANISM_ID_VALID=13596
ACCESSION_UPDATE_OK=DEMO_CREATE_OK_001
ADMIN_PASSWORD_RESET_TO=admin12345

01_tx_delete_success.png : DELETE审批成功且多表计数为0
02_tx_rollback_failure.png : 中途报错后rollback，计数恢复
03_trigger_create_success.png : 触发器合法插入成功，length/review/log正常
04_trigger_create_fail.png : 非法序列触发器报错，且无脏数据
05_sp_update_success.png : 存储过程更新成功，字段/feature/log符合预期
06_sp_update_fail.png : 不存在accession报错，且无新增日志
07_view_query_consistency.png : 列表与详情API同SQL视图对照一致
