-- Revert to the pre-wecom_chat origin list (dingtalk_chat stays). Existing
-- wecom_chat rows must be deleted or relabeled before this rollback succeeds.
--
-- VALIDATEs in this same file, unlike the up path: narrowing a CHECK can
-- genuinely be violated by existing data, so this must fail closed while
-- wecom_chat rows remain rather than leave a constraint the rows contradict.
-- Same reasoning as 259_issue_origin_dingtalk_chat.down.sql.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat'))
    NOT VALID;
ALTER TABLE issue VALIDATE CONSTRAINT issue_origin_type_check;
