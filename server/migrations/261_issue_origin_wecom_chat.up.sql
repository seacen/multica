-- Extend issue.origin_type for issues created through the WeCom smart bot's
-- /issue command. The shared channel Router stamps origin_type='wecom_chat'
-- and origin_id=<chat_session.id>; without this CHECK entry every WeCom issue
-- creation fails with SQLSTATE 23514.
--
-- The list carries dingtalk_chat forward from 259: rebuilding the constraint
-- restates every allowed value, so leaving one out would silently revoke it.
--
-- Split NOT VALID here / VALIDATE in 262, following 259/260 and the original
-- 197/198 pattern: adding a validating CHECK takes ACCESS EXCLUSIVE on issue
-- for the whole scan, and issue is a hot core table. The runner hands each
-- file to one conn.Exec, so both statements would share an implicit
-- transaction and the strong lock would be held through the scan — which is
-- why the VALIDATE lives in its own file rather than below.
ALTER TABLE issue DROP CONSTRAINT IF EXISTS issue_origin_type_check;
ALTER TABLE issue ADD CONSTRAINT issue_origin_type_check
    CHECK (origin_type IN ('autopilot', 'quick_create', 'lark_chat', 'slack_chat', 'agent_create', 'dingtalk_chat', 'wecom_chat'))
    NOT VALID;
