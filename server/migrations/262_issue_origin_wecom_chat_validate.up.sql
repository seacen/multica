-- Validate the widened issue_origin_type_check that 261 added NOT VALID. Takes
-- SHARE UPDATE EXCLUSIVE while scanning issue, so normal traffic continues —
-- the point of splitting the two steps across files (259/260, 197/198).
--
-- 261 only WIDENED the allowed set, so every pre-existing row already
-- satisfies the constraint and this scan cannot fail on legacy data.
ALTER TABLE issue VALIDATE CONSTRAINT issue_origin_type_check;
