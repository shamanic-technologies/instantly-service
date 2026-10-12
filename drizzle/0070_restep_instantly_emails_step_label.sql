-- Instantly's /emails poll labels every email `<sub-sequence>_<0-based step>_<variant>`
-- ("0_1_0" = the 2nd email). Until v0.83.45 (#1084) the poll parser read the
-- first digit, so 5,698 real sends (2026-02-19 .. 2026-08-31, 22 brands) and 26
-- replies were promoted at step 0. This puts each one at its real step, read
-- from the raw label, which stays untouched in instantly_emails_raw.
--
-- The outcome is the one the fixed parser would have produced at promote time
-- (silver-promote insertOrUpgradeSilverEvent, one-shot index = one row per
-- campaign, lead, event type, step):
--   - nothing at the real step yet -> the row moves to the real step;
--   - an INFERRED row holds it     -> that row is upgraded to real in place (id
--                                     kept, the poll's timestamp/source/payload),
--                                     the step-0 row is removed;
--   - a REAL row holds it (the webhook twin of the SAME send) -> the step-0
--     duplicate is removed and the kept row takes the EARLIER timestamp
--     (Instantly's own send time: the webhook lands ~2 s later), so no send
--     is counted twice and no first-sent time moves.
-- A synthetic row projected at step 0 FROM one of those rows follows it the
-- same way (second pass, after the first one settled).
--
-- Every touched row is archived whole in instantly_events_retracted first
-- (action 'retracted' = row removed, 'restepped' = row kept and changed); undo
-- is documented on that table. Idempotent: a re-run finds no step-0 poll row.
CREATE TEMP TABLE step0_fix ON COMMIT DROP AS
SELECT e.id,
       split_part(r.payload->>'step', '_', 2)::int + 1 AS real_step,
       t.id AS twin_id,
       t.inferred AS twin_inferred
FROM instantly_events e
JOIN instantly_emails_raw r ON r.id = e.source_row_id
LEFT JOIN instantly_events t
  ON t.campaign_id = e.campaign_id
 AND t.lead_email IS NOT DISTINCT FROM e.lead_email
 AND t.event_type = e.event_type
 AND t.step = split_part(r.payload->>'step', '_', 2)::int + 1
WHERE e.source = 'poll_emails'
  AND e.step = 0
  AND e.inferred = false
  AND e.event_type IN ('email_sent', 'reply_received')
  AND r.payload->>'step' ~ '^[0-9]+_[0-9]+_[0-9]+$';
--> statement-breakpoint
INSERT INTO instantly_events_retracted (id, event_id, action, reason, event)
SELECT gen_random_uuid()::text, e.id,
       CASE WHEN f.twin_id IS NULL THEN 'restepped' ELSE 'retracted' END,
       'instantly_emails_step_label: stored at step 0, real step ' || f.real_step
         || CASE WHEN f.twin_id IS NULL THEN '' ELSE ', duplicate of ' || f.twin_id END,
       to_jsonb(e)
FROM step0_fix f
JOIN instantly_events e ON e.id = f.id
ON CONFLICT (event_id) DO NOTHING;
--> statement-breakpoint
INSERT INTO instantly_events_retracted (id, event_id, action, reason, event)
SELECT gen_random_uuid()::text, t.id, 'restepped',
       'instantly_emails_step_label: absorbed step-0 duplicate ' || f.id
         || CASE WHEN f.twin_inferred THEN ' (inferred upgraded to real)' ELSE ' (earlier timestamp kept)' END,
       to_jsonb(t)
FROM step0_fix f
JOIN instantly_events t ON t.id = f.twin_id
ON CONFLICT (event_id) DO NOTHING;
--> statement-breakpoint
UPDATE instantly_events t
SET inferred = false,
    source = e.source,
    source_row_id = e.source_row_id,
    "timestamp" = e."timestamp",
    raw_payload = e.raw_payload,
    account_email = COALESCE(e.account_email, t.account_email),
    inferred_from_event_id = NULL,
    inferred_rule = NULL
FROM step0_fix f
JOIN instantly_events e ON e.id = f.id
WHERE t.id = f.twin_id
  AND f.twin_inferred = true;
--> statement-breakpoint
UPDATE instantly_events t
SET "timestamp" = e."timestamp"
FROM step0_fix f
JOIN instantly_events e ON e.id = f.id
WHERE t.id = f.twin_id
  AND f.twin_inferred = false
  AND e."timestamp" < t."timestamp";
--> statement-breakpoint
DELETE FROM instantly_events e
USING step0_fix f
WHERE e.id = f.id
  AND f.twin_id IS NOT NULL;
--> statement-breakpoint
UPDATE instantly_events e
SET step = f.real_step
FROM step0_fix f
WHERE e.id = f.id
  AND f.twin_id IS NULL;
--> statement-breakpoint
-- Second pass: synthetic rows projected at step 0 from a row restepped above.
CREATE TEMP TABLE step0_fix_inferred ON COMMIT DROP AS
SELECT i.id,
       substring(a.reason from 'real step ([0-9]+)')::int AS real_step,
       t.id AS twin_id
FROM instantly_events_retracted a
JOIN instantly_events i
  ON i.inferred_from_event_id = a.event_id
 AND i.inferred = true
 AND i.step = 0
LEFT JOIN instantly_events t
  ON t.campaign_id = i.campaign_id
 AND t.lead_email IS NOT DISTINCT FROM i.lead_email
 AND t.event_type = i.event_type
 AND t.step = substring(a.reason from 'real step ([0-9]+)')::int
WHERE a.reason LIKE 'instantly_emails_step_label: stored at step 0, real step %';
--> statement-breakpoint
INSERT INTO instantly_events_retracted (id, event_id, action, reason, event)
SELECT gen_random_uuid()::text, i.id,
       CASE WHEN f.twin_id IS NULL THEN 'restepped' ELSE 'retracted' END,
       'instantly_emails_step_label: inferred at step 0 from a restepped event, real step ' || f.real_step
         || CASE WHEN f.twin_id IS NULL THEN '' ELSE ', duplicate of ' || f.twin_id END,
       to_jsonb(i)
FROM step0_fix_inferred f
JOIN instantly_events i ON i.id = f.id
ON CONFLICT (event_id) DO NOTHING;
--> statement-breakpoint
DELETE FROM instantly_events i
USING step0_fix_inferred f
WHERE i.id = f.id
  AND f.twin_id IS NOT NULL;
--> statement-breakpoint
UPDATE instantly_events i
SET step = f.real_step
FROM step0_fix_inferred f
WHERE i.id = f.id
  AND f.twin_id IS NULL;
