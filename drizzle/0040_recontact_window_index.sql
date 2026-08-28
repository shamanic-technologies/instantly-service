-- Per-brand re-contact window: serve the send-time lookup's identity match.
--
-- `findRecentBrandContact` (src/lib/recontact-window.ts) matches the prospect
-- on `lower(lead_email)` — the same trim+lowercase normalization the serve
-- path applies, so `Joe@X.com` and `joe@x.com` resolve to one inbox. The plain
-- `instantly_campaigns_lead_email_idx` cannot serve a `lower(...)` predicate,
-- so the lookup would seq-scan the whole table on every send.
--
-- Expression index, hand-written (drizzle's schema builder has no expression
-- form) — do NOT drop it on a `db:generate` diff. `IF NOT EXISTS` so a re-run
-- is a no-op. Non-concurrent is safe here: the table is tens of thousands of
-- rows, so the build is sub-second and the boot window is not at risk.
CREATE INDEX IF NOT EXISTS instantly_campaigns_lead_email_lower_idx
  ON instantly_campaigns (lower(lead_email));
