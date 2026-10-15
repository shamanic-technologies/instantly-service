-- Every Instantly DFY mailbox was cancelled on 2026-10-15 (the Instantly plan on
-- 2026-10-12: its API answers 402, so no sync can ever observe the cancellation).
-- Record it once, by hand, so the infra spend stops counting ~$10/mailbox/month
-- and the 22 DFY accounts stop reading as in_production capacity.
-- Rows are kept as history (never deleted). Idempotent.
UPDATE infra_domains
   SET status = 'cancelled', cancelled_at = now()
 WHERE provider = 'instantly-dfy' AND cancelled_at IS NULL;
--> statement-breakpoint
UPDATE instantly_accounts
   SET absent_since = now()
 WHERE absent_since IS NULL
   AND send_transport = 'instantly'
   AND split_part(lower(email), '@', 2) IN (
     SELECT domain FROM infra_domains WHERE provider = 'instantly-dfy'
   );
