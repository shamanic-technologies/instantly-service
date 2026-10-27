-- BOX-DB-LOAD-1027 (round 2): serve the send QUEUE reads and the inbound-blindness
-- read from small indexes instead of whole-table work.
--
-- The queue is ~3.2k `provisioned` holds out of ~443k `sequence_costs` rows. Every
-- read of it (stats `emailStats.queued`, the status read's queued/finished
-- EXISTS, the capacity snapshot behind send selection) probed the full-table
-- indexes and fetched heap rows to drop the 99 % that are not provisioned:
-- 26,734 heap blocks for one fleet stats read (3.0 s), 2,449 with these. The
-- capacity reads also seq-scanned the table to join on `lead_email`.
--
-- The inbound-blindness read (`loadAddressInboundRows`, every send path) read the
-- 14-day window of `imap_messages_raw` (3.7 GB) off the heap to test
-- `payload ? 'recoveredFrom'`: 9-12 s a call. With the partial index it reads
-- each sending address's latest message backward, index only: 37 ms.
--
-- The predicates are the readers' literals: change one, change both (guard in
-- tests/unit/migrations.test.ts).
--
-- Pre-built CONCURRENTLY out of band on prod (2026-10-27: 2.4 s, 0.7 s, 0.9 s,
-- 4.5 s and 116 s), so these IF NOT EXISTS are no-ops there; the boot migrator
-- runs in a transaction, which forbids CONCURRENTLY. Any new prod-scale
-- environment pre-builds them CONCURRENTLY before deploy. Hand-written; declared
-- in schema.ts with `.where`.
CREATE INDEX IF NOT EXISTS sequence_costs_provisioned_campaign_idx
  ON sequence_costs (instantly_campaign_id, step)
  WHERE status = 'provisioned';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS sequence_costs_provisioned_legacy_idx
  ON sequence_costs (campaign_id, lead_email, step)
  WHERE status = 'provisioned' AND instantly_campaign_id IS NULL;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS sequence_costs_provisioned_lead_idx
  ON sequence_costs (lead_email)
  WHERE status = 'provisioned';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS sequence_costs_lead_email_idx
  ON sequence_costs (lead_email);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS imap_messages_raw_inbound_account_idx
  ON imap_messages_raw (account_email, polled_at)
  WHERE NOT (payload ? 'recoveredFrom');
