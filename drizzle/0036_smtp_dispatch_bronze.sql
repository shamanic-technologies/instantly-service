-- Bronze mirror of what an SMTP server said about a message we dispatched
-- ourselves (issue #590, PR 2). Empty on creation, written only by the self-send
-- path, which no account is on yet — so this is inert until a send_transport flip.
--
-- A genuinely PARALLEL bronze source to Instantly's webhooks, not a replacement:
-- both promote into the same silver (instantly_events), exactly as the webhook
-- path and the reconcile-poll path already converge on promoteEvent. Named for
-- the transport rather than the vendor, because these are not Instantly payloads.
--
-- One row per dispatch ATTEMPT, success or failure, so a refused step leaves the
-- same evidence trail as one that went out. outcome is 'sent' | 'permanent' |
-- 'transient' — the SMTP reply class, which is the discriminator this codebase
-- already uses for account health (5xx is a real rejection, 4xx is retryable).
CREATE TABLE IF NOT EXISTS "smtp_dispatch_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "account_email" text NOT NULL,
  "step" integer NOT NULL,
  "outcome" text NOT NULL,
  -- Message-Id the server accepted: threads the NEXT step of this sequence and
  -- correlates an async DSN bounce back to the dispatch that caused it.
  "message_id" text,
  "response_code" integer,
  -- The reply line verbatim. Bronze keeps what the server said, not our reading
  -- of it — the classification lives in "outcome" beside it.
  "response" text,
  "payload" jsonb NOT NULL,
  "dispatched_at" timestamp DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "smtp_dispatch_raw_campaign_idx"
  ON "smtp_dispatch_raw" ("instantly_campaign_id");
CREATE INDEX IF NOT EXISTS "smtp_dispatch_raw_account_idx"
  ON "smtp_dispatch_raw" ("account_email");
CREATE INDEX IF NOT EXISTS "smtp_dispatch_raw_dispatched_at_idx"
  ON "smtp_dispatch_raw" ("dispatched_at");
CREATE INDEX IF NOT EXISTS "smtp_dispatch_raw_message_id_idx"
  ON "smtp_dispatch_raw" ("message_id");

-- Bronze: an HTTP hit from a recipient on a link we minted (opt-out click now;
-- open pixel and click redirect once tracking lands).
--
-- Bronze because it is an external request, and because promoteEvent requires
-- real provenance — a silver event points at the bronze row that caused it, so an
-- unsubscribe cannot be promoted without recording the hit first. One "kind"
-- column rather than a table per link type: they are the same kind of fact (a
-- recipient hit a URL we signed) and differ only in which one.
CREATE TABLE IF NOT EXISTS "tracking_hits_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "kind" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "step" integer,
  -- An opt-out is only ACTED on for POST: corporate link scanners fetch every URL
  -- in an inbound email, so acting on GET would unsubscribe prospects who never
  -- clicked. The GET is still recorded — it is a real hit, and it is the evidence
  -- that the scanner arrived before the human did.
  "method" text,
  "user_agent" text,
  "payload" jsonb NOT NULL,
  "received_at" timestamp DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "tracking_hits_raw_campaign_idx"
  ON "tracking_hits_raw" ("instantly_campaign_id");
CREATE INDEX IF NOT EXISTS "tracking_hits_raw_received_at_idx"
  ON "tracking_hits_raw" ("received_at");
