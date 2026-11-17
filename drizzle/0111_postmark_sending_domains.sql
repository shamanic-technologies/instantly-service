-- POSTMARK-COLD-1117: sending domains whose mail to prospects leaves through
-- Postmark (server "Cold placement test", key-service `postmark-cold-placement`)
-- instead of the mailbox's own SMTP relay. A domain sends through Postmark only
-- once `live_since` is set (DKIM + Return-Path verified at Postmark). Replies
-- keep arriving in the mailbox (MX untouched). Written by
-- `self-send/postmark-domains.ts` only.
CREATE TABLE IF NOT EXISTS postmark_sending_domains (
  domain text PRIMARY KEY,
  postmark_domain_id bigint,
  dkim_verified boolean NOT NULL DEFAULT false,
  return_path_verified boolean NOT NULL DEFAULT false,
  dns_written_at timestamp,
  live_since timestamp,
  last_error text,
  checked_at timestamp NOT NULL DEFAULT now(),
  created_at timestamp NOT NULL DEFAULT now()
);

-- Postmark's own bounce reports, one row per Postmark bounce id (bronze).
-- `filed_as` says what we made of it: an imap_messages_raw `bounce`/`delay`
-- row (judged by the DSN sweep like any DSN), an unsubscribe, or nothing.
CREATE TABLE IF NOT EXISTS postmark_bounces_raw (
  bounce_id bigint PRIMARY KEY,
  postmark_message_id text NOT NULL,
  type text NOT NULL,
  email text,
  bounced_at timestamp,
  payload jsonb NOT NULL,
  filed_as text NOT NULL,
  instantly_campaign_id text,
  step integer,
  polled_at timestamp NOT NULL DEFAULT now()
);

-- A Postmark bounce names the send by Postmark's own message id, which the send
-- stored as `response = 'postmark:<id>'`. Partial: only Postmark rows.
CREATE INDEX IF NOT EXISTS smtp_dispatch_raw_postmark_response_idx
  ON smtp_dispatch_raw (response) WHERE response LIKE 'postmark:%';
