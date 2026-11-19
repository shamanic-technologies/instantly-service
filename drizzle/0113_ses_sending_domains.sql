-- SES-COLD-1119: sending domains whose mail to people leaves through Amazon
-- SES (marketing account, key-service `aws-ses-marketing`) instead of the
-- mailbox's relay or Postmark. A domain sends through SES only while
-- `live_since` is set (identity verified for sending, Easy DKIM SUCCESS,
-- custom MAIL FROM SUCCESS, cold events confirmed on our SNS topic); a lost
-- verification clears it. Replies keep arriving in the mailbox (root MX/SPF
-- untouched). Written by `self-send/ses-domains.ts` only.
CREATE TABLE IF NOT EXISTS ses_sending_domains (
  domain text PRIMARY KEY,
  dkim_tokens jsonb,
  dkim_status text,
  mail_from_domain text,
  mail_from_status text,
  verified_for_sending boolean NOT NULL DEFAULT false,
  dns_written_at timestamp,
  live_since timestamp,
  last_error text,
  checked_at timestamp NOT NULL DEFAULT now(),
  created_at timestamp NOT NULL DEFAULT now()
);

-- SES bounce / complaint / delay events of cold mail as SNS delivered them
-- (bronze, one row per SNS message id: a redelivery is a no-op). `filed_as`
-- says what we made of it (`bounce` / `delay` imap_messages_raw row, an
-- `unsubscribe`, or `ignored`).
CREATE TABLE IF NOT EXISTS ses_events_raw (
  sns_message_id text PRIMARY KEY,
  event_type text NOT NULL,
  ses_message_id text,
  recipient text,
  payload jsonb NOT NULL,
  filed_as text NOT NULL,
  instantly_campaign_id text,
  step integer,
  received_at timestamp NOT NULL DEFAULT now()
);
