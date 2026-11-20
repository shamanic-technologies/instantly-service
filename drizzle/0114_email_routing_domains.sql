-- PF-CLOUDFLARE-1120: domains whose inbound is routed to the staff Gmail mirror
-- through Cloudflare Email Routing (catch-all forward), so replies keep being
-- filed after their mailboxes are gone. `mx_switch_after` = when the root MX may
-- move to Cloudflare (an operator fact, set by POST
-- /internal/self-send/email-routing/schedule); `mx_switched_at` = when it did;
-- `live_since` = routing enabled + ready, catch-all ours, destination verified.
-- Written by `self-send/email-routing.ts` only.
CREATE TABLE IF NOT EXISTS email_routing_domains (
  domain text PRIMARY KEY,
  zone_id text,
  destination text NOT NULL,
  rule_ready_at timestamptz,
  mx_switch_after timestamptz,
  mx_switched_at timestamptz,
  live_since timestamptz,
  last_error text,
  checked_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Which DNS host wrote (or would write) an SES domain's records, read from its NS
-- each pass: `gandi` | `cloudflare` | `unsupported`.
ALTER TABLE ses_sending_domains ADD COLUMN IF NOT EXISTS dns_host text;
