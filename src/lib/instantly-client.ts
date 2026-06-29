/**
 * HTTP client for Instantly.ai API V2
 * https://developer.instantly.ai/
 */

const INSTANTLY_API_URL = "https://api.instantly.ai/api/v2";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface Campaign {
  id: string;
  name: string;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface Lead {
  email: string;
  first_name?: string;
  last_name?: string;
  company_name?: string;
  variables?: Record<string, string>;
}

export interface Account {
  email: string;
  warmup_status: number;
  status: number;
  first_name?: string;
  last_name?: string;
  signature?: string;
  stat_warmup_score?: number;
  daily_limit?: number;
}

interface PaginatedResponse<T> {
  items: T[];
  next_starting_after?: string;
}

// NAMING COLLISION: Instantly's `contacted_count` / `new_leads_contacted_count`
// represent leads that Instantly DISPATCHED an email to (their stage 3, our
// "sent"). This is NOT our internal `contacted` stage (stage 2 — lead pushed
// into Instantly via POST /send). Wire-format field names are preserved here;
// callers must read them as "dispatched" counts.
export interface CampaignAnalytics {
  campaign_id: string;
  campaign_name: string;
  campaign_status: number;
  leads_count: number;
  /** Instantly's count of leads they have dispatched an email to (= our stage 3 "sent"). */
  contacted_count: number;
  emails_sent_count: number;
  /** New leads Instantly dispatched in the latest window (= delta of stage 3 "sent"). */
  new_leads_contacted_count: number;
  open_count: number;
  open_count_unique: number;
  reply_count: number;
  link_click_count: number;
  bounced_count: number;
  unsubscribed_count: number;
  completed_count: number;
}

export interface CampaignStepAnalytics {
  step: string | null;
  variant: string | null;
  sent: number;
  opened: number;
  unique_opened: number;
  replies: number;
  unique_replies: number;
  replies_automatic: number;
  unique_replies_automatic: number;
  clicks: number;
  unique_clicks: number;
}

export interface LeadFull {
  id: string;
  email: string;
  campaign?: string;
  status?: number;
  email_open_count?: number;
  email_reply_count?: number;
  email_click_count?: number;
  email_opened_step?: number | null;
  email_opened_variant?: number | null;
  email_replied_step?: number | null;
  email_clicked_step?: number | null;
  email_clicked_variant?: number | null;
  timestamp_last_contact?: string | null;
  timestamp_last_open?: string | null;
  timestamp_last_click?: string | null;
  timestamp_last_reply?: string | null;
  timestamp_last_interest_change?: string | null;
  lt_interest_status?: number | null;
  [key: string]: unknown;
}

export interface EmailRecord {
  id: string;
  campaign_id: string | null;
  lead: string | null;
  lead_id: string | null;
  eaccount: string;
  ue_type: 1 | 2 | 3 | 4;
  step: string | null;
  subject?: string;
  timestamp_email: string;
  timestamp_created?: string;
  [key: string]: unknown;
}

export interface SequenceStep {
  subject: string;
  bodyHtml: string;
  daysSinceLastStep: number;
}

export interface CreateCampaignParams {
  name: string;
  steps: SequenceStep[];
}

/**
 * Wire-format step shape accepted by Instantly's PATCH /campaigns/{id} for
 * the `sequences` field. Distinct from our internal `SequenceStep` (which
 * uses `bodyHtml` / `daysSinceLastStep`); this mirrors Instantly's V2 schema
 * 1:1 so callers can round-trip a `getCampaign` response back to PATCH.
 */
export interface InstantlySequenceStep {
  delay?: number;
  variants?: Array<{
    subject?: string;
    body?: string;
    v_disabled?: boolean;
  }>;
}

export interface UpdateCampaignParams {
  email_list?: string[];
  bcc_list?: string[];
  open_tracking?: boolean;
  link_tracking?: boolean;
  insert_unsubscribe_header?: boolean;
  stop_on_reply?: boolean;
  sequences?: Array<{ steps: InstantlySequenceStep[] }>;
}

export interface AddLeadsParams {
  campaign_id: string;
  leads: Lead[];
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Per-path throttling: Instantly caps `/emails` at 20 req/min per workspace
// and the general API at ~600 req/min. We serialize requests through a
// per-slot "next-call-allowed-at" timestamp so concurrent workers (reconcile
// loops, retry-stuck batches) pace below the cap regardless of how many
// promises run in parallel. The slot is selected by `slotForPath()`.
//   /emails:   3500ms ≈ 17.1 req/min (safety margin under 20/min cap)
//   general:   110ms  ≈ 545 req/min  (safety margin under 600/min cap)
//
// Margin reasoning: a single replica with 3500ms cadence emits 17 calls per
// rolling 60s window. The previous 3100ms cadence was too tight — combined
// with retries-not-going-through-throttle (fixed below), it crossed the
// sliding-window edge and tripped 429s in prod 2026-05-26.
interface ThrottleSlot {
  nextAllowedAt: number;
  minIntervalMs: number;
}

const emailsSlot: ThrottleSlot = { nextAllowedAt: 0, minIntervalMs: 3500 };
const generalSlot: ThrottleSlot = { nextAllowedAt: 0, minIntervalMs: 110 };

function slotForPath(path: string): ThrottleSlot {
  return path.startsWith("/emails") ? emailsSlot : generalSlot;
}

async function throttle(slot: ThrottleSlot): Promise<void> {
  const now = Date.now();
  const scheduledAt = Math.max(now, slot.nextAllowedAt);
  slot.nextAllowedAt = scheduledAt + slot.minIntervalMs;
  const wait = scheduledAt - now;
  if (wait > 0) await sleep(wait);
}

/**
 * Parse a Retry-After header (RFC 9110). Accepts seconds-from-now (`"5"`) or
 * an HTTP-date. Returns the wait in milliseconds, capped at 60_000ms so a
 * misconfigured upstream cannot stall a worker indefinitely. Returns null
 * when header is absent or unparseable — caller falls back to exponential
 * backoff.
 */
function parseRetryAfter(header: string | null): number | null {
  if (!header) return null;
  const trimmed = header.trim();

  const seconds = Number(trimmed);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.min(seconds * 1000, 60_000);
  }

  const date = Date.parse(trimmed);
  if (Number.isFinite(date)) {
    return Math.min(Math.max(0, date - Date.now()), 60_000);
  }

  return null;
}

async function instantlyRequest<T>(
  apiKey: string,
  path: string,
  options: { method?: string; body?: unknown; retries?: number } = {}
): Promise<T> {
  const { method = "GET", body, retries = 3 } = options;

  const headers: Record<string, string> = {
    Authorization: `Bearer ${apiKey}`,
  };

  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  let lastError: Error | null = null;

  for (let attempt = 0; attempt < retries; attempt++) {
    // Re-pace through the throttle slot on EVERY attempt (including retries).
    // Previously the throttle ran once before the loop, so a 429 retry could
    // fire within the rate-limit window and trip a second 429 — cascading
    // until retries exhausted.
    await throttle(slotForPath(path));

    try {
      const response = await fetch(`${INSTANTLY_API_URL}${path}`, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
      });

      if (response.status === 429 || response.status >= 500) {
        const errorText = await response.text().catch(() => "");
        lastError = new Error(
          `instantly-api ${method} ${path} failed: ${response.status} - ${errorText.slice(0, 500)}`
        );
        if (attempt < retries - 1) {
          const retryAfterMs = parseRetryAfter(response.headers.get("Retry-After"));
          const delay =
            retryAfterMs !== null
              ? retryAfterMs
              : Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 10000);
          await sleep(delay);
        }
        continue;
      }

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`[instantly-api] ${method} ${path} → ${response.status}: ${errorText}`);
        throw new Error(
          `instantly-api ${method} ${path} failed: ${response.status} - ${errorText}`
        );
      }

      return (await response.json()) as T;
    } catch (error) {
      lastError = error as Error;
      if (attempt < retries - 1) {
        const delay = Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 10000);
        await sleep(delay);
      }
    }
  }

  throw lastError || new Error(`instantly-api ${method} ${path} failed after ${retries} retries`);
}

// ─── Campaigns ───────────────────────────────────────────────────────────────

export async function createCampaign(apiKey: string, params: CreateCampaignParams): Promise<Campaign> {
  // Instantly's `delay` on step N means "days to wait AFTER step N before
  // sending step N+1".  Our `daysSinceLastStep` on step N means "days to
  // wait BEFORE step N (since step N-1)".  So step[i].delay must be
  // steps[i+1].daysSinceLastStep, and the last step gets delay 0.
  const instantlySteps = params.steps.map((step, i) => ({
    type: "email" as const,
    delay:
      i < params.steps.length - 1
        ? params.steps[i + 1].daysSinceLastStep
        : 0,
    variants: [
      {
        subject: step.subject,
        body: step.bodyHtml,
      },
    ],
  }));

  const body: Record<string, unknown> = {
    name: params.name,
    campaign_schedule: {
      schedules: [
        {
          name: "Default",
          // Business hours, weekdays only — sending 24/7 (incl. 3am / weekends)
          // is an unnatural pattern that filters read as bulk/spam. Days are
          // Instantly's 0=Sunday..6=Saturday, so Mon-Fri = "1".."5".
          // NOTE: a single fixed timezone, NOT the recipient's — the lead
          // payload carries no timezone/country today. America/Chicago (US
          // Central) is a reasonable default spanning US business hours. To
          // schedule in each prospect's local time, thread a lead timezone
          // through SendOptions and set it here per-campaign (1 campaign = 1 lead).
          timing: { from: "08:00", to: "17:00" },
          days: { "0": false, "1": true, "2": true, "3": true, "4": true, "5": true, "6": false },
          timezone: "America/Chicago",
        },
      ],
    },
    ...(instantlySteps.length > 0 && { sequences: [{ steps: instantlySteps }] }),
  };

  return instantlyRequest<Campaign>(apiKey, "/campaigns", {
    method: "POST",
    body,
  });
}

/**
 * PATCH /campaigns/{id} — assign sending accounts (email_list) to a campaign.
 * Instantly V2 ignores account_ids in create; accounts must be set via PATCH.
 */
export async function updateCampaign(
  apiKey: string,
  campaignId: string,
  params: UpdateCampaignParams
): Promise<Campaign> {
  return instantlyRequest<Campaign>(apiKey, `/campaigns/${campaignId}`, {
    method: "PATCH",
    body: params,
  });
}

export async function getCampaign(apiKey: string, campaignId: string): Promise<Campaign> {
  return instantlyRequest<Campaign>(apiKey, `/campaigns/${campaignId}`);
}

export async function updateCampaignStatus(
  apiKey: string,
  campaignId: string,
  status: "active" | "paused" | "completed"
): Promise<Campaign> {
  const action = status === "active" ? "activate" : "pause";
  return instantlyRequest<Campaign>(apiKey, `/campaigns/${campaignId}/${action}`, {
    method: "POST",
  });
}

// ─── Leads ───────────────────────────────────────────────────────────────────

export async function addLeads(apiKey: string, params: AddLeadsParams): Promise<{ added: number }> {
  let added = 0;
  for (const lead of params.leads) {
    await instantlyRequest<unknown>(apiKey, "/leads", {
      method: "POST",
      body: {
        email: lead.email,
        first_name: lead.first_name,
        last_name: lead.last_name,
        company_name: lead.company_name,
        campaign: params.campaign_id,
        ...(lead.variables && { custom_variables: lead.variables }),
      },
    });
    added++;
  }
  return { added };
}

export async function deleteLeads(
  apiKey: string,
  campaignId: string,
  emails: string[]
): Promise<{ deleted: number }> {
  let deleted = 0;
  for (const email of emails) {
    await instantlyRequest<unknown>(apiKey, "/leads", {
      method: "DELETE",
      body: {
        campaign_id: campaignId,
        delete_list: [email],
      },
    });
    deleted++;
  }
  return { deleted };
}

// ─── Accounts ────────────────────────────────────────────────────────────────

/**
 * Fetch ALL accounts in the workspace. Paginates via `next_starting_after`
 * using the maximum per-page limit Instantly accepts (100 — probed
 * empirically; any value >100 returns an empty `items` array). The loop
 * terminates only when Instantly stops returning a cursor, so this scales
 * to any account count.
 *
 * Historic bug 2026-05-28: the previous non-paginated implementation only
 * saw Instantly's default page (10 items). With 156 active senders in the
 * workspace, 146 were invisible to `pickRandomAccount` — sends + retry-
 * stuck redispatches saturated the first 10 accounts to 30/day while the
 * rest sat idle. Any new "list all of X" helper added to this file MUST
 * follow this pattern (see CLAUDE.md "Instantly client — pagination
 * convention").
 */
export async function listAccounts(apiKey: string): Promise<Account[]> {
  const PAGE_LIMIT = 100;
  const results: Account[] = [];
  let startingAfter: string | undefined;
  for (;;) {
    const query = new URLSearchParams({ limit: String(PAGE_LIMIT) });
    if (startingAfter) query.set("starting_after", startingAfter);
    const response = await instantlyRequest<PaginatedResponse<Account>>(
      apiKey,
      `/accounts?${query.toString()}`,
    );
    const items = response.items ?? [];
    results.push(...items);
    if (!response.next_starting_after || items.length === 0) break;
    startingAfter = response.next_starting_after;
  }
  return results;
}

export async function enableWarmup(apiKey: string, email: string): Promise<Account> {
  const encoded = encodeURIComponent(email);
  return instantlyRequest<Account>(apiKey, `/accounts/${encoded}/warmup`, {
    method: "POST",
    body: { enabled: true },
  });
}

export async function disableWarmup(apiKey: string, email: string): Promise<Account> {
  const encoded = encodeURIComponent(email);
  return instantlyRequest<Account>(apiKey, `/accounts/${encoded}/warmup`, {
    method: "POST",
    body: { enabled: false },
  });
}

export async function getWarmupAnalytics(apiKey: string): Promise<unknown> {
  return instantlyRequest<unknown>(apiKey, "/accounts/warmup/analytics");
}

// ─── Analytics ───────────────────────────────────────────────────────────────

export async function getCampaignAnalytics(apiKey: string, campaignId: string): Promise<CampaignAnalytics | null> {
  const results = await instantlyRequest<CampaignAnalytics[]>(
    apiKey,
    `/campaigns/analytics?id=${encodeURIComponent(campaignId)}`
  );
  return results[0] ?? null;
}

export async function getCampaignStepsAnalytics(
  apiKey: string,
  campaignId: string,
): Promise<CampaignStepAnalytics[]> {
  return instantlyRequest<CampaignStepAnalytics[]>(
    apiKey,
    `/campaigns/analytics/steps?campaign_id=${encodeURIComponent(campaignId)}`,
  );
}

/**
 * POST /leads/list — returns full Lead objects (status, engagement counts,
 * last-step engaged, last reply/open timestamps). Paginates via starting_after.
 */
export async function listLeadsFull(
  apiKey: string,
  campaignId: string,
  limit = 100,
): Promise<LeadFull[]> {
  const results: LeadFull[] = [];
  let startingAfter: string | undefined;
  for (;;) {
    const body: Record<string, unknown> = { campaign: campaignId, limit };
    if (startingAfter) body.starting_after = startingAfter;
    const response = await instantlyRequest<{ items: LeadFull[]; next_starting_after?: string }>(
      apiKey,
      "/leads/list",
      { method: "POST", body },
    );
    const items = response.items ?? [];
    results.push(...items);
    if (!response.next_starting_after || items.length === 0) break;
    startingAfter = response.next_starting_after;
  }
  return results;
}

/**
 * GET /emails — paginated list of individual email records with `step` field.
 * Rate-limited to 20 req/min per workspace by Instantly. Pacing handled in
 * `instantlyRequest` via a process-wide gate on `/emails` paths.
 * `min_timestamp_created` filters to emails created after a cursor.
 */
export async function listEmails(
  apiKey: string,
  params: {
    campaignId: string;
    minTimestampCreated?: string;
    limit?: number;
  },
): Promise<EmailRecord[]> {
  const { campaignId, minTimestampCreated, limit = 100 } = params;
  const results: EmailRecord[] = [];
  let startingAfter: string | undefined;
  for (;;) {
    const query = new URLSearchParams({
      campaign_id: campaignId,
      limit: String(limit),
    });
    if (minTimestampCreated) query.set("min_timestamp_created", minTimestampCreated);
    if (startingAfter) query.set("starting_after", startingAfter);
    const response = await instantlyRequest<{ items: EmailRecord[]; next_starting_after?: string }>(
      apiKey,
      `/emails?${query.toString()}`,
    );
    const items = response.items ?? [];
    results.push(...items);
    if (!response.next_starting_after || items.length === 0) break;
    startingAfter = response.next_starting_after;
  }
  return results;
}
