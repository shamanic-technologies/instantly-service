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

export interface CampaignAnalytics {
  campaign_id: string;
  campaign_name: string;
  campaign_status: number;
  leads_count: number;
  contacted_count: number;
  emails_sent_count: number;
  new_leads_contacted_count: number;
  open_count: number;
  open_count_unique: number;
  reply_count: number;
  link_click_count: number;
  bounced_count: number;
  unsubscribed_count: number;
  completed_count: number;
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

export interface UpdateCampaignParams {
  email_list?: string[];
  bcc_list?: string[];
  open_tracking?: boolean;
  link_tracking?: boolean;
  insert_unsubscribe_header?: boolean;
  stop_on_reply?: boolean;
}

export interface AddLeadsParams {
  campaign_id: string;
  leads: Lead[];
}

// ─── HTTP helpers ────────────────────────────────────────────────────────────

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    try {
      const response = await fetch(`${INSTANTLY_API_URL}${path}`, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
      });

      if (response.status === 429 || response.status >= 500) {
        const delay = Math.min(1000 * Math.pow(2, attempt) + Math.random() * 1000, 10000);
        await sleep(delay);
        continue;
      }

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`[instantly-api] ${method} ${path} → ${response.status}: ${errorText}`);
        throw new Error(
          `instantly-api ${method} ${path} failed: ${response.status} - ${errorText}`
        );
      }

      const json = await response.json() as T;
      console.log(`[instantly-api] ${method} ${path} → ${response.status}`, JSON.stringify(json).slice(0, 500));
      return json;
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
          timing: { from: "00:00", to: "23:59" },
          days: { "0": true, "1": true, "2": true, "3": true, "4": true, "5": true, "6": true },
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

export async function listCampaigns(apiKey: string, limit = 100, skip = 0): Promise<Campaign[]> {
  return instantlyRequest<Campaign[]>(apiKey, `/campaigns?limit=${limit}&skip=${skip}`);
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

export async function listLeads(
  apiKey: string,
  campaignId: string,
  limit = 100,
  skip = 0
): Promise<Lead[]> {
  const response = await instantlyRequest<{ items: Lead[] }>(apiKey, "/leads/list", {
    method: "POST",
    body: {
      campaign: campaignId,
      limit,
      skip,
    },
  });
  return response.items ?? [];
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

export async function listAccounts(apiKey: string): Promise<Account[]> {
  const response = await instantlyRequest<PaginatedResponse<Account>>(apiKey, "/accounts");
  return response.items;
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
