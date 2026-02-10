/**
 * HTTP client for Instantly.ai API V2
 * https://developer.instantly.ai/
 */

const INSTANTLY_API_URL = "https://api.instantly.ai/api/v2";

function getApiKey(): string {
  const key = process.env.INSTANTLY_API_KEY;
  if (!key) {
    throw new Error("INSTANTLY_API_KEY environment variable is not set");
  }
  return key;
}

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
  stat_warmup_score?: number;
  daily_limit?: number;
}

interface PaginatedResponse<T> {
  items: T[];
  next_starting_after?: string;
}

export interface CampaignAnalytics {
  campaign_id: string;
  total_leads: number;
  contacted: number;
  opened: number;
  replied: number;
  bounced: number;
  unsubscribed: number;
}

export interface EmailContent {
  subject: string;
  body: string;
}

export interface CreateCampaignParams {
  name: string;
  email?: EmailContent;
}

export interface UpdateCampaignParams {
  email_list?: string[];
  bcc_list?: string[];
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
  path: string,
  options: { method?: string; body?: unknown; retries?: number } = {}
): Promise<T> {
  const { method = "GET", body, retries = 3 } = options;

  const headers: Record<string, string> = {
    Authorization: `Bearer ${getApiKey()}`,
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

export async function createCampaign(params: CreateCampaignParams): Promise<Campaign> {
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
  };

  // Add email sequence if content provided
  if (params.email) {
    body.sequences = [
      {
        steps: [
          {
            type: "email",
            delay: 0,
            variants: [
              {
                subject: params.email.subject,
                body: params.email.body,
              },
            ],
          },
        ],
      },
    ];
  }

  return instantlyRequest<Campaign>("/campaigns", {
    method: "POST",
    body,
  });
}

/**
 * PATCH /campaigns/{id} — assign sending accounts (email_list) to a campaign.
 * Instantly V2 ignores account_ids in create; accounts must be set via PATCH.
 */
export async function updateCampaign(
  campaignId: string,
  params: UpdateCampaignParams
): Promise<Campaign> {
  return instantlyRequest<Campaign>(`/campaigns/${campaignId}`, {
    method: "PATCH",
    body: params,
  });
}

export async function getCampaign(campaignId: string): Promise<Campaign> {
  return instantlyRequest<Campaign>(`/campaigns/${campaignId}`);
}

export async function listCampaigns(limit = 100, skip = 0): Promise<Campaign[]> {
  return instantlyRequest<Campaign[]>(`/campaigns?limit=${limit}&skip=${skip}`);
}

export async function updateCampaignStatus(
  campaignId: string,
  status: "active" | "paused" | "completed"
): Promise<Campaign> {
  const action = status === "active" ? "activate" : "pause";
  return instantlyRequest<Campaign>(`/campaigns/${campaignId}/${action}`, {
    method: "POST",
  });
}

// ─── Leads ───────────────────────────────────────────────────────────────────

export async function addLeads(params: AddLeadsParams): Promise<{ added: number }> {
  let added = 0;
  for (const lead of params.leads) {
    await instantlyRequest<unknown>("/leads", {
      method: "POST",
      body: {
        email: lead.email,
        first_name: lead.first_name,
        last_name: lead.last_name,
        company_name: lead.company_name,
        campaign_id: params.campaign_id,
        ...(lead.variables && { custom_variables: lead.variables }),
      },
    });
    added++;
  }
  return { added };
}

export async function listLeads(
  campaignId: string,
  limit = 100,
  skip = 0
): Promise<Lead[]> {
  const response = await instantlyRequest<{ items: Lead[] }>("/leads/list", {
    method: "POST",
    body: {
      campaign_id: campaignId,
      limit,
      skip,
    },
  });
  return response.items ?? [];
}

export async function deleteLeads(
  campaignId: string,
  emails: string[]
): Promise<{ deleted: number }> {
  let deleted = 0;
  for (const email of emails) {
    await instantlyRequest<unknown>("/leads", {
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

export async function listAccounts(): Promise<Account[]> {
  const response = await instantlyRequest<PaginatedResponse<Account>>("/accounts");
  return response.items;
}

export async function enableWarmup(email: string): Promise<Account> {
  const encoded = encodeURIComponent(email);
  return instantlyRequest<Account>(`/accounts/${encoded}/warmup`, {
    method: "POST",
    body: { enabled: true },
  });
}

export async function disableWarmup(email: string): Promise<Account> {
  const encoded = encodeURIComponent(email);
  return instantlyRequest<Account>(`/accounts/${encoded}/warmup`, {
    method: "POST",
    body: { enabled: false },
  });
}

export async function getWarmupAnalytics(): Promise<unknown> {
  return instantlyRequest<unknown>("/accounts/warmup/analytics");
}

// ─── Analytics ───────────────────────────────────────────────────────────────

export async function getCampaignAnalytics(campaignId: string): Promise<CampaignAnalytics> {
  return instantlyRequest<CampaignAnalytics>(`/campaigns/${campaignId}/analytics`);
}
