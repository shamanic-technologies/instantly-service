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
  account_ids?: string[];
  email?: EmailContent;
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
    "Content-Type": "application/json",
    Authorization: `Bearer ${getApiKey()}`,
  };

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
        throw new Error(
          `instantly-api ${method} ${path} failed: ${response.status} - ${errorText}`
        );
      }

      return response.json() as Promise<T>;
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

  if (params.account_ids) {
    body.account_ids = params.account_ids;
  }

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
  return instantlyRequest<Campaign>(`/campaigns/${campaignId}`, {
    method: "PATCH",
    body: { status },
  });
}

// ─── Leads ───────────────────────────────────────────────────────────────────

export async function addLeads(params: AddLeadsParams): Promise<{ added: number }> {
  return instantlyRequest<{ added: number }>(`/campaigns/${params.campaign_id}/leads`, {
    method: "POST",
    body: { leads: params.leads },
  });
}

export async function listLeads(
  campaignId: string,
  limit = 100,
  skip = 0
): Promise<Lead[]> {
  return instantlyRequest<Lead[]>(
    `/campaigns/${campaignId}/leads?limit=${limit}&skip=${skip}`
  );
}

export async function deleteLeads(
  campaignId: string,
  emails: string[]
): Promise<{ deleted: number }> {
  return instantlyRequest<{ deleted: number }>(`/campaigns/${campaignId}/leads`, {
    method: "DELETE",
    body: { emails },
  });
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
