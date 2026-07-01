/**
 * HTTP client for brand-service.
 *
 * Reads a brand's runtime goal (`current_goal`) — the brand-owned, MUTABLE
 * outreach target ('signup' | 'meetingBooked' | 'purchase'). Used by the
 * stop-on-click feature: when a prospect clicks and the brand is maximizing
 * signups, the lead's Instantly sequence is paused (the landing page owns the
 * conversion from there — more cold emails only distract).
 *
 * brand-service owns this value; instantly-service reads it LIVE at decision
 * time (the goal is designed to change without touching campaign rows, so a
 * send-time snapshot would freeze a value meant to move).
 */

const BRAND_SERVICE_URL = process.env.BRAND_SERVICE_URL || "http://localhost:3010";
const BRAND_SERVICE_API_KEY = process.env.BRAND_SERVICE_API_KEY || "";

interface RuntimeContextResponse {
  currentGoal: string;
}

/**
 * Fetch a single brand's runtime goal via
 * GET /internal/brands/:brandId/runtime-context.
 *
 * @throws on network / non-2xx errors (caller decides fail-soft behavior).
 */
export async function getCurrentGoal(brandId: string): Promise<string> {
  const response = await fetch(
    `${BRAND_SERVICE_URL}/internal/brands/${brandId}/runtime-context`,
    {
      method: "GET",
      headers: { "x-api-key": BRAND_SERVICE_API_KEY },
    },
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `brand-service GET /internal/brands/${brandId}/runtime-context failed: ${response.status} - ${errorText}`,
    );
  }

  const body = (await response.json()) as RuntimeContextResponse;
  return body.currentGoal;
}

/**
 * Fetch the runtime goal for every brand in the set. Throws if ANY lookup
 * fails — the stop-on-click caller wraps this in a fail-soft try/catch, so a
 * failed lookup conservatively results in NOT stopping the sequence.
 */
export async function getCurrentGoals(brandIds: string[]): Promise<string[]> {
  return Promise.all(brandIds.map((id) => getCurrentGoal(id)));
}
