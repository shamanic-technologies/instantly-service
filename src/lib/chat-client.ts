/**
 * HTTP client for chat-service LLM completion.
 *
 * ⚠️ ALL LLM spend routes through chat-service — never a provider SDK here.
 * chat-service owns the model resolution, the provider key AND the cost
 * declaration, so this service declares no LLM cost of its own; it just calls
 * and passes identity headers.
 *
 * Two endpoints, picked by whether the caller carries a run id:
 *   - WITH runId → `POST /complete`, org/run-scoped. The spend is metered
 *     against the caller's org.
 *   - WITHOUT runId → `POST /internal/platform-complete`, for run-less internal
 *     work (sweeps, pollers). chat-service uses the platform key and declares
 *     the spend on a platform run, with no org balance gate.
 *
 * The IMAP poller is a sweep with no inbound request, so it takes the platform
 * path — the reply it is classifying belongs to our own outreach, not to a
 * customer request we can bill.
 */

export type ChatProvider = "google" | "anthropic" | "deepseek";
export type ChatModel =
  | "flash"
  | "flash-lite"
  | "flash-pro"
  | "pro"
  | "sonnet"
  | "haiku"
  | "opus"
  | "deepseek-flash"
  | "deepseek-pro";

export interface ChatCompleteParams {
  message: string;
  systemPrompt: string;
  provider: ChatProvider;
  model: ChatModel;
  responseFormat?: "json";
  temperature?: number;
  disableThinking?: boolean;
}

export interface ChatCompleteResult {
  content: string;
  json?: Record<string, unknown>;
  tokensInput: number;
  tokensOutput: number;
  model: string;
}

function baseUrl(): string {
  const url = process.env.CHAT_SERVICE_URL;
  if (!url) throw new Error("[instantly-service] CHAT_SERVICE_URL is required");
  return url;
}

function apiKey(): string {
  const key = process.env.CHAT_SERVICE_API_KEY;
  if (!key) throw new Error("[instantly-service] CHAT_SERVICE_API_KEY is required");
  return key;
}

/**
 * Platform completion — no org, no run, no balance gate.
 *
 * Throws on any non-2xx. Callers on a fail-soft path (the poller) catch it; a
 * classification we could not obtain is better left absent than guessed.
 */
export async function platformComplete(
  params: ChatCompleteParams,
): Promise<ChatCompleteResult> {
  const response = await fetch(`${baseUrl()}/internal/platform-complete`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey(),
    },
    body: JSON.stringify({
      message: params.message,
      systemPrompt: params.systemPrompt,
      provider: params.provider,
      model: params.model,
      ...(params.responseFormat && { responseFormat: params.responseFormat }),
      ...(params.temperature !== undefined && { temperature: params.temperature }),
      ...(params.disableThinking !== undefined && {
        disableThinking: params.disableThinking,
      }),
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `[instantly-service] chat-service POST /internal/platform-complete returned ${response.status}: ${text.slice(0, 300)}`,
    );
  }

  return (await response.json()) as ChatCompleteResult;
}

/**
 * A typed JUDGMENT, as opposed to a completion.
 *
 * chat-service's judgment route answers a typed question about a piece of text
 * and returns the answer WITH its full probability distribution and a
 * confidence — never prose. That distribution is the whole point: a completion
 * that hesitated between two labels looks exactly like one it was certain
 * about, so nothing downstream can decline to act on a shaky answer.
 *
 * Shapes below are conformed to the DEPLOYED contract
 * (`POST /internal/platform-judgments`, read off the container's own
 * `openapi.json`), not to a spec. The route is org-less — service auth only, no
 * `x-org-id` / `x-user-id` / `x-run-id`, because a cron has none of them and a
 * fabricated run id is rejected by runs-service as a non-existent parent.
 *
 * Billing is chat-service's, on a platform run, against the vendor's reported
 * INPUT-token count; output is free at this vendor, so nothing declares one.
 * This service declares no cost of its own here, same as for completions.
 */

/** The `choice` question shape: pick one named option. */
export interface JudgmentChoiceQuestion {
  type: "choice";
  instructions: string;
  /** Option name → a plain description, or `{ what, examples }` when the plain form reads ambiguously. */
  criteria: Record<string, string | { what: string; examples?: string[] }>;
}

export interface JudgmentChoiceAnswer {
  type: "choice";
  /** The highest-probability option. */
  choice: string;
  /** 0..1, derived from how concentrated the distribution is. */
  confidence: number;
  /** Probability per option. Sums to 1. */
  probabilities: Record<string, number>;
}

export interface JudgmentsResult {
  model: string;
  answers: Record<string, JudgmentChoiceAnswer>;
  usage: { inputTokens: number; outputTokens: number };
}

/**
 * Ask one or more typed questions about a piece of text, platform-billed.
 *
 * Throws on any non-2xx, carrying the status and the body — callers on a
 * measurement path swallow it LOUDLY, and the body is what distinguishes a
 * vendor error (429, 502) from a bug of ours (400 naming the bad field).
 */
export async function platformJudgment(params: {
  state: string;
  questions: Record<string, JudgmentChoiceQuestion>;
}): Promise<JudgmentsResult> {
  const response = await fetch(`${baseUrl()}/internal/platform-judgments`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey(),
    },
    body: JSON.stringify({ state: params.state, questions: params.questions }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `[instantly-service] chat-service POST /internal/platform-judgments returned ${response.status}: ${text.slice(0, 300)}`,
    );
  }

  return (await response.json()) as JudgmentsResult;
}
