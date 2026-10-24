/**
 * HTTP client for chat-service LLM completion.
 *
 * ⚠️ ALL LLM spend routes through chat-service — never a provider SDK here.
 * chat-service owns the model resolution, the provider key AND the cost
 * declaration, so this service declares no LLM cost of its own; it just calls
 * and passes identity headers.
 *
 * Two endpoints, picked by who PAYS:
 *   - `POST /complete`, org/run-scoped. The spend is metered against the
 *     caller's org.
 *   - `POST /internal/platform-complete` (and `/internal/platform-judgments`),
 *     platform-billed, no org balance gate, for internal work (sweeps,
 *     pollers). Every such call still states WHOSE work it is
 *     (`PlatformCaller`): our run, or a declared support function.
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
 * WHOSE work a platform call is (PLATFORM-CALLER-RUN-1024, chat-service's
 * contract COST-SUPPORT-1024). Every call to a platform route states exactly
 * one, never both, never neither:
 *  - `{ runId }`: the run OF THIS SERVICE doing the work (the job, request or
 *    step). chat-service's platform run becomes its child and the spend rides
 *    the outcome that run declares. The default whenever a run exists: an AI
 *    call made while judging a reply or reading a DSN is production.
 *  - `{ support }`: only when no run stands behind the call BY DESIGN; reported
 *    apart, never on an outcome. Never a way out of a 400 for production work.
 * Never a run id forwarded from an inbound request: that is another service's
 * run, not ours.
 */
export const SUPPORT_FUNCTIONS = [
  "payment_fees",
  "brand_setup",
  "copilot",
  "platform_llm",
  "staff_experiment",
  "product_feature",
  "retired_product",
] as const;
export type SupportFunction = (typeof SUPPORT_FUNCTIONS)[number];

export type PlatformCaller =
  | { runId: string; support?: never }
  | { support: { kind: SupportFunction; reason: string }; runId?: never };

/** Headers + body field that state the caller. Throws on a caller stating neither. */
export function platformCallerParts(caller: PlatformCaller): {
  headers: Record<string, string>;
  body: Record<string, unknown>;
} {
  if (caller.runId) return { headers: { "x-run-id": caller.runId }, body: {} };
  if (caller.support) return { headers: {}, body: { support: caller.support } };
  throw new Error("[instantly-service] a platform call must state its caller run or its support function");
}

/**
 * Platform completion — no org, no balance gate; platform-billed.
 *
 * Throws on any non-2xx (a refused `x-run-id` included: never retried without
 * it, that would file production work as nobody's). Callers on a fail-soft
 * path (the poller) catch it; a classification we could not obtain is better
 * left absent than guessed, and the fallback sweep asks again.
 */
export async function platformComplete(
  params: ChatCompleteParams,
  caller: PlatformCaller,
): Promise<ChatCompleteResult> {
  const parts = platformCallerParts(caller);
  const response = await fetch(`${baseUrl()}/internal/platform-complete`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey(),
      ...parts.headers,
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
      ...parts.body,
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
 * Org-scoped completion — `POST /complete`, billed to the caller's org on the
 * caller's run (chat-service declares the cost as a child of `x-run-id`). For
 * work done on behalf of a customer request, e.g. the hand-over email an
 * escalation writes into the prospect's thread.
 *
 * Throws on any non-2xx, carrying the status and body.
 */
export async function orgComplete(
  params: ChatCompleteParams & { maxTokens?: number },
  identity: { orgId: string; userId: string; runId: string; brandId?: string | null },
): Promise<ChatCompleteResult> {
  const response = await fetch(`${baseUrl()}/complete`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey(),
      "x-org-id": identity.orgId,
      "x-user-id": identity.userId,
      "x-run-id": identity.runId,
      ...(identity.brandId ? { "x-brand-id": identity.brandId } : {}),
    },
    body: JSON.stringify({
      message: params.message,
      systemPrompt: params.systemPrompt,
      provider: params.provider,
      model: params.model,
      ...(params.maxTokens !== undefined && { maxTokens: params.maxTokens }),
      ...(params.temperature !== undefined && { temperature: params.temperature }),
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `[instantly-service] chat-service POST /complete returned ${response.status}: ${text.slice(0, 300)}`,
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
 * `x-org-id` / `x-user-id`; the caller (`PlatformCaller`) is our own run in
 * `x-run-id` or a declared support function, never a fabricated id.
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
export async function platformJudgment(
  params: {
    state: string;
    questions: Record<string, JudgmentChoiceQuestion>;
  },
  caller: PlatformCaller,
): Promise<JudgmentsResult> {
  const parts = platformCallerParts(caller);
  const response = await fetch(`${baseUrl()}/internal/platform-judgments`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": apiKey(),
      ...parts.headers,
    },
    body: JSON.stringify({ state: params.state, questions: params.questions, ...parts.body }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `[instantly-service] chat-service POST /internal/platform-judgments returned ${response.status}: ${text.slice(0, 300)}`,
    );
  }

  return (await response.json()) as JudgmentsResult;
}
