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

export type ChatProvider = "google" | "anthropic";
export type ChatModel =
  | "flash"
  | "flash-lite"
  | "flash-pro"
  | "pro"
  | "sonnet"
  | "haiku"
  | "opus";

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
