/**
 * FACTORY-P3 next outcomes (lead_interested): the reply judge as a STORED SCRIPT (workflow-service
 * `lead_interested/<dynasty>`, its code in the database, proven 100 % on recorded judgments) instead of this repo's
 * `qualifyReply` internals, while workflow-service says the outcome is switched for this service.
 *
 * No workflow step calls the reply judge (our poller and sweeps do), so the switch is a SERVICE switch: workflow-service
 * holds one row per (outcome, caller service) and its roll-back guard flips it back when the script errs more than our
 * own code did. This service reads it (`GET /internal/scripts/switches/service`, at most once a minute) and calls the
 * script door (`POST /internal/scripts/run`, service key, platform-billed exactly as our own call was: chat-service's
 * platform run becomes a child of the script's run, itself a child of the org's reply-judging run of the day).
 *
 * Everything around the judgment stays here: the caller still records the verdict, promotes the event, records an
 * opt-out, and the sweep declares the ONE Positive reply on the reply-judging run (`recordedBy: "instantly"` = the
 * script run declares nothing).
 */

export const REPLY_JUDGE_OUTCOME = "lead_interested";
export const CALLER_SERVICE = "instantly";
const STATE_TTL_MS = 60_000;
const RUN_TIMEOUT_MS = 120_000;

export interface ReplyJudgeSwitch {
  switched: boolean;
  dynasty: string | null;
}

let cached: { state: ReplyJudgeSwitch; at: number } | null = null;

/** Test hook. */
export function resetReplyJudgeSwitchMemo(): void {
  cached = null;
}

function workflow(): { base: string; key: string } {
  const base = (process.env.WORKFLOW_SERVICE_URL ?? "").replace(/\/$/, "");
  const key = process.env.WORKFLOW_SERVICE_API_KEY ?? "";
  if (!base || !key) throw new Error("[instantly-service] WORKFLOW_SERVICE_URL and WORKFLOW_SERVICE_API_KEY are required for the reply judge switch");
  return { base, key };
}

/**
 * Whether the reply judge runs as the stored script now. Read at most once a minute. An unreadable state keeps the last
 * one read (logged loudly); never read yet = our own code, the path that ran before any switch existed.
 */
export async function replyJudgeSwitch(now = Date.now()): Promise<ReplyJudgeSwitch> {
  if (cached && now - cached.at < STATE_TTL_MS) return cached.state;
  try {
    const { base, key } = workflow();
    const q = new URLSearchParams({ outcome: REPLY_JUDGE_OUTCOME, callerService: CALLER_SERVICE });
    const res = await fetch(`${base}/internal/scripts/switches/service?${q}`, { headers: { "x-api-key": key }, signal: AbortSignal.timeout(10_000) });
    const text = await res.text();
    if (!res.ok) throw new Error(`GET /internal/scripts/switches/service -> ${res.status}: ${text.slice(0, 200)}`);
    const body = JSON.parse(text) as { switched?: unknown; dynasty?: unknown };
    const state: ReplyJudgeSwitch = body.switched === true && typeof body.dynasty === "string" && body.dynasty ? { switched: true, dynasty: body.dynasty } : { switched: false, dynasty: null };
    cached = { state, at: now };
    return state;
  } catch (error) {
    const kept = cached?.state ?? { switched: false, dynasty: null };
    console.error(
      `[instantly-service] reply judge switch: state unreadable, keeping ${kept.switched ? `the script ${kept.dynasty}` : "our own code"}: ${error instanceof Error ? error.message : String(error)}`,
    );
    cached = { state: kept, at: now };
    return kept;
  }
}

export interface ReplyJudgeScriptInput {
  dynasty: string;
  orgId: string;
  /** The org's reply-judging run of the day: the script's run is its child. */
  judgingRunId: string;
  instantlyCampaignId: string;
  replyText: string;
  subject: string | null;
  ourOutreach: string | null;
}

export interface ReplyJudgeScriptResult {
  /** The script's own run. */
  runId: string | null;
  scriptSlug: string | null;
  status: "made" | "nothing";
  /** The label the script answered (unchecked here: the caller reads it like a model's answer). */
  kind: unknown;
  /** The exact completion request the script sent and chat-service's whole answer (null when it asked nothing). */
  request: Record<string, unknown> | null;
  response: Record<string, unknown> | null;
}

/** A judgment by the stored script that did not come back with a run that made or made nothing. */
export class ReplyJudgeScriptError extends Error {
  constructor(message: string, readonly runId: string | null, readonly scriptSlug: string | null) {
    super(message);
  }
}

/** Judge one reply with the stored script. Throws `ReplyJudgeScriptError` on any refusal or failure. */
export async function judgeWithScript(input: ReplyJudgeScriptInput): Promise<ReplyJudgeScriptResult> {
  const { base, key } = workflow();
  const res = await fetch(`${base}/internal/scripts/run`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": key, "x-org-id": input.orgId, "x-run-id": input.judgingRunId },
    body: JSON.stringify({
      outcome: REPLY_JUDGE_OUTCOME,
      dynasty: input.dynasty,
      subject: { service: "instantly", id: input.instantlyCampaignId },
      units: { reply_received: { content: { text: input.replyText, subject: input.subject, ourOutreach: input.ourOutreach } } },
      recordedBy: CALLER_SERVICE,
      trace: true,
    }),
    signal: AbortSignal.timeout(RUN_TIMEOUT_MS),
  });
  const text = await res.text();
  let body: Record<string, unknown> = {};
  try {
    body = JSON.parse(text) as Record<string, unknown>;
  } catch {
    // Not JSON: named below by status.
  }
  const details = (res.ok ? body : ((body.details ?? {}) as Record<string, unknown>)) as Record<string, unknown>;
  const runId = typeof details.runId === "string" ? details.runId : null;
  const scriptSlug = typeof details.scriptSlug === "string" ? details.scriptSlug : null;
  if (!res.ok) {
    throw new ReplyJudgeScriptError(`workflow-service POST /internal/scripts/run -> ${res.status}: ${String(body.error ?? text).slice(0, 300)}`, runId, scriptSlug);
  }
  const status = body.status;
  if (status !== "made" && status !== "nothing") throw new ReplyJudgeScriptError(`the script run answered status ${String(status)}`, runId, scriptSlug);
  const trace = Array.isArray(body.trace) ? (body.trace as Array<Record<string, unknown>>) : [];
  const completion = trace.find((t) => t.name === "llm.complete") ?? null;
  const unit = (body.unit ?? null) as { content?: Record<string, unknown> } | null;
  return {
    runId,
    scriptSlug,
    status,
    kind: status === "made" ? (unit?.content?.kind ?? null) : null,
    request: (completion?.sent as Record<string, unknown> | undefined) ?? null,
    response: (completion?.raw as Record<string, unknown> | undefined) ?? null,
  };
}
