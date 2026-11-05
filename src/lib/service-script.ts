/**
 * MIGRATE-1105: ONE mechanism for every decision of this service that a stored factory Script can take
 * (workflow-service `<outcome>/<dynasty>`), the same shape the send selection proved (`send-select-script.ts`):
 *
 *   - workflow-service holds one SERVICE switch row per (outcome, caller `instantly`), read here at most once a
 *     minute (`GET /internal/scripts/switches/service`). Unreadable = the last state read; never read = our own code.
 *   - NO row: our own decision is used, recorded with its exact material (path `service`), nothing asked.
 *   - SHADOW: ours is used at once; the script is asked the same question in the background (never delaying the
 *     caller), both answers compared and recorded (path `shadow`).
 *   - SWITCHED: the script's answer is used (awaited; a failure fails the caller, never a silent fall back); ours is
 *     still computed and compared (path `script`). Rolled back = our code within a minute.
 *
 * Every decision opens ONE platform run of ours under the Engine's task name (spends nothing): Monitoring's
 * service path, and the parent of the script's run when one is asked. Recorded in `script_decisions`, served by
 * `GET /internal/script-decisions/<outcome>/recorded` to workflow-service's case recorder.
 *
 * No per-Engine branch here: an Engine is its outcome, its task name, the unit its script consumes, and how its
 * words map back to the caller's value.
 */
import { lt } from "drizzle-orm";
import { db } from "../db";
import { scriptDecisions } from "../db/schema";
import { closePlatformRun, createPlatformRun } from "./runs-client";

export const CALLER_SERVICE = "instantly";
const STATE_TTL_MS = 60_000;
const RUN_TIMEOUT_MS = 120_000;
export const SCRIPT_DECISION_RETENTION_DAYS = 14;

export interface ServiceScriptSwitch {
  switched: boolean;
  /** Not switched, but the script is asked BESIDE our own decision and compared (the live proof before a switch). */
  shadow: boolean;
  dynasty: string | null;
}

const NOT_SWITCHED: ServiceScriptSwitch = { switched: false, shadow: false, dynasty: null };
const memo = new Map<string, { state: ServiceScriptSwitch; at: number }>();

/** Test hook. */
export function resetServiceScriptSwitchMemo(): void {
  memo.clear();
}

function workflow(): { base: string; key: string } {
  const base = (process.env.WORKFLOW_SERVICE_URL ?? "").replace(/\/$/, "");
  const key = process.env.WORKFLOW_SERVICE_API_KEY ?? "";
  if (!base || !key) throw new Error("[instantly-service] WORKFLOW_SERVICE_URL and WORKFLOW_SERVICE_API_KEY are required for a stored script");
  return { base, key };
}

/** Whether `outcome` is the stored script's now, for this service. Unreadable = the last state read; never read = our own code. */
export async function serviceScriptSwitch(outcome: string, now = Date.now()): Promise<ServiceScriptSwitch> {
  const held = memo.get(outcome);
  if (held && now - held.at < STATE_TTL_MS) return held.state;
  try {
    const { base, key } = workflow();
    const q = new URLSearchParams({ outcome, callerService: CALLER_SERVICE });
    const res = await fetch(`${base}/internal/scripts/switches/service?${q}`, { headers: { "x-api-key": key }, signal: AbortSignal.timeout(10_000) });
    const text = await res.text();
    if (!res.ok) throw new Error(`GET /internal/scripts/switches/service -> ${res.status}: ${text.slice(0, 200)}`);
    const body = JSON.parse(text) as { switched?: unknown; shadow?: unknown; dynasty?: unknown };
    const dynasty = typeof body.dynasty === "string" && body.dynasty ? body.dynasty : null;
    const state: ServiceScriptSwitch =
      body.switched === true && dynasty ? { switched: true, shadow: false, dynasty } : body.shadow === true && dynasty ? { switched: false, shadow: true, dynasty } : NOT_SWITCHED;
    memo.set(outcome, { state, at: now });
    return state;
  } catch (error) {
    const kept = held?.state ?? NOT_SWITCHED;
    console.error(`[instantly-service] ${outcome} switch: state unreadable, keeping ${kept.switched ? `the script ${kept.dynasty}` : "our own code"}: ${error instanceof Error ? error.message : String(error)}`);
    memo.set(outcome, { state: kept, at: now });
    return kept;
  }
}

export class ServiceScriptError extends Error {
  constructor(message: string, readonly runId: string | null) {
    super(message);
  }
}

/** Run the stored script under our platform run. Answers its unit's content (the compared words). */
export async function runServiceScript(input: {
  outcome: string;
  dynasty: string;
  parentRunId: string;
  /** The unit the script consumes (`units[<consumes>].content` = the material). */
  consumes: string;
  material: Record<string, unknown>;
}): Promise<{ runId: string | null; scriptSlug: string | null; words: Record<string, unknown> }> {
  const { base, key } = workflow();
  const res = await fetch(`${base}/internal/scripts/run`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": key, "x-run-id": input.parentRunId },
    body: JSON.stringify({
      outcome: input.outcome,
      dynasty: input.dynasty,
      subject: { service: CALLER_SERVICE, id: input.parentRunId },
      units: { [input.consumes]: { content: input.material } },
      recordedBy: CALLER_SERVICE,
    }),
    signal: AbortSignal.timeout(RUN_TIMEOUT_MS),
  });
  const text = await res.text();
  let body: Record<string, unknown> = {};
  try {
    body = JSON.parse(text) as Record<string, unknown>;
  } catch {
    // Named below by status.
  }
  const details = (res.ok ? body : ((body.details ?? {}) as Record<string, unknown>)) as Record<string, unknown>;
  const runId = typeof details.runId === "string" ? details.runId : null;
  if (!res.ok) throw new ServiceScriptError(`workflow-service POST /internal/scripts/run -> ${res.status}: ${String(body.error ?? text).slice(0, 400)}`, runId);
  const content = ((body.unit ?? null) as { content?: Record<string, unknown> } | null)?.content ?? null;
  if (body.status !== "made" || !content) throw new ServiceScriptError(`the ${input.outcome} script made nothing (status ${String(body.status)})`, runId);
  return { runId, scriptSlug: typeof body.scriptSlug === "string" ? body.scriptSlug : null, words: content };
}

/** JSON with every object's keys sorted: two answers are the same iff these strings are equal. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value as Record<string, unknown>).sort()) out[k] = sortKeys((value as Record<string, unknown>)[k]);
    return out;
  }
  return value;
}

/** Same answer, key order aside. */
export function sameWords(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  return canonicalJson(a) === canonicalJson(b);
}

export type DecisionPath = "service" | "shadow" | "script";

export interface DecisionRecord {
  outcome: string;
  path: DecisionPath;
  runId: string | null;
  scriptRunId: string | null;
  scriptSlug: string | null;
  material: Record<string, unknown>;
  ours: Record<string, unknown>;
  script: Record<string, unknown> | null;
  same: boolean | null;
  error: string | null;
}

/** Fail-soft: a decision is never failed by its record. */
export async function recordDecision(r: DecisionRecord): Promise<void> {
  try {
    await db.insert(scriptDecisions).values({
      outcome: r.outcome,
      path: r.path,
      runId: r.runId,
      scriptRunId: r.scriptRunId,
      scriptSlug: r.scriptSlug,
      material: r.material,
      ours: r.ours,
      script: r.script,
      same: r.same,
      error: r.error ? r.error.slice(0, 1000) : null,
    });
    await db.delete(scriptDecisions).where(lt(scriptDecisions.createdAt, new Date(Date.now() - SCRIPT_DECISION_RETENTION_DAYS * 86_400_000)));
  } catch (error) {
    console.error(`[instantly-service] ${r.outcome} decision NOT recorded: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export interface ServiceScriptEngine<T> {
  outcome: string;
  /** Our platform run's task name (Monitoring's service path). */
  task: string;
  /** The unit the script consumes. */
  consumes: string;
  /** The caller's value as the script answers it. */
  fromWords(words: Record<string, unknown>): T;
}

export interface DecideDeps {
  switchState?: (outcome: string) => Promise<ServiceScriptSwitch>;
  run?: typeof runServiceScript;
  openRun?: (task: string) => Promise<{ id: string }>;
  closeRun?: (runId: string, status: "completed" | "failed") => Promise<void>;
  record?: (r: DecisionRecord) => Promise<void>;
}

/**
 * The decision to ACT on: ours while not switched (recorded; the script asked beside it in shadow), the stored
 * script's while switched. `material` and `oursWords` are built lazily, only where they are recorded.
 */
export async function decideThroughScript<T>(
  engine: ServiceScriptEngine<T>,
  ours: T,
  build: () => { material: Record<string, unknown>; oursWords: Record<string, unknown> },
  deps: DecideDeps = {},
): Promise<T> {
  // Resolved at use, never up front: nothing on our own path may fail the caller.
  const openRun = (task: string) => (deps.openRun ?? createPlatformRun)(task);
  const closeRun = (runId: string, status: "completed" | "failed") => (deps.closeRun ?? closePlatformRun)(runId, status);
  const record = (r: DecisionRecord) => (deps.record ?? recordDecision)(r);
  const run = (input: Parameters<typeof runServiceScript>[0]) => (deps.run ?? runServiceScript)(input);

  const choose = await (deps.switchState ?? serviceScriptSwitch)(engine.outcome);
  const dynasty = choose.dynasty;

  if (!dynasty || (!choose.switched && !choose.shadow)) {
    // Our own path, recorded with its exact material; never delays nor fails the caller.
    void (async () => {
      const { material, oursWords } = build();
      const opened = await openRun(engine.task).catch((error: unknown) => {
        console.error(`[instantly-service] ${engine.outcome}: run not opened: ${error instanceof Error ? error.message : String(error)}`);
        return null;
      });
      await record({ outcome: engine.outcome, path: "service", runId: opened?.id ?? null, scriptRunId: null, scriptSlug: null, material, ours: oursWords, script: null, same: null, error: null });
      if (opened) await closeRun(opened.id, "completed").catch(() => undefined);
    })().catch((error: unknown) => console.error(`[instantly-service] ${engine.outcome}: decision not recorded: ${error instanceof Error ? error.message : String(error)}`));
    return ours;
  }

  const path: DecisionPath = choose.switched ? "script" : "shadow";
  const ask = async (): Promise<T> => {
    const { material, oursWords } = build();
    const opened = await openRun(engine.task);
    try {
      const out = await run({ outcome: engine.outcome, dynasty, parentRunId: opened.id, consumes: engine.consumes, material });
      const same = sameWords(oursWords, out.words);
      const value = path === "script" ? engine.fromWords(out.words) : ours;
      await record({ outcome: engine.outcome, path, runId: opened.id, scriptRunId: out.runId, scriptSlug: out.scriptSlug, material, ours: oursWords, script: out.words, same, error: null });
      if (!same) console.error(`[instantly-service] ${engine.outcome}: the script's answer differs from ours (run ${opened.id}, ${path === "script" ? "the script's is used" : "ours is used"})`);
      await closeRun(opened.id, "completed").catch(() => undefined);
      return value;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await record({ outcome: engine.outcome, path, runId: opened.id, scriptRunId: error instanceof ServiceScriptError ? error.runId : null, scriptSlug: null, material, ours: oursWords, script: null, same: null, error: message });
      await closeRun(opened.id, "failed").catch(() => undefined);
      throw error;
    }
  };
  if (path === "script") return ask();
  void ask().catch((error: unknown) => console.error(`[instantly-service] ${engine.outcome} shadow failed (ours was used): ${error instanceof Error ? error.message : String(error)}`));
  return ours;
}
