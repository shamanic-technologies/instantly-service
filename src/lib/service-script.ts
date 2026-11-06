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

/**
 * Whether `outcome` is the stored script's now, for this service. Unreadable = the last state read; never read = our own
 * code. `task` names the decision (MIGRATE-1105 S2b: reply_received is made by two, `inbound-decision` and
 * `orphan-judgment`): workflow-service keys the switch by it.
 */
export async function serviceScriptSwitch(outcome: string, task?: string, now = Date.now()): Promise<ServiceScriptSwitch> {
  const memoKey = task ? `${outcome}|${task}` : outcome;
  const held = memo.get(memoKey);
  if (held && now - held.at < STATE_TTL_MS) return held.state;
  try {
    const { base, key } = workflow();
    const q = new URLSearchParams({ outcome, callerService: CALLER_SERVICE, ...(task ? { callerTask: task } : {}) });
    const res = await fetch(`${base}/internal/scripts/switches/service?${q}`, { headers: { "x-api-key": key }, signal: AbortSignal.timeout(10_000) });
    const text = await res.text();
    if (!res.ok) throw new Error(`GET /internal/scripts/switches/service -> ${res.status}: ${text.slice(0, 200)}`);
    const body = JSON.parse(text) as { switched?: unknown; shadow?: unknown; dynasty?: unknown };
    const dynasty = typeof body.dynasty === "string" && body.dynasty ? body.dynasty : null;
    const state: ServiceScriptSwitch =
      body.switched === true && dynasty ? { switched: true, shadow: false, dynasty } : body.shadow === true && dynasty ? { switched: false, shadow: true, dynasty } : NOT_SWITCHED;
    memo.set(memoKey, { state, at: now });
    return state;
  } catch (error) {
    const kept = held?.state ?? NOT_SWITCHED;
    console.error(`[instantly-service] ${outcome} switch: state unreadable, keeping ${kept.switched ? `the script ${kept.dynasty}` : "our own code"}: ${error instanceof Error ? error.message : String(error)}`);
    memo.set(memoKey, { state: kept, at: now });
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
  /** The decision (the Engine's task). */
  task?: string | null;
  path: DecisionPath;
  runId: string | null;
  scriptRunId: string | null;
  scriptSlug: string | null;
  material: Record<string, unknown>;
  ours: Record<string, unknown>;
  script: Record<string, unknown> | null;
  same: boolean | null;
  error: string | null;
  /** The vendor calls OUR decision made, in the script's words (null for a pure decision). */
  calls?: unknown[] | null;
}

/** Fail-soft: a decision is never failed by its record. */
export async function recordDecision(r: DecisionRecord): Promise<void> {
  try {
    await db.insert(scriptDecisions).values({
      outcome: r.outcome,
      task: r.task ?? null,
      path: r.path,
      runId: r.runId,
      scriptRunId: r.scriptRunId,
      scriptSlug: r.scriptSlug,
      material: r.material,
      ours: r.ours,
      script: r.script,
      same: r.same,
      error: r.error ? r.error.slice(0, 1000) : null,
      calls: r.calls ?? null,
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
  /**
   * The decision calls a PAID vendor (the orphan-reply judgment's Jev): once switched, ours is not computed beside the
   * script (it would pay twice); in shadow both are asked, ours used.
   */
  paidVendor?: boolean;
  /** What two answers are compared on (default the whole answer): a vendor's probabilities differ call to call. */
  compareOn?(words: Record<string, unknown>): Record<string, unknown>;
}

/** Our own decision's words, its material, and the vendor calls it made (in the script's words). */
export interface BuiltDecision {
  material: Record<string, unknown>;
  oursWords: Record<string, unknown>;
  calls?: unknown[] | null;
}

export interface DecideDeps {
  switchState?: (outcome: string, task?: string) => Promise<ServiceScriptSwitch>;
  run?: typeof runServiceScript;
  openRun?: (task: string) => Promise<{ id: string }>;
  closeRun?: (runId: string, status: "completed" | "failed") => Promise<void>;
  record?: (r: DecisionRecord) => Promise<void>;
}

/**
 * The decision to ACT on: ours while not switched (recorded; the script asked beside it in shadow), the stored
 * script's while switched. `material` and `oursWords` are built lazily, only where they are recorded.
 *
 * `ours` is our value, or (a paid decision) how to compute it: computed only where it is used or compared. `build`
 * receives it (null when a switched paid decision never computed it: then only the material is read).
 */
export async function decideThroughScript<T>(
  engine: ServiceScriptEngine<T>,
  ours: T | (() => Promise<T>),
  build: (ours: T | null) => { material: Record<string, unknown>; oursWords: Record<string, unknown> | null; calls?: unknown[] | null },
  deps: DecideDeps = {},
): Promise<T> {
  // Resolved at use, never up front: nothing on our own path may fail the caller.
  const openRun = (task: string) => (deps.openRun ?? createPlatformRun)(task);
  const closeRun = (runId: string, status: "completed" | "failed") => (deps.closeRun ?? closePlatformRun)(runId, status);
  const record = (r: DecisionRecord) => (deps.record ?? recordDecision)(r);
  const run = (input: Parameters<typeof runServiceScript>[0]) => (deps.run ?? runServiceScript)(input);

  const choose = await (deps.switchState ?? serviceScriptSwitch)(engine.outcome, engine.task);
  const dynasty = choose.dynasty;
  const ourValue = (): Promise<T> => (typeof ours === "function" ? (ours as () => Promise<T>)() : Promise.resolve(ours));
  const task = engine.task;
  const compared = (w: Record<string, unknown>) => (engine.compareOn ? engine.compareOn(w) : w);

  if (!dynasty || (!choose.switched && !choose.shadow)) {
    const value = await ourValue();
    // Our own path, recorded with its exact material; never delays nor fails the caller.
    void (async () => {
      const { material, oursWords, calls } = build(value);
      const opened = await openRun(engine.task).catch((error: unknown) => {
        console.error(`[instantly-service] ${engine.outcome}: run not opened: ${error instanceof Error ? error.message : String(error)}`);
        return null;
      });
      await record({ outcome: engine.outcome, task, path: "service", runId: opened?.id ?? null, scriptRunId: null, scriptSlug: null, material, ours: oursWords ?? {}, script: null, same: null, error: null, calls: calls ?? null });
      if (opened) await closeRun(opened.id, "completed").catch(() => undefined);
    })().catch((error: unknown) => console.error(`[instantly-service] ${engine.outcome}: decision not recorded: ${error instanceof Error ? error.message : String(error)}`));
    return value;
  }

  const path: DecisionPath = choose.switched ? "script" : "shadow";
  // A switched paid decision runs the script alone; every other path computes ours (used, or compared).
  const value = path === "script" && engine.paidVendor ? null : await ourValue();
  const ask = async (): Promise<T> => {
    const { material, oursWords, calls } = build(value);
    const opened = await openRun(engine.task);
    try {
      const out = await run({ outcome: engine.outcome, dynasty, parentRunId: opened.id, consumes: engine.consumes, material });
      const same = oursWords ? sameWords(compared(oursWords), compared(out.words)) : null;
      const used = path === "script" ? engine.fromWords(out.words) : (value as T);
      await record({ outcome: engine.outcome, task, path, runId: opened.id, scriptRunId: out.runId, scriptSlug: out.scriptSlug, material, ours: oursWords ?? {}, script: out.words, same, error: null, calls: calls ?? null });
      if (same === false) console.error(`[instantly-service] ${engine.outcome}: the script's answer differs from ours (run ${opened.id}, ${path === "script" ? "the script's is used" : "ours is used"})`);
      await closeRun(opened.id, "completed").catch(() => undefined);
      return used;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await record({ outcome: engine.outcome, task, path, runId: opened.id, scriptRunId: error instanceof ServiceScriptError ? error.runId : null, scriptSlug: null, material, ours: oursWords ?? {}, script: null, same: null, error: message, calls: calls ?? null });
      await closeRun(opened.id, "failed").catch(() => undefined);
      throw error;
    }
  };
  if (path === "script") return ask();
  void ask().catch((error: unknown) => console.error(`[instantly-service] ${engine.outcome} shadow failed (ours was used): ${error instanceof Error ? error.message : String(error)}`));
  return value as T;
}
