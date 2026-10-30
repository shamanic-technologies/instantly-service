/**
 * FACTORY-P3 next outcomes (message_sent): the SEND SELECTION (which due steps leave now, from which mailbox) as a
 * STORED SCRIPT (workflow-service `message_sent/<dynasty>`), the same inputs `selectDueSteps` reads handed as one
 * material, ONE run per dispatch selection (never per email).
 *
 * Two modes, both recorded (`dispatch_selections`, read by workflow-service's case recorder):
 *   - SHADOW (not switched): our own selection is sent; the script is asked the same question beside it, never
 *     awaited by the send, and both answers are compared. This is the live proof the switch needs: on real dispatch
 *     runs, the same recipients, the same mailbox and the same moment (the run) as our own choice.
 *   - SWITCHED (workflow-service says message_sent is switched for this service, read at most once a minute): the
 *     script's selection is the one sent; ours is still computed and compared, recorded beside it.
 * Everything around the selection stays here: the poll before it, the send lock, the transport, the send records, the
 * costs, the ONE `message_sent` declaration per email (send-outcomes.ts).
 *
 * The runs: a selection has no org (the queue is the fleet's), so it runs under a PLATFORM run of ours
 * (`dispatch-selection`, spends nothing), the script's run its child.
 */
import { lt } from "drizzle-orm";
import { db } from "../../db";
import { dispatchSelections } from "../../db/schema";
import { closePlatformRun, createPlatformRun } from "../runs-client";
import { resolveLeadTimezone } from "../sending-window";
import type { FleetBounceHistory } from "../bounce-budget";
import type { AccountCapacity, DueSelection, DueStep, PendingSequence } from "./dispatch";

export const MESSAGE_SENT_OUTCOME = "message_sent";
export const CALLER_SERVICE = "instantly";
export const DISPATCH_SELECTION_TASK = "dispatch-selection";
const STATE_TTL_MS = 60_000;
const RUN_TIMEOUT_MS = 120_000;

export interface SendSelectSwitch {
  switched: boolean;
  /** Not switched, but the script is asked BESIDE our own selection and compared (the live proof before a switch). */
  shadow: boolean;
  dynasty: string | null;
}

let cached: { state: SendSelectSwitch; at: number } | null = null;

/** Test hook. */
export function resetSendSelectSwitchMemo(): void {
  cached = null;
}

function workflow(): { base: string; key: string } {
  const base = (process.env.WORKFLOW_SERVICE_URL ?? "").replace(/\/$/, "");
  const key = process.env.WORKFLOW_SERVICE_API_KEY ?? "";
  if (!base || !key) throw new Error("[instantly-service] WORKFLOW_SERVICE_URL and WORKFLOW_SERVICE_API_KEY are required for the send selection script");
  return { base, key };
}

/** Whether the selection is the stored script's now. Unreadable = the last state read; never read = our own code. */
export async function sendSelectSwitch(now = Date.now()): Promise<SendSelectSwitch> {
  if (cached && now - cached.at < STATE_TTL_MS) return cached.state;
  try {
    const { base, key } = workflow();
    const q = new URLSearchParams({ outcome: MESSAGE_SENT_OUTCOME, callerService: CALLER_SERVICE });
    const res = await fetch(`${base}/internal/scripts/switches/service?${q}`, { headers: { "x-api-key": key }, signal: AbortSignal.timeout(10_000) });
    const text = await res.text();
    if (!res.ok) throw new Error(`GET /internal/scripts/switches/service -> ${res.status}: ${text.slice(0, 200)}`);
    const body = JSON.parse(text) as { switched?: unknown; shadow?: unknown; dynasty?: unknown };
    const dynasty = typeof body.dynasty === "string" && body.dynasty ? body.dynasty : null;
    const state: SendSelectSwitch =
      body.switched === true && dynasty ? { switched: true, shadow: false, dynasty } : body.shadow === true && dynasty ? { switched: false, shadow: true, dynasty } : { switched: false, shadow: false, dynasty: null };
    cached = { state, at: now };
    return state;
  } catch (error) {
    const kept = cached?.state ?? { switched: false, shadow: false, dynasty: null };
    console.error(`[instantly-service] send selection switch: state unreadable, keeping ${kept.switched ? `the script ${kept.dynasty}` : "our own code"}: ${error instanceof Error ? error.message : String(error)}`);
    cached = { state: kept, at: now };
    return kept;
  }
}

/** The selection's inputs as the script reads them (JSON: instants as ISO text, the lead's time zone resolved). */
export function selectionMaterial(input: {
  asOf: Date;
  sequences: readonly PendingSequence[];
  capacities: readonly AccountCapacity[];
  skippedMailboxes: ReadonlySet<string>;
  bounceHistory: FleetBounceHistory | null;
}): Record<string, unknown> {
  const iso = (d: Date | null | undefined) => (d ? d.toISOString() : null);
  return {
    asOf: input.asOf.toISOString(),
    sequences: input.sequences.map((s) => ({
      instantlyCampaignId: s.instantlyCampaignId,
      leadEmail: s.leadEmail,
      accountEmail: s.accountEmail,
      provisionedSteps: [...s.provisionedSteps],
      lastSentStep: s.lastSentStep,
      lastSentAt: iso(s.lastSentAt),
      stepDelays: [...s.stepDelays],
      timezone: resolveLeadTimezone(s.timezone),
      queuedAt: iso(s.queuedAt ?? null),
      ...(s.answersPerson !== undefined ? { answersPerson: s.answersPerson } : {}),
      ...(s.verdict !== undefined ? { verdict: s.verdict } : {}),
      ...(s.addressProven !== undefined ? { addressProven: s.addressProven } : {}),
      ...(s.recipientProvider !== undefined ? { recipientProvider: s.recipientProvider } : {}),
    })),
    capacities: input.capacities.map((c) => ({ ...c })),
    skippedMailboxes: [...input.skippedMailboxes].sort(),
    bounceHistory: input.bounceHistory
      ? Object.fromEntries([...input.bounceHistory].map(([domain, windows]) => [domain, Object.fromEntries([...windows].map(([hours, counts]) => [String(hours), { ...counts }]))]))
      : null,
  };
}

/** A selection in the words both sides are compared in (order kept: the send order). */
export function selectionWords(selection: DueSelection): Record<string, unknown> {
  return {
    selected: selection.selected.map((s) => ({
      instantlyCampaignId: s.instantlyCampaignId,
      leadEmail: s.leadEmail,
      accountEmail: s.accountEmail,
      step: s.step,
      dueAt: s.dueAt.toISOString(),
      ...(s.rehomedFrom ? { rehomedFrom: s.rehomedFrom } : {}),
    })),
    dueBeforeCapacity: selection.dueBeforeCapacity,
    blockedNoCapacityRow: selection.blockedNoCapacityRow,
    skippedSilenced: selection.skippedSilenced,
    rehomed: selection.rehomed,
    heldBounceBudget: selection.heldBounceBudget,
    heldBlocklisted: selection.heldBlocklisted,
    heldPlacement: selection.heldPlacement,
    heldMostlySpam: selection.heldMostlySpam,
  };
}

/** The script's answer as our own `DueSelection` (the shape the send loop reads). */
export function selectionFromWords(words: Record<string, unknown>): DueSelection {
  const n = (k: string) => {
    const v = words[k];
    if (typeof v !== "number") throw new Error(`the send selection answered no ${k}`);
    return v;
  };
  const selected = (Array.isArray(words.selected) ? words.selected : null) as Array<Record<string, unknown>> | null;
  if (!selected) throw new Error("the send selection answered no selected list");
  return {
    selected: selected.map((s): DueStep => ({
      instantlyCampaignId: String(s.instantlyCampaignId),
      leadEmail: String(s.leadEmail),
      accountEmail: String(s.accountEmail),
      step: Number(s.step),
      dueAt: new Date(String(s.dueAt)),
      ...(typeof s.rehomedFrom === "string" ? { rehomedFrom: s.rehomedFrom } : {}),
    })),
    dueBeforeCapacity: n("dueBeforeCapacity"),
    blockedNoCapacityRow: n("blockedNoCapacityRow"),
    skippedSilenced: n("skippedSilenced"),
    rehomed: n("rehomed"),
    heldBounceBudget: n("heldBounceBudget"),
    heldBlocklisted: n("heldBlocklisted"),
    heldPlacement: n("heldPlacement"),
    heldMostlySpam: n("heldMostlySpam"),
  };
}

export class SendSelectScriptError extends Error {
  constructor(message: string, readonly runId: string | null) {
    super(message);
  }
}

/** Run the stored selection under our platform run. Answers its selection in the compared words. */
export async function selectWithScript(dynasty: string, parentRunId: string, material: Record<string, unknown>): Promise<{ runId: string | null; scriptSlug: string | null; words: Record<string, unknown> }> {
  const { base, key } = workflow();
  const res = await fetch(`${base}/internal/scripts/run`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-api-key": key, "x-run-id": parentRunId },
    body: JSON.stringify({
      outcome: MESSAGE_SENT_OUTCOME,
      dynasty,
      subject: { service: "instantly", id: parentRunId },
      units: { message_queued: { content: material } },
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
  if (!res.ok) throw new SendSelectScriptError(`workflow-service POST /internal/scripts/run -> ${res.status}: ${String(body.error ?? text).slice(0, 400)}`, runId);
  const content = ((body.unit ?? null) as { content?: Record<string, unknown> } | null)?.content ?? null;
  if (body.status !== "made" || !content) throw new SendSelectScriptError(`the send selection made no selection (status ${String(body.status)})`, runId);
  return { runId, scriptSlug: typeof body.scriptSlug === "string" ? body.scriptSlug : null, words: content };
}

/** Same selection, in the same order, with the same counts. */
export function sameSelection(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  return JSON.stringify(a) === JSON.stringify(canonical(b, a));
}

/** `b` with `a`'s key order (both are built from the same words; a key absent on one side stays absent). */
function canonical(b: Record<string, unknown>, a: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(a)) if (k in b) out[k] = k === "selected" && Array.isArray(b[k]) ? (b[k] as Array<Record<string, unknown>>).map((s, i) => orderLike(s, ((a.selected as Array<Record<string, unknown>>)[i] ?? {}))) : b[k];
  for (const k of Object.keys(b)) if (!(k in out)) out[k] = b[k];
  return out;
}

function orderLike(s: Record<string, unknown>, like: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(like)) if (k in s) out[k] = s[k];
  for (const k of Object.keys(s)) if (!(k in out)) out[k] = s[k];
  return out;
}

// ─── The selection a dispatch run acts on ─────────────────────────────────────────────────────────────────────────


export const DISPATCH_SELECTION_RETENTION_DAYS = 14;

/**
 * The selection to SEND: our own (`ours`, computed by the caller) while not switched, the script asked beside it in
 * the background and compared (never delaying a send); the stored script's while switched (awaited; a failure fails
 * this dispatch run, the next one asks again: never a silent fall back to our own). Every selection asked of the
 * script is recorded with both answers.
 */
export async function selectionToSend(
  ours: DueSelection,
  input: Parameters<typeof selectionMaterial>[0],
  deps: { switchState?: typeof sendSelectSwitch; run?: typeof selectWithScript } = {},
): Promise<DueSelection> {
  const choose = await (deps.switchState ?? sendSelectSwitch)();
  if (!choose.dynasty || (!choose.switched && !choose.shadow)) return ours;
  const dynasty = choose.dynasty;
  const path = choose.switched ? "script" : "shadow";
  const ask = async (): Promise<DueSelection> => {
    const material = selectionMaterial(input);
    const oursWords = selectionWords(ours);
    const run = await createPlatformRun(DISPATCH_SELECTION_TASK);
    try {
      const out = await (deps.run ?? selectWithScript)(dynasty, run.id, material);
      const same = sameSelection(oursWords, out.words);
      await recordSelection({ path, runId: run.id, scriptRunId: out.runId, scriptSlug: out.scriptSlug, material, ours: oursWords, script: out.words, same, error: null });
      if (!same) console.error(`[instantly-service] send selection: the script's selection differs from ours (run ${run.id}, ${path === "script" ? "the script's is sent" : "ours is sent"})`);
      await closePlatformRun(run.id, "completed").catch(() => undefined);
      return path === "script" ? selectionFromWords(out.words) : ours;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await recordSelection({ path, runId: run.id, scriptRunId: error instanceof SendSelectScriptError ? error.runId : null, scriptSlug: null, material, ours: oursWords, script: null, same: null, error: message });
      await closePlatformRun(run.id, "failed").catch(() => undefined);
      throw error;
    }
  };
  if (path === "script") return ask();
  // Shadow: ours is sent now; the script is asked beside it, never delaying a send.
  void ask().catch((error: unknown) => console.error(`[instantly-service] send selection shadow failed (ours was sent): ${error instanceof Error ? error.message : String(error)}`));
  return ours;
}

async function recordSelection(r: {
  path: "shadow" | "script";
  runId: string;
  scriptRunId: string | null;
  scriptSlug: string | null;
  material: Record<string, unknown>;
  ours: Record<string, unknown>;
  script: Record<string, unknown> | null;
  same: boolean | null;
  error: string | null;
}): Promise<void> {
  try {
    await db.insert(dispatchSelections).values({
      path: r.path,
      runId: r.runId,
      scriptRunId: r.scriptRunId,
      scriptSlug: r.scriptSlug,
      material: r.material,
      ours: r.ours,
      script: r.script,
      same: r.same,
      sends: Array.isArray(r.ours.selected) ? r.ours.selected.length : 0,
      error: r.error ? r.error.slice(0, 1000) : null,
    });
    await db.delete(dispatchSelections).where(lt(dispatchSelections.createdAt, new Date(Date.now() - DISPATCH_SELECTION_RETENTION_DAYS * 86_400_000)));
  } catch (error) {
    console.error(`[instantly-service] send selection NOT recorded: ${error instanceof Error ? error.message : String(error)}`);
  }
}
