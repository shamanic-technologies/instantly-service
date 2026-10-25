/**
 * LEDGER-RESIDUAL-1025: a positive reply (`conversation`) and a website visit
 * (`website_visit`) CONSUME the person's `message_sent`, our send. The ledger
 * resolves a consumption to the run that declared producing that occurrence
 * (same key, same subject = the lead). Some of our sends can never be declared:
 * the run that sent them is gone from runs-service, the sequence was never tied
 * to its run (platform holds of 2026-06 hold no sequence id, several runs per
 * address), or the run is marked support / unattributed by its owner (runs-
 * service refuses an outcome on it). A consumption of such a send names no
 * producer and keeps the reconciliation open for good ("unknown provenance"),
 * while its cost is in no unit anyway: there is no standard cost to carry.
 *
 * So the input is stated from OUR records, per lead:
 *  - `declared`: one of our sends to this lead is declared in the ledger
 *    (a hold marked `declared`): consume `message_sent`;
 *  - `pending`: a send of ours to this lead is sent or queued and not yet
 *    declared (actual / provisioned hold, no mark): wait, the send sweep
 *    declares it within minutes;
 *  - `unplaceable`: nothing of ours can ever be declared for this lead:
 *    the occurrence is declared with NO consumption (marked
 *    `declared_no_send`), never against an invented producer.
 * Never inferred from what another service declared; never an estimate.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";

export type SendInputState = "declared" | "pending" | "unplaceable";

/** The mark of a row declared without consuming a send nobody can place. */
export const DECLARED_NO_SEND = "declared_no_send" as const;

/** Precedence over a lead's holds. Pure. */
export function sendInputStateOf(holds: Array<{ declaration: string | null; status: string }>): SendInputState {
  if (holds.some((h) => h.declaration === "declared")) return "declared";
  if (holds.some((h) => h.declaration === null && (h.status === "actual" || h.status === "provisioned"))) return "pending";
  return "unplaceable";
}

/**
 * Each lead's state, read from every hold of every sequence of that lead (a hold
 * joins its sequence by the per-lead campaign id, else, for holds written before
 * migration 0027, by caller campaign + address). A lead with no hold at all is
 * `unplaceable`.
 */
export async function sendInputStates(leadIds: string[]): Promise<Map<string, SendInputState>> {
  const states = new Map<string, SendInputState>();
  const ids = [...new Set(leadIds)];
  if (ids.length === 0) return states;
  const result = await db.execute(sql`
    SELECT c.lead_id, sc.outcomes_declaration AS declaration, sc.status
    FROM instantly_campaigns c
    JOIN sequence_costs sc ON sc.instantly_campaign_id = c.instantly_campaign_id
    WHERE c.lead_id = ANY(${sql.param(ids)}::text[])
    UNION ALL
    SELECT c.lead_id, sc.outcomes_declaration AS declaration, sc.status
    FROM instantly_campaigns c
    JOIN sequence_costs sc
      ON sc.instantly_campaign_id IS NULL AND sc.campaign_id = c.campaign_id AND lower(sc.lead_email) = lower(c.lead_email)
    WHERE c.lead_id = ANY(${sql.param(ids)}::text[])
  `);
  const byLead = new Map<string, Array<{ declaration: string | null; status: string }>>();
  for (const r of result.rows as Array<Record<string, unknown>>) {
    const lead = String(r.lead_id);
    const list = byLead.get(lead) ?? [];
    list.push({ declaration: (r.declaration as string | null) ?? null, status: String(r.status) });
    byLead.set(lead, list);
  }
  for (const id of ids) states.set(id, sendInputStateOf(byLead.get(id) ?? []));
  return states;
}

// ─── The past: consumptions already declared against a send nobody can place ──

const runsUrl = () => process.env.RUNS_SERVICE_URL || "http://localhost:3006";
function runsHeaders(): Record<string, string> {
  return {
    "Content-Type": "application/json",
    "x-api-key": process.env.RUNS_SERVICE_API_KEY || "",
    "x-service-name": "instantly-service",
  };
}

/**
 * Run ids per ledger read. These runs carry many priced children (a day's
 * reply-judging run, an org's visit anchor): 200 a call hit runs-service's
 * statement timeout (500, prod 2026-10-25), 25 answered in ~2 s.
 */
export const LEDGER_READ_RUNS = 20;

/** Per run, the leads whose `message_sent` consumption resolves to NO producer in the ledger. Throws on non-2xx. */
export async function fetchUnknownSendConsumptions(runIds: string[]): Promise<Map<string, Set<string>>> {
  const unknown = new Map<string, Set<string>>();
  for (let i = 0; i < runIds.length; i += LEDGER_READ_RUNS) {
    let cursor: string | null = null;
    do {
      const qs = new URLSearchParams({
        runIds: runIds.slice(i, i + LEDGER_READ_RUNS).join(","),
        direction: "consumed",
        outcomeKey: "message_sent",
        limit: "1000",
      });
      if (cursor) qs.set("cursor", cursor);
      const res = await fetch(`${runsUrl()}/internal/outcome-occurrences?${qs}`, { headers: runsHeaders(), signal: AbortSignal.timeout(120_000) });
      const body = await res.text();
      if (!res.ok) throw new Error(`runs-service outcome-occurrences ${res.status}: ${body.slice(0, 300)}`);
      const page = JSON.parse(body) as {
        occurrences?: Array<{ runId: string; outcomeKey: string; subjectId: string; consumption?: { provenance?: string } | null }>;
        nextCursor?: string | null;
      };
      for (const o of page.occurrences ?? []) {
        if (o.outcomeKey !== "message_sent" || o.consumption?.provenance !== "unknown") continue;
        const set = unknown.get(o.runId) ?? new Set<string>();
        set.add(o.subjectId);
        unknown.set(o.runId, set);
      }
      cursor = page.nextCursor ?? null;
    } while (cursor);
  }
  return unknown;
}

async function voidSendConsumptions(runId: string): Promise<void> {
  const res = await fetch(`${runsUrl()}/v1/platform-runs/${encodeURIComponent(runId)}/outcomes/void`, {
    method: "POST",
    headers: runsHeaders(),
    body: JSON.stringify({
      reason:
        "the occurrence consumed a send of ours that no run can ever declare (run gone, sequence never tied to its run, or run marked by its owner); re-declared consuming only sends that are placed (LEDGER-RESIDUAL-1025)",
      voidedBy: "instantly-service LEDGER-RESIDUAL-1025",
      scope: { direction: "consumed", outcomeKey: "message_sent" },
    }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`runs-service void ${runId} ${res.status}: ${(await res.text()).slice(0, 300)}`);
}

async function redeclareSendConsumption(runId: string, orgId: string, leadId: string, occurredAt: string): Promise<void> {
  const subject = { subjectService: "lead-service", subjectId: leadId };
  const headers = runsHeaders();
  if (/^[0-9a-f-]{36}$/i.test(orgId)) headers["x-org-id"] = orgId;
  const res = await fetch(`${runsUrl()}/v1/platform-runs/${encodeURIComponent(runId)}/outcomes`, {
    method: "POST",
    headers,
    body: JSON.stringify({ produced: [], consumed: [{ outcomeKey: "message_sent", ...subject }], occurredAt }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`runs-service re-declare ${runId} ${res.status}: ${(await res.text()).slice(0, 300)}`);
}

interface DeclaredConsumer {
  id: string;
  kind: "conversation" | "website_visit";
  runId: string;
  orgId: string;
  leadId: string;
  /** When the consumption is re-stated: the first real click, else the run's start. */
  occurredAt: string;
}

/** Every sequence whose positive reply or website visit is declared consuming our send. */
async function loadDeclaredConsumers(): Promise<DeclaredConsumer[]> {
  const result = await db.execute(sql`
    SELECT c.id, 'conversation' AS kind, c.conversation_run_id AS run_id, c.org_id, c.lead_id, NULL AS visited_at
    FROM instantly_campaigns c
    WHERE c.conversation_declaration = 'declared' AND c.conversation_run_id IS NOT NULL AND c.lead_id IS NOT NULL
    UNION ALL
    SELECT c.id, 'website_visit', c.website_visit_run_id, c.org_id, c.lead_id,
      (SELECT to_char(min(e."timestamp"), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id AND lower(e.lead_email) = lower(c.lead_email)
          AND e.event_type = 'email_link_clicked' AND e.inferred = false AND e.withdrawn_at IS NULL)
    FROM instantly_campaigns c
    WHERE c.website_visit_declaration = 'declared' AND c.website_visit_run_id IS NOT NULL AND c.lead_id IS NOT NULL
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => ({
    id: String(r.id),
    kind: r.kind === "conversation" ? "conversation" : "website_visit",
    runId: String(r.run_id),
    orgId: String(r.org_id),
    leadId: String(r.lead_id),
    occurredAt: (r.visited_at as string | null) ?? "run_started_at",
  }));
}

export interface UnplacedSendInputSummary {
  dryRun: boolean;
  /** Declared positive replies + website visits read. */
  consumersRead: number;
  /** Their runs whose `message_sent` consumption names no producer in the ledger. */
  runsWithUnknown: number;
  /** Consumptions of a send nobody can place: taken back, row marked `declared_no_send`. */
  takenBack: number;
  /** Unknown consumptions whose lead still has a send of ours to declare (pending) or a declared one: left as is. */
  leftForTheSendSweep: number;
  /** The other consumptions on the same runs, re-declared after the scoped void. */
  redeclared: number;
}

let inFlight: Promise<UnplacedSendInputSummary> | null = null;

/**
 * LEDGER-RESIDUAL-1025, owner door only (`POST /internal/send-outcomes/unplaced-send-inputs`):
 * for every declared positive reply / website visit whose `message_sent`
 * consumption the LEDGER resolves to no producer AND whose lead has no send of
 * ours that can ever be declared (`unplaceable`, our records): take back the
 * run's `message_sent` consumptions (scoped void, the run stays open), re-declare
 * the run's other consumptions as they were (same date), and mark the row
 * `declared_no_send`. A run holding a `pending` lead is left for a later pass.
 * Idempotent: a marked row is never read again; a re-run takes back nothing.
 * Sequential.
 */
export function restateUnplacedSendInputs(opts: { dryRun: boolean }): Promise<UnplacedSendInputSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const consumers = await loadDeclaredConsumers();
    const summary: UnplacedSendInputSummary = {
      dryRun: opts.dryRun, consumersRead: consumers.length, runsWithUnknown: 0, takenBack: 0, leftForTheSendSweep: 0, redeclared: 0,
    };
    const byRun = new Map<string, DeclaredConsumer[]>();
    for (const c of consumers) byRun.set(c.runId, [...(byRun.get(c.runId) ?? []), c]);
    const unknown = await fetchUnknownSendConsumptions([...byRun.keys()]);
    summary.runsWithUnknown = unknown.size;
    const states = await sendInputStates([...unknown.values()].flatMap((s) => [...s]).concat(
      [...unknown.keys()].flatMap((run) => (byRun.get(run) ?? []).map((c) => c.leadId)),
    ));
    for (const [runId, leads] of unknown) {
      const onRun = byRun.get(runId) ?? [];
      const takeBack = onRun.filter((c) => leads.has(c.leadId) && states.get(c.leadId) === "unplaceable");
      summary.leftForTheSendSweep += [...leads].filter((l) => states.get(l) !== "unplaceable").length;
      if (takeBack.length === 0) continue;
      // A lead whose send is still to be declared rides this run: its consumption must survive the void.
      if (onRun.some((c) => states.get(c.leadId) === "pending")) continue;
      // Every other consumption on the run is put back as it was (a known one stays known).
      const takenLeads = new Set(takeBack.map((c) => c.leadId));
      const keep = [...new Map(onRun.filter((c) => !takenLeads.has(c.leadId)).map((c) => [c.leadId, c])).values()];
      summary.takenBack += takeBack.length;
      summary.redeclared += keep.length;
      if (opts.dryRun) continue;
      await voidSendConsumptions(runId);
      for (const c of keep) await redeclareSendConsumption(runId, c.orgId, c.leadId, c.occurredAt);
      for (const c of takeBack) {
        const column = c.kind === "conversation" ? sql`conversation_declaration` : sql`website_visit_declaration`;
        await db.execute(sql`UPDATE instantly_campaigns SET ${column} = ${DECLARED_NO_SEND} WHERE id = ${c.id}`);
      }
    }
    console.log(`[send-input] unplaced send inputs ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
