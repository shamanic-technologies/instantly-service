/**
 * LEDGER-HISTORY-1024: a send run consumes only an occurrence that exists. Until
 * 2026-10-24 every send declared it consumed the lead's `email_checked`; no
 * email check ran on served leads before 2026-09-24 18:12, so ~6,400 leads'
 * sends pointed at a check nobody produced (runs-service reconciliation:
 * unknown provenance). What the email actually went to was the address the
 * reveal found: `email_found`.
 *
 * This re-states the PAST declared sends, through the owner doors only: walk
 * every declared hold (keyset on hold id), read what each run consumed from the
 * ledger (`GET /internal/outcome-occurrences?runIds=`), read which leads have a
 * produced `email_checked` by the end of the send's UTC day, and where the
 * declared key is not the one that exists, take back ONLY that consumption
 * (scoped void, the run stays open) and declare the right one dated at the
 * send. Both directions: a run that consumed `email_found` while a check
 * existed (a check declared after the live sweep read the ledger) is moved
 * back to `email_checked`. Idempotent: a run already right is left alone; a
 * re-run voids nothing.
 */
import {
  BACKFILL_PAGE_HOLDS,
  OCCURRED_AT_RUN_START,
  SEND_UNCHECKED_ADDRESS_OUTCOME,
  addressWasChecked,
  buildSendOutcomeDeclaration,
  fetchEarliestCheckByLead,
  loadBackfillPage,
  planRuns,
  postOutcomesBatch,
  readBatchAnswer,
} from "./send-outcomes";

const CHECKED = "email_checked";
const runsUrl = () => process.env.RUNS_SERVICE_URL || "http://localhost:3006";
function runsHeaders(): Record<string, string> {
  return {
    "Content-Type": "application/json",
    "x-api-key": process.env.RUNS_SERVICE_API_KEY || "",
    "x-service-name": "instantly-service",
  };
}

/** The address keys each run consumed, read from the ledger (500 runs a call, every page). Throws on non-2xx. */
export async function fetchConsumedAddressKeys(runIds: string[]): Promise<Map<string, Set<string>>> {
  const keys = new Map<string, Set<string>>();
  for (let i = 0; i < runIds.length; i += 500) {
    let cursor: string | null = null;
    do {
      const qs = new URLSearchParams({ runIds: runIds.slice(i, i + 500).join(","), direction: "consumed", limit: "1000" });
      if (cursor) qs.set("cursor", cursor);
      const res = await fetch(`${runsUrl()}/internal/outcome-occurrences?${qs}`, { headers: runsHeaders(), signal: AbortSignal.timeout(120_000) });
      const body = await res.text();
      if (!res.ok) throw new Error(`runs-service outcome-occurrences ${res.status}: ${body.slice(0, 300)}`);
      const page = JSON.parse(body) as { occurrences?: Array<{ runId: string; outcomeKey: string }>; nextCursor?: string | null };
      for (const o of page.occurrences ?? []) {
        if (o.outcomeKey !== CHECKED && o.outcomeKey !== SEND_UNCHECKED_ADDRESS_OUTCOME) continue;
        const set = keys.get(o.runId) ?? new Set<string>();
        set.add(o.outcomeKey);
        keys.set(o.runId, set);
      }
      cursor = page.nextCursor ?? null;
    } while (cursor);
  }
  return keys;
}

/**
 * What a run's address consumption needs: take back the wrong key when it is
 * there, declare the right one when it is missing (a run voided by an earlier
 * pass whose re-declaration did not land is completed here). Pure.
 */
export function addressFix(consumed: Set<string> | undefined, checked: boolean): { voidKey: string | null; declare: boolean } {
  const want = checked ? CHECKED : SEND_UNCHECKED_ADDRESS_OUTCOME;
  const wrong = checked ? SEND_UNCHECKED_ADDRESS_OUTCOME : CHECKED;
  return { voidKey: consumed?.has(wrong) ? wrong : null, declare: !consumed?.has(want) };
}

async function voidConsumed(runId: string, outcomeKey: string): Promise<void> {
  const res = await fetch(`${runsUrl()}/v1/platform-runs/${encodeURIComponent(runId)}/outcomes/void`, {
    method: "POST",
    headers: runsHeaders(),
    body: JSON.stringify({
      reason: `the send consumed ${outcomeKey}, but the occurrence that exists for this address is the other one (LEDGER-HISTORY-1024)`,
      voidedBy: "instantly-service LEDGER-HISTORY-1024",
      scope: { direction: "consumed", outcomeKey },
    }),
    signal: AbortSignal.timeout(60_000),
  });
  if (!res.ok) throw new Error(`runs-service void ${runId} ${res.status}: ${(await res.text()).slice(0, 300)}`);
}

export interface CheckRestateSummary {
  holdsWalked: number;
  runsRead: number;
  /** checked -> found: no check of the address was ever produced by the send day. */
  toEmailFound: number;
  /** found -> checked: a check existed. */
  toEmailChecked: number;
}

let inFlight: Promise<CheckRestateSummary> | null = null;

export function restateSendAddressConsumption(opts: { dryRun: boolean; limit?: number }): Promise<CheckRestateSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const summary: CheckRestateSummary = { holdsWalked: 0, runsRead: 0, toEmailFound: 0, toEmailChecked: 0 };
    let cursor = "";
    for (;;) {
      const page = opts.limit === undefined ? BACKFILL_PAGE_HOLDS : Math.min(BACKFILL_PAGE_HOLDS, opts.limit - summary.holdsWalked);
      if (page <= 0) break;
      const { rows, lastHoldId } = await loadBackfillPage(cursor, page, true);
      if (!lastHoldId) break;
      summary.holdsWalked += page;
      cursor = lastHoldId;
      const { runs } = planRuns(rows);
      if (runs.length === 0) continue;
      summary.runsRead += runs.length;
      const [consumed, checks] = await Promise.all([
        fetchConsumedAddressKeys(runs.map((r) => r.runId)),
        fetchEarliestCheckByLead(runs.map((r) => r.leadId)),
      ]);
      const fixes = runs
        .map((r) => {
          const checked = addressWasChecked(checks.get(r.leadId), r.occurredAt, r.occurredAt);
          return { run: r, checked, ...addressFix(consumed.get(r.runId), checked) };
        })
        .filter((f) => f.voidKey !== null || f.declare);
      for (const f of fixes) {
        if (f.checked) summary.toEmailChecked++;
        else summary.toEmailFound++;
      }
      if (opts.dryRun || fixes.length === 0) continue;
      for (const f of fixes) if (f.voidKey) await voidConsumed(f.run.runId, f.voidKey);
      const toDeclare = fixes.filter((f) => f.declare);
      for (let i = 0; i < toDeclare.length; i += 300) {
        const batch = toDeclare.slice(i, i + 300);
        let entries = batch.map((f) => ({ runId: f.run.runId, ...buildSendOutcomeDeclaration(f.run.leadId, f.checked), occurredAt: f.run.occurredAt }));
        for (let attempt = 0; entries.length > 0; attempt++) {
          if (attempt > 2) throw new Error("runs-service still refusing the re-stated sends");
          const posted = await postOutcomesBatch(entries);
          const answer = readBatchAnswer(posted.status, posted.body);
          if (answer.ok) break;
          // An early stamp is re-dated at the run's start once; anything else is logged and dropped.
          const refused = new Map(answer.invalid.map((x) => [x.runId.toLowerCase(), x.reason]));
          entries = entries.flatMap((e) => {
            const reason = refused.get(e.runId.toLowerCase());
            if (!reason) return [e];
            if (reason === "invalid_occurred_at" && e.occurredAt !== OCCURRED_AT_RUN_START) return [{ ...e, occurredAt: OCCURRED_AT_RUN_START }];
            console.error(`[send-check-restate] run ${e.runId} refused (${reason}) after its void: re-run to retry`);
            return [];
          });
        }
      }
    }
    console.log(`[send-check-restate] ${opts.dryRun ? "dry-run " : ""}done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
