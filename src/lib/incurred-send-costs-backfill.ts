/**
 * Put back into the runs-service ledger what the unbilled sends really cost
 * (SCOUT-VISIT-SCREEN-1018), WITHOUT charging anyone.
 *
 * From 2026-08-24 (#622, owner decision: absorb the cold-email infrastructure)
 * to 2026-10-02 (#965, billed again) a queued step carried no cost id
 * (`sequence_costs.cost_id` / `domain_cost_id` NULL, migration 0038). Every one
 * of those steps that really left (a non-withdrawn `email_sent` at its step) is
 * real spend that sits in no runs-service row, so a workflow that sent mostly in
 * that window reads several times cheaper on the INCURRED basis
 * (features-service's workflow ranking) than its siblings.
 *
 * This backfill declares, on each step's OWN run (same org / campaign / workflow
 * attribution the send carried), the two email costs every send of that era
 * carried (`instantly-account-email-sent` + `instantly-domain-email-sent`, qty 1,
 * platform), recorded by runs-service as REAL-BUT-NEVER-CHARGED spend (born in
 * the state a refund leaves a row in, never in a charge predicate) and dated at
 * the email's real send instant. The owner's 08-23 decision stands: no charge,
 * balance, bill, budget pacing or Stripe moves.
 *
 * Scope = every such send, every org, no per-workflow branch. The step's own row
 * is NOT touched here (`cost_id` stays NULL: a NULL there still means "this step
 * was never charged", which the hold settlement relies on); runs-service's
 * per-item idempotency key (`incurredSendCostKey`) makes a re-run write nothing.
 *
 * Price: runs-service prices the names as it prices every send today (the
 * measured real cost per email, a since-inception average, so today's value is
 * the right one for these sends too). Verdict-tiered names (catch-all /
 * unknown) post-date this window (2026-10-14): every send here is the base pair.
 *
 * Manual only, dry-run by default (`POST /internal/backfill/incurred-send-costs`).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { ACCOUNT_EMAIL_SENT_COST, DOMAIN_EMAIL_SENT_COST } from "./send-costs";
import { incurredCosts, type IncurredCostItemRequest, type IncurredCostsResult } from "./runs-client";

/** Why these costs are not charged. Recorded in runs-service's audit trail. */
export const INCURRED_SEND_COST_REASON =
  "Cold email sent while the sending infrastructure was absorbed (owner decision 2026-08-23, instantly-service #622, billed again from 2026-10-02 #965): real spend, never charged to the customer (SCOUT-VISIT-SCREEN-1018).";
/** Who authorized not charging them. */
export const INCURRED_SEND_COST_AUTHORIZED_BY = "kevin@distribute.you (owner decision 2026-08-23)";

/** One unbilled step that really left. */
export interface UnbilledSend {
  holdId: string;
  runId: string;
  orgId: string;
  userId: string;
  sentAt: Date;
}


/** Pure. The per-item dedup key: one per (step hold, side), stable across re-runs. */
export function incurredSendCostKey(holdId: string, side: "account" | "domain"): string {
  return `instantly-incurred-send-cost:${holdId}:${side}`;
}

/** Pure. The two cost items one unbilled send declares. */
export function incurredItemsForSend(send: UnbilledSend): IncurredCostItemRequest[] {
  const incurredAt = send.sentAt.toISOString();
  return [
    {
      runId: send.runId,
      costName: ACCOUNT_EMAIL_SENT_COST,
      quantity: 1,
      idempotencyKey: incurredSendCostKey(send.holdId, "account"),
      incurredAt,
    },
    {
      runId: send.runId,
      costName: DOMAIN_EMAIL_SENT_COST,
      quantity: 1,
      idempotencyKey: incurredSendCostKey(send.holdId, "domain"),
      incurredAt,
    },
  ];
}

function rowsOf<T>(result: unknown): T[] {
  const rows = (result as { rows?: unknown }).rows;
  return (Array.isArray(rows) ? rows : []) as T[];
}

/**
 * Every unbilled step (`actual`, both cost ids NULL) after `afterHoldId`, keyset
 * ordered, with the instant its email really left (earliest non-withdrawn
 * `email_sent` at that step) and the owning org/user. A hold with no such event
 * is NOT returned (counted apart by `countUnbilledWithoutSendEvidence`).
 */
export async function loadUnbilledSends(afterHoldId: string, batch: number): Promise<UnbilledSend[]> {
  const result = await db.execute(sql`
    SELECT sc.id AS "holdId", sc.run_id AS "runId", c.org_id AS "orgId", c.user_id AS "userId",
      -- The column holds UTC without a zone: read it as an ISO instant, never a local Date.
      to_char(e.sent_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "sentAt"
    FROM sequence_costs sc
    JOIN instantly_campaigns c ON c.instantly_campaign_id = sc.instantly_campaign_id
    JOIN LATERAL (
      SELECT MIN(ev."timestamp") AS sent_at
      FROM instantly_events ev
      WHERE ev.campaign_id = sc.instantly_campaign_id
        AND ev.event_type = 'email_sent'
        AND ev.step = sc.step
        AND ev.withdrawn_at IS NULL
    ) e ON e.sent_at IS NOT NULL
    WHERE sc.status = 'actual'
      AND sc.cost_id IS NULL
      AND sc.domain_cost_id IS NULL
      AND c.org_id IS NOT NULL
      AND c.user_id IS NOT NULL
      AND sc.id > ${afterHoldId}
    ORDER BY sc.id
    LIMIT ${batch}
  `);
  return rowsOf<{ holdId: string; runId: string; orgId: string; userId: string; sentAt: string }>(result).map(
    (r) => ({
      holdId: r.holdId,
      runId: r.runId,
      orgId: r.orgId,
      userId: r.userId,
      sentAt: new Date(r.sentAt),
    }),
  );
}

/** Unbilled `actual` steps the backfill cannot declare, by cause. Must be zero, else the run reports them. */
export async function countUnbilledNotDeclarable(): Promise<{ noSendEvidence: number; noOrg: number }> {
  const result = await db.execute(sql`
    SELECT
      COUNT(*) FILTER (WHERE c.org_id IS NOT NULL AND c.user_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM instantly_events ev
        WHERE ev.campaign_id = sc.instantly_campaign_id AND ev.event_type = 'email_sent'
          AND ev.step = sc.step AND ev.withdrawn_at IS NULL))::int AS "noSendEvidence",
      COUNT(*) FILTER (WHERE c.org_id IS NULL OR c.user_id IS NULL)::int AS "noOrg"
    FROM sequence_costs sc
    LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = sc.instantly_campaign_id
    WHERE sc.status = 'actual' AND sc.cost_id IS NULL AND sc.domain_cost_id IS NULL
  `);
  const row = rowsOf<{ noSendEvidence: number; noOrg: number }>(result)[0];
  return { noSendEvidence: Number(row?.noSendEvidence ?? 0), noOrg: Number(row?.noOrg ?? 0) };
}

// ─── Runner ──────────────────────────────────────────────────────────────────

/** runs-service takes at most 500 items per call; one send = two items. */
export const SENDS_PER_CALL = 250;

/** Statuses a cost is CHARGED in. A row the backfill writes must never carry one. */
const CHARGED_STATUSES = new Set(["actual", "provisioned"]);

const SCALE = 10;

/** Pure. Decimal string (runs-service, scale <= 10) to integer units of 1e-10 cent. */
export function toUnits(v: string | null | undefined): bigint {
  if (v === null || v === undefined || v === "") return 0n;
  const neg = v.startsWith("-");
  const [int, frac = ""] = (neg ? v.slice(1) : v).split(".");
  const units = BigInt(int || "0") * 10n ** BigInt(SCALE) + BigInt((frac + "0".repeat(SCALE)).slice(0, SCALE) || "0");
  return neg ? -units : units;
}

/** Pure. Integer units of 1e-10 cent to USD with 2 decimals (rounded to the cent, half up). */
export function unitsToUsd(units: bigint): string {
  const neg = units < 0n;
  const abs = neg ? -units : units;
  const unitsPerCent = 10n ** BigInt(SCALE);
  const usdCents = (abs + unitsPerCent / 2n) / unitsPerCent; // whole cents, half up
  return `${neg ? "-" : ""}${usdCents / 100n}.${(usdCents % 100n).toString().padStart(2, "0")}`;
}

export interface OrgIncurredLine {
  orgId: string;
  sends: number;
  itemsToRecord: number;
  itemsRecorded: number;
  itemsAlreadyRecorded: number;
  incurredUsd: string;
  /** What this run CHARGED the org. Always "0.00"; anything else fails the run. */
  chargedByThisRunUsd: string;
}

export interface IncurredSendCostsSummary {
  dryRun: boolean;
  /** Unbilled steps that really left, seen by this run. */
  sendsSeen: number;
  itemsToRecord: number;
  itemsRecorded: number;
  itemsAlreadyRecorded: number;
  itemsInvalid: number;
  /** Gross incurred spend of the items recorded (apply) or to record (dry run). */
  totalIncurredUsd: string;
  alreadyRecordedUsd: string;
  unitPricesUsdCents: Record<string, string>;
  /** Unbilled `actual` steps not declarable (no real send, or no owner): must be zero. */
  notDeclarable: { noSendEvidence: number; noOrg: number };
  /** Sum of every org's `chargedByThisRunUsd`. Always "0.00". */
  chargedByThisRunUsd: string;
  byOrg: OrgIncurredLine[];
  /** First invalid items (dry run), at most 20. */
  invalidSamples: Array<{ runId: string; idempotencyKey: string; error: string | null }>;
}

type Caller = typeof incurredCosts;

/**
 * Walk every unbilled send that really left and preview (default) or apply its
 * two never-charged costs. Fail loud: a runs-service error, an apply that
 * returns a row in a charged state, or a row count mismatch throws (what was
 * already written stays written and is idempotent on the re-run).
 */
export async function runIncurredSendCostsBackfill(
  opts: { dryRun: boolean; limit?: number },
  call: Caller = incurredCosts,
): Promise<IncurredSendCostsSummary> {
  const mode = opts.dryRun ? "preview" : "apply";
  const byOrg = new Map<string, OrgIncurredLine & { incurred: bigint; charged: bigint }>();
  const unitPrices: Record<string, string> = {};
  const invalidSamples: IncurredSendCostsSummary["invalidSamples"] = [];
  let sendsSeen = 0;
  let toRecord = 0;
  let recorded = 0;
  let already = 0;
  let invalid = 0;
  let total = 0n;
  let alreadyTotal = 0n;
  let charged = 0n;
  let after = "";

  for (;;) {
    const room = opts.limit !== undefined ? opts.limit - sendsSeen : SENDS_PER_CALL;
    if (room <= 0) break;
    const sends = await loadUnbilledSends(after, Math.min(SENDS_PER_CALL, room));
    if (sends.length === 0) break;
    after = sends[sends.length - 1].holdId;
    sendsSeen += sends.length;

    const items = sends.flatMap(incurredItemsForSend);
    const result: IncurredCostsResult = await call(mode, items, {
      reason: INCURRED_SEND_COST_REASON,
      authorizedBy: INCURRED_SEND_COST_AUTHORIZED_BY,
    });
    if (result.items.length !== items.length) {
      throw new Error(`runs-service answered ${result.items.length} items for ${items.length} sent`);
    }
    Object.assign(unitPrices, result.unitPrices);
    toRecord += result.toRecordCount;
    already += result.alreadyRecordedCount;
    invalid += result.invalidCount;
    recorded += result.recordedCount ?? 0;
    total += toUnits(result.totalCostInUsdCents);
    alreadyTotal += toUnits(result.alreadyRecordedTotalCostInUsdCents);

    const orgOfRun = new Map(sends.map((s) => [s.runId, s.orgId]));
    for (const s of sends) {
      const line = byOrg.get(s.orgId) ?? {
        orgId: s.orgId, sends: 0, itemsToRecord: 0, itemsRecorded: 0, itemsAlreadyRecorded: 0,
        incurredUsd: "0.00", chargedByThisRunUsd: "0.00", incurred: 0n, charged: 0n,
      };
      line.sends += 1;
      byOrg.set(s.orgId, line);
    }
    for (const it of result.items) {
      const line = byOrg.get(orgOfRun.get(it.runId) ?? "");
      if (!line) throw new Error(`runs-service answered an item for unknown run ${it.runId}`);
      if (it.outcome === "invalid") {
        if (invalidSamples.length < 20) invalidSamples.push({ runId: it.runId, idempotencyKey: it.idempotencyKey, error: it.error });
        continue;
      }
      if (it.outcome === "already_recorded") {
        line.itemsAlreadyRecorded += 1;
      } else {
        if (it.outcome === "to_record") line.itemsToRecord += 1;
        else line.itemsRecorded += 1;
        line.incurred += toUnits(it.totalCostInUsdCents);
      }
      // A written row in a charged state is exactly what this backfill must never do.
      if (it.status !== null && CHARGED_STATUSES.has(it.status)) {
        const amount = toUnits(it.totalCostInUsdCents);
        line.charged += amount;
        charged += amount;
      }
    }
    if (charged !== 0n) {
      throw new Error(`incurred-send-costs: runs-service holds ${unitsToUsd(charged)} USD of these costs in a CHARGED state; stopped`);
    }
    if (!opts.dryRun && result.items.some((it) => it.outcome !== "recorded" && it.outcome !== "already_recorded")) {
      throw new Error("incurred-send-costs: apply returned items neither recorded nor already recorded");
    }
  }

  return {
    dryRun: opts.dryRun,
    sendsSeen,
    itemsToRecord: toRecord,
    itemsRecorded: recorded,
    itemsAlreadyRecorded: already,
    itemsInvalid: invalid,
    totalIncurredUsd: unitsToUsd(total),
    alreadyRecordedUsd: unitsToUsd(alreadyTotal),
    unitPricesUsdCents: unitPrices,
    notDeclarable: await countUnbilledNotDeclarable(),
    chargedByThisRunUsd: unitsToUsd(charged),
    byOrg: [...byOrg.values()]
      .map(({ incurred, charged: c, ...line }) => ({ ...line, incurredUsd: unitsToUsd(incurred), chargedByThisRunUsd: unitsToUsd(c) }))
      .sort((a, b) => b.sends - a.sends),
    invalidSamples,
  };
}
