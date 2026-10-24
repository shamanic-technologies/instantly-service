import { describe, it, expect, vi, beforeEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const resolveLeadIds = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({ resolveLeadIds: (...a: unknown[]) => resolveLeadIds(...a) }));

import { backfillMissingLeadIds } from "../../src/lib/lead-id-backfill";

const pgResult = (rows: unknown[], rowCount = rows.length) => ({ rows, rowCount, command: "SELECT", fields: [] });
const ORG = "06ffd332-0000-4000-8000-000000000001";
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const sqlText = (call: unknown[]) => JSON.stringify((call[0] as { queryChunks?: unknown[] }).queryChunks ?? call[0]);

describe("backfillMissingLeadIds: a send with no lead id is named by lead-service, never invented (LEDGER-HISTORY-1024)", () => {
  beforeEach(() => {
    execute.mockReset();
    resolveLeadIds.mockReset();
  });

  const rows = [
    { id: "a", org_id: ORG, brand_ids: ["b1"], lead_email: "Ann@x.com" },
    { id: "b", org_id: ORG, brand_ids: [], lead_email: "bob@y.com" },
    { id: "c", org_id: "org_legacy", brand_ids: [], lead_email: "c@z.com" },
  ];

  it("asks by org + brands + address, records only what lead-service answers, then lifts the no_lead_id marks", async () => {
    execute.mockResolvedValueOnce(pgResult(rows)).mockResolvedValue(pgResult([], 3));
    resolveLeadIds.mockResolvedValue([LEAD, null]);

    const s = await backfillMissingLeadIds({ dryRun: false });

    expect(resolveLeadIds).toHaveBeenCalledWith([
      { orgId: ORG, email: "Ann@x.com", brandIds: ["b1"] },
      { orgId: ORG, email: "bob@y.com" },
    ]);
    expect(s).toMatchObject({ read: 2, resolved: 1, unresolved: 1 });
    const writes = execute.mock.calls.slice(1).map(sqlText);
    expect(writes[0]).toContain("SET lead_id");
    expect(writes[0]).toContain("lead_id IS NULL");
    expect(writes.filter((w) => w.includes("SET lead_id"))).toHaveLength(1);
    expect(writes.some((w) => w.includes("UPDATE sequence_costs SET outcomes_declared_at = NULL"))).toBe(true);
  });

  it("dry run asks but writes nothing", async () => {
    execute.mockResolvedValueOnce(pgResult(rows));
    resolveLeadIds.mockResolvedValue([LEAD, LEAD]);
    const s = await backfillMissingLeadIds({ dryRun: true });
    expect(s.resolved).toBe(2);
    expect(execute).toHaveBeenCalledTimes(1);
  });
});
