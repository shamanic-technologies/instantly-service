import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

import { DECLARED_NO_SEND, restateUnplacedSendInputs, sendInputStateOf, sendInputStates } from "../../src/lib/send-input";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const GONE = "11111111-1111-4111-8111-111111111111"; // lead whose send nobody can place
const OK = "22222222-2222-4222-8222-222222222222"; // lead whose send is declared
const WAIT = "33333333-3333-4333-8333-333333333333"; // lead whose send is not declared yet

describe("sendInputStateOf: what a reply / visit may consume (LEDGER-RESIDUAL-1025)", () => {
  it("a declared send of ours wins", () => {
    expect(sendInputStateOf([{ declaration: "run_gone", status: "actual" }, { declaration: "declared", status: "actual" }])).toBe("declared");
  });
  it("a sent or queued send not marked yet: wait for the send sweep", () => {
    expect(sendInputStateOf([{ declaration: null, status: "actual" }])).toBe("pending");
    expect(sendInputStateOf([{ declaration: null, status: "provisioned" }])).toBe("pending");
  });
  it("nothing that can ever be declared: run gone, refused (marked run), cancelled retry, or no hold at all", () => {
    expect(sendInputStateOf([{ declaration: "run_gone", status: "actual" }, { declaration: "refused", status: "actual" }, { declaration: null, status: "cancelled" }])).toBe("unplaceable");
    expect(sendInputStateOf([])).toBe("unplaceable");
  });
  it("a lead with no hold row reads unplaceable", async () => {
    execute.mockReset().mockResolvedValueOnce(pgResult([{ lead_id: OK, declaration: "declared", status: "actual" }]));
    const states = await sendInputStates([OK, GONE]);
    expect(states.get(OK)).toBe("declared");
    expect(states.get(GONE)).toBe("unplaceable");
  });
});

describe("restateUnplacedSendInputs: the past consumptions of a send nobody can place", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  const consumer = (id: string, lead: string, over: Record<string, unknown> = {}) => ({
    id, kind: "website_visit", run_id: RUN, org_id: ORG, lead_id: lead, visited_at: "2026-06-12T09:14:03.000Z", ...over,
  });
  const ledger = (unknownLeads: string[]) =>
    new Response(
      JSON.stringify({
        occurrences: unknownLeads.map((l) => ({ runId: RUN, outcomeKey: "message_sent", subjectId: l, consumption: { provenance: "unknown" } })),
        nextCursor: null,
      }),
      { status: 200 },
    );
  const holds = (rows: Array<[string, string | null, string]>) => pgResult(rows.map(([lead_id, declaration, status]) => ({ lead_id, declaration, status })));

  it("takes back ONLY the unknown consumption of an unplaceable lead, puts the run's other consumptions back, marks the row", async () => {
    execute
      .mockResolvedValueOnce(pgResult([consumer("a", GONE), consumer("b", OK)])) // declared consumers
      .mockResolvedValueOnce(holds([[OK, "declared", "actual"]])) // states
      .mockResolvedValue(pgResult([])); // marks
    fetchMock
      .mockResolvedValueOnce(ledger([GONE])) // ledger read
      .mockResolvedValue(new Response("{}", { status: 201 }));

    const s = await restateUnplacedSendInputs({ dryRun: false });

    expect(s).toMatchObject({ consumersRead: 2, runsWithUnknown: 1, takenBack: 1, redeclared: 1, leftForTheSendSweep: 0 });
    const [voidUrl, voidInit] = fetchMock.mock.calls[1];
    expect(voidUrl).toMatch(new RegExp(`/v1/platform-runs/${RUN}/outcomes/void$`));
    expect(JSON.parse(voidInit.body).scope).toEqual({ direction: "consumed", outcomeKey: "message_sent" });
    const [, redeclare] = fetchMock.mock.calls[2];
    expect(JSON.parse(redeclare.body)).toEqual({
      produced: [],
      consumed: [{ outcomeKey: "message_sent", subjectService: "lead-service", subjectId: OK }],
      occurredAt: "2026-06-12T09:14:03.000Z",
    });
    const mark = JSON.stringify(execute.mock.calls[2][0]);
    expect(mark).toContain("website_visit_declaration");
    expect(mark).toContain(DECLARED_NO_SEND);
  });

  it("a run where a lead's send is still to be declared is left for a later pass (its consumption must survive)", async () => {
    execute
      .mockResolvedValueOnce(pgResult([consumer("a", GONE), consumer("c", WAIT)]))
      .mockResolvedValueOnce(holds([[WAIT, null, "actual"]]));
    fetchMock.mockResolvedValueOnce(ledger([GONE, WAIT]));
    const s = await restateUnplacedSendInputs({ dryRun: false });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(s).toMatchObject({ takenBack: 0, leftForTheSendSweep: 1 });
  });

  it("a known consumption is never touched; a dry run writes nothing", async () => {
    execute.mockResolvedValueOnce(pgResult([consumer("a", GONE)])).mockResolvedValueOnce(holds([]));
    fetchMock.mockResolvedValueOnce(ledger([GONE]));
    const dry = await restateUnplacedSendInputs({ dryRun: true });
    expect(dry.takenBack).toBe(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    execute.mockReset().mockResolvedValueOnce(pgResult([consumer("a", GONE)])).mockResolvedValueOnce(holds([]));
    fetchMock.mockReset().mockResolvedValueOnce(ledger([]));
    const none = await restateUnplacedSendInputs({ dryRun: false });
    expect(none).toMatchObject({ runsWithUnknown: 0, takenBack: 0 });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("a positive reply's consumption is re-declared at its run's start", async () => {
    execute
      .mockResolvedValueOnce(pgResult([consumer("a", GONE, { kind: "conversation", visited_at: null }), consumer("b", OK, { kind: "conversation", visited_at: null })]))
      .mockResolvedValueOnce(holds([[OK, "declared", "actual"]]))
      .mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValueOnce(ledger([GONE])).mockResolvedValue(new Response("{}", { status: 201 }));
    await restateUnplacedSendInputs({ dryRun: false });
    expect(JSON.parse(fetchMock.mock.calls[2][1].body).occurredAt).toBe("run_started_at");
    expect(JSON.stringify(execute.mock.calls[2][0])).toContain("conversation_declaration");
  });
});
