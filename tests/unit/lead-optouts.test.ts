import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────
//
// The db mock is a chainable thenable: every builder method returns the chain,
// and awaiting it resolves the next queued result. That keeps the tests about
// WHAT the module does (which events it promotes, what it pauses, what it marks
// withdrawn) rather than about drizzle's builder shape.

let selectResults: unknown[] = [];
let insertResults: unknown[] = [];
let updateResults: unknown[] = [];

const mockInsertValues = vi.fn();
const mockUpdateSet = vi.fn();

function chain(pop: () => unknown) {
  const c: Record<string, unknown> = {};
  for (const m of ["from", "leftJoin", "where", "orderBy", "limit", "returning"]) {
    c[m] = () => c;
  }
  (c as any).then = (resolve: (v: unknown) => void, reject: (e: unknown) => void) =>
    Promise.resolve(pop()).then(resolve, reject);
  return c;
}

vi.mock("../../src/db", () => ({
  db: {
    select: () => chain(() => selectResults.shift() ?? []),
    insert: () => ({
      values: (v: unknown) => {
        mockInsertValues(v);
        return chain(() => insertResults.shift() ?? []);
      },
    }),
    update: () => ({
      set: (v: unknown) => {
        mockUpdateSet(v);
        return chain(() => updateResults.shift() ?? []);
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    orgId: "org_id",
    leadEmail: "lead_email",
    instantlyCampaignId: "instantly_campaign_id",
    campaignId: "campaign_id",
    userId: "user_id",
    runId: "run_id",
    status: "status",
  },
  instantlyEvents: {
    sourceRowId: "source_row_id",
    source: "source",
    eventType: "event_type",
    leadEmail: "lead_email",
    campaignId: "campaign_id",
    withdrawnAt: "withdrawn_at",
  },
  instantlyLeadOptoutsRaw: {
    id: "id",
    orgId: "org_id",
    leadEmail: "lead_email",
    statedAt: "stated_at",
  },
  instantlyLeadOptoutWithdrawals: { id: "id", optoutId: "optout_id" },
}));

const mockPromoteEvent = vi.fn();
vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...a: unknown[]) => mockPromoteEvent(...a),
}));

const mockRefresh = vi.fn();
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...a: unknown[]) => mockRefresh(...a),
}));

const mockResolveKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));

const mockUpdateCampaignStatus = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockUpdateCampaignStatus(...a),
}));

const mockStopSelfSend = vi.fn();
vi.mock("../../src/lib/self-send/stop-sequence", () => ({
  stopSelfSendSequence: (...a: unknown[]) => mockStopSelfSend(...a),
}));

import {
  OPT_OUT_CHANNELS,
  isOptOutChannel,
  listLeadOptOuts,
  recordLeadOptOut,
  withdrawLeadOptOut,
} from "../../src/lib/lead-optouts";

const ORG = "org-1";
const LEAD = "alice@media.com";
const STATED_AT = new Date("2026-09-01T09:00:00.000Z");

const OPTOUT_ROW = {
  id: "optout-1",
  orgId: ORG,
  leadEmail: LEAD,
  channel: "sms",
  statedBy: "user-1",
  notes: null,
  statedAt: STATED_AT,
};

function instantlyCampaign(id: string, status = "active") {
  return {
    instantlyCampaignId: id,
    campaignId: "camp-1",
    orgId: ORG,
    userId: "user-9",
    runId: "run-9",
    status,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  selectResults = [];
  insertResults = [];
  updateResults = [];
  mockResolveKey.mockResolvedValue({ key: "api-key" });
  mockUpdateCampaignStatus.mockResolvedValue(undefined);
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
  mockStopSelfSend.mockResolvedValue(undefined);
});

describe("the opt-out channel vocabulary", () => {
  it("is closed — an unlisted channel is not a channel", () => {
    expect(isOptOutChannel("sms")).toBe(true);
    expect(isOptOutChannel("carrier_pigeon")).toBe(false);
    expect(isOptOutChannel(undefined)).toBe(false);
  });

  it("covers the ways a person actually tells us, the unsubscribe link aside", () => {
    expect(OPT_OUT_CHANNELS).toContain("sms");
    expect(OPT_OUT_CHANNELS).toContain("phone_call");
    expect(OPT_OUT_CHANNELS).toContain("in_person");
  });
});

describe("recordLeadOptOut", () => {
  it("promotes the SAME lead_unsubscribed event a clicked unsubscribe promotes, for every campaign", async () => {
    selectResults = [
      [], // no standing opt-out
      [instantlyCampaign("inst-1"), instantlyCampaign("inst-2")],
    ];
    insertResults = [[OPTOUT_ROW]];

    const result = await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "sms",
      statedBy: "user-1",
      payload: { email: LEAD, channel: "sms" },
    });

    expect(result.recorded).toBe(true);
    expect(result.campaignsAffected).toBe(2);
    expect(mockPromoteEvent).toHaveBeenCalledTimes(2);
    for (const call of mockPromoteEvent.mock.calls) {
      expect(call[0]).toMatchObject({
        eventType: "lead_unsubscribed",
        leadEmail: LEAD,
        source: "manual",
        // Points at the consent record — this is what lets a withdrawal find
        // exactly these events later.
        sourceRowId: "optout-1",
        inferred: false,
        // The opt-out is about the whole relationship, not one step of it.
        step: null,
      });
    }
  });

  it("stops the sending at the SENDER too — Instantly never saw the SMS", async () => {
    selectResults = [[], [instantlyCampaign("inst-1")]];
    insertResults = [[OPTOUT_ROW]];

    const result = await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "phone_call",
      statedBy: "user-1",
      payload: {},
    });

    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("api-key", "inst-1", "paused");
    expect(result.campaignsStopped).toBe(1);
  });

  it("stops a self-dispatched sequence LOCALLY — there is no Instantly campaign to pause", async () => {
    selectResults = [[], [instantlyCampaign("self:abc")]];
    insertResults = [[OPTOUT_ROW]];

    await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "sms",
      statedBy: "user-1",
      payload: {},
    });

    expect(mockStopSelfSend).toHaveBeenCalledTimes(1);
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("keeps going when one campaign cannot be stopped, and reports the shortfall", async () => {
    selectResults = [[], [instantlyCampaign("inst-1"), instantlyCampaign("inst-2")]];
    insertResults = [[OPTOUT_ROW]];
    mockUpdateCampaignStatus
      .mockRejectedValueOnce(new Error("instantly 500"))
      .mockResolvedValueOnce(undefined);

    const result = await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "sms",
      statedBy: "user-1",
      payload: {},
    });

    expect(result.campaignsAffected).toBe(2);
    expect(result.campaignsStopped).toBe(1);
    // The local stop still fired for both — the consent record holds either way.
    expect(mockPromoteEvent).toHaveBeenCalledTimes(2);
  });

  it("is idempotent — a standing record writes nothing and fires no side effect", async () => {
    selectResults = [[{ o: OPTOUT_ROW }]];

    const result = await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "sms",
      statedBy: "user-1",
      payload: {},
    });

    expect(result.recorded).toBe(false);
    expect(result.optOut.id).toBe("optout-1");
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockPromoteEvent).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("records the consent even when the org holds no campaign for that address", async () => {
    selectResults = [[], []];
    insertResults = [[OPTOUT_ROW]];

    const result = await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "in_person",
      statedBy: "user-1",
      payload: {},
    });

    // Refusing to record a consent statement because we have nothing to stop
    // would be the wrong direction to be wrong in. Zero is the honest count.
    expect(result.recorded).toBe(true);
    expect(result.campaignsAffected).toBe(0);
    expect(mockPromoteEvent).not.toHaveBeenCalled();
  });

  it("keeps who / when / how on the stored record", async () => {
    selectResults = [[], []];
    insertResults = [[OPTOUT_ROW]];

    await recordLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      channel: "forwarded_thread",
      statedBy: "user-42",
      notes: "asked me over the phone",
      payload: { any: "thing" },
    });

    expect(mockInsertValues).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: ORG,
        leadEmail: LEAD,
        channel: "forwarded_thread",
        statedBy: "user-42",
        notes: "asked me over the phone",
      }),
    );
  });
});

describe("withdrawLeadOptOut", () => {
  it("marks the events it promoted withdrawn — it never deletes them — and refreshes the read", async () => {
    selectResults = [[{ o: OPTOUT_ROW }]];
    insertResults = [[{ withdrawnAt: new Date("2026-09-02T09:00:00.000Z"), withdrawnBy: "user-2" }]];
    updateResults = [[{ campaignId: "inst-1" }, { campaignId: "inst-2" }]];

    const result = await withdrawLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      withdrawnBy: "user-2",
    });

    expect(result).toMatchObject({ withdrawn: true, campaignsAffected: 2 });
    expect(mockUpdateSet).toHaveBeenCalledWith({
      withdrawnAt: new Date("2026-09-02T09:00:00.000Z"),
    });
    expect(mockRefresh).toHaveBeenCalledWith("inst-1", LEAD);
    expect(mockRefresh).toHaveBeenCalledWith("inst-2", LEAD);
  });

  it("does NOT resume the stopped sequences — a new send is a new decision", async () => {
    selectResults = [[{ o: OPTOUT_ROW }]];
    insertResults = [[{ withdrawnAt: new Date(), withdrawnBy: "user-2" }]];
    updateResults = [[{ campaignId: "inst-1" }]];

    await withdrawLeadOptOut({ orgId: ORG, leadEmail: LEAD, withdrawnBy: "user-2" });

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockStopSelfSend).not.toHaveBeenCalled();
  });

  it("refuses distinguishably when nothing stands, and writes nothing", async () => {
    selectResults = [[]];

    const result = await withdrawLeadOptOut({
      orgId: ORG,
      leadEmail: LEAD,
      withdrawnBy: "user-2",
    });

    expect(result).toEqual({ withdrawn: false, reason: "no_standing_optout" });
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });
});

describe("listLeadOptOuts", () => {
  it("returns withdrawn records too, marked — hiding them would destroy the audit", async () => {
    const withdrawnAt = new Date("2026-09-02T09:00:00.000Z");
    selectResults = [
      [
        { o: OPTOUT_ROW, w: { id: "w-1", withdrawnAt, withdrawnBy: "user-2" } },
        { o: { ...OPTOUT_ROW, id: "optout-2" }, w: { id: null, withdrawnAt: null, withdrawnBy: null } },
      ],
    ];

    const rows = await listLeadOptOuts({ orgId: ORG });

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ id: "optout-1", withdrawnAt, withdrawnBy: "user-2" });
    expect(rows[1]).toMatchObject({ id: "optout-2", withdrawnAt: null, withdrawnBy: null });
  });
});
