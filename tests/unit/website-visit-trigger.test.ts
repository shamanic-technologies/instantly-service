/**
 * CLICK-FIRES-WEBSITE-VISIT-1015: a click on a link in one of our emails fires the `website_visit` outcome
 * trigger ONCE per person through campaign-service's internal door, retried until answered.
 *
 * The door is modelled with its real dedup rule (an idempotency key is unique per org: a key already held
 * answers the FIRST event, `replayed: true`), so "one event" is asserted where it is decided, not inferred
 * from a call count.
 */
import { createHash } from "node:crypto";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockGetScope = vi.fn();
const mockGetOutcomeTriggers = vi.fn();

// The door: (org, idempotencyKey) -> the first event. `doorFailures` queues errors to throw first.
const doorEvents = new Map<string, { id: string; body: Record<string, unknown> }>();
const doorCalls: Array<Record<string, unknown>> = [];
const doorFailures: Error[] = [];

vi.mock("../../src/lib/campaign-client", async (importOriginal) => {
  const real = (await importOriginal()) as Record<string, unknown>;
  return {
    ...real,
    getCampaignTriggerScope: (...args: unknown[]) => mockGetScope(...args),
    recordTriggerEvent: async (params: Record<string, unknown>) => {
      doorCalls.push(params);
      const failure = doorFailures.shift();
      if (failure) throw failure;
      const key = `${params.orgId}|${params.idempotencyKey}`;
      const existing = doorEvents.get(key);
      if (existing) {
        return { event: { id: existing.id, status: "done", outcome: "ran", skipReason: null, ranCampaignIds: ["c-reactive"], detail: null, triggerId: params.triggerId }, replayed: true };
      }
      const id = `evt-${doorEvents.size + 1}`;
      doorEvents.set(key, { id, body: params });
      return { event: { id, status: "done", outcome: "ran", skipReason: null, ranCampaignIds: ["c-reactive"], detail: null, triggerId: params.triggerId }, replayed: false };
    },
  };
});

vi.mock("../../src/lib/leg-catalogue", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getOutcomeTriggers: (...args: unknown[]) => mockGetOutcomeTriggers(...args),
}));

// The real store is never reached: every test hands its own in-memory store.
vi.mock("../../src/db", () => ({ db: {} }));

const {
  noteWebsiteVisitFromClick,
  deliverOutcomeFire,
  drainOutcomeTriggerFires,
  outcomeIdempotencyKey,
  isFreshClick,
  backoffMs,
  OUTCOME_FRESH_WINDOW_MS,
  WRITER_DELIVERY_LEASE_MS,
} = await import("../../src/lib/website-visit-trigger");
const { TriggerDoorError } = await import("../../src/lib/campaign-client");
type Store = import("../../src/lib/website-visit-trigger").OutcomeFireStore;
type Fire = import("../../src/lib/website-visit-trigger").OutcomeFire;

// ── an in-memory store with the table's own rules ─────────────────────────────────────────────

interface Row extends Fire {
  status: string;
  nextAttemptAt: Date;
  settlement: Record<string, unknown> | null;
}

function memoryStore() {
  const rows: Row[] = [];
  const store: Store = {
    async insertOnce(fire, firstAttemptAt) {
      // UNIQUE (outcome_key, org_id, campaign_id, lead_email)
      if (rows.some((r) => r.outcomeKey === fire.outcomeKey && r.orgId === fire.orgId && r.campaignId === fire.campaignId && r.leadEmail === fire.leadEmail)) {
        return null;
      }
      const row: Row = { ...fire, id: `fire-${rows.length + 1}`, attempts: 0, status: "pending", nextAttemptAt: firstAttemptAt, settlement: null };
      rows.push(row);
      const { status: _s, nextAttemptAt: _n, settlement: _x, ...out } = row;
      return out;
    },
    async claimDue(now, leaseUntil, limit) {
      const due = rows
        .filter((r) => r.status === "pending" && r.nextAttemptAt.getTime() <= now.getTime())
        .sort((a, b) => a.nextAttemptAt.getTime() - b.nextAttemptAt.getTime())
        .slice(0, limit);
      for (const r of due) r.nextAttemptAt = leaseUntil;
      return due.map(({ status: _s, nextAttemptAt: _n, settlement: _x, ...f }) => ({ ...f }));
    },
    async settle(id, s, _now) {
      const r = rows.find((x) => x.id === id);
      if (!r || r.status !== "pending") return;
      if (s.status === "pending") {
        r.attempts = s.attempts;
        r.nextAttemptAt = s.nextAttemptAt;
        r.settlement = { error: s.error };
        return;
      }
      r.status = s.status;
      r.settlement = { ...s };
    },
  };
  return { store, rows };
}

/** Let the detached delivery run to its end. */
const settleDetached = () => new Promise((r) => setTimeout(r, 0));

const NOW = new Date("2026-10-15T12:00:00.000Z");
const CLICKED_AT = new Date("2026-10-15T11:58:00.000Z");
const CAMPAIGN = { instantlyCampaignId: "self:abc", campaignId: "camp-1", orgId: "org-1", leadId: "lead-1" };
const LEAD_SERVICE_KEY = "lead-service:outcome:website_visit:brand-1:offer-1:lead:lead-1";

let errorSpy: ReturnType<typeof vi.spyOn>;
let warnSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  vi.resetAllMocks();
  doorEvents.clear();
  doorCalls.length = 0;
  doorFailures.length = 0;
  mockGetScope.mockResolvedValue({ brandId: "brand-1", offerId: "offer-1" });
  // features-service's catalogue, as served in prod 2026-10-15 (outcome triggers only).
  mockGetOutcomeTriggers.mockResolvedValue([
    { id: "positive_reply_received", outcomeKey: "conversation" },
    { id: "website_visited", outcomeKey: "website_visit" },
    { id: "meeting_booked", outcomeKey: "meeting_booked" },
  ]);
  errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
  warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(console, "log").mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ── AC 1: one click, one event; a second click or a replay, none ──────────────────────────────

describe("a click on our link fires website_visited once per person", () => {
  it("one click → one website_visited event at the door, keyed on the person", async () => {
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, " Prospect@Example.com ", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    expect(doorCalls).toHaveLength(1);
    expect(doorEvents.size).toBe(1);
    expect(doorCalls[0]).toEqual({
      orgId: "org-1",
      triggerId: "website_visited",
      brandId: "brand-1",
      offerId: "offer-1",
      leadId: "lead-1",
      requestedByCampaignId: "camp-1",
      idempotencyKey: LEAD_SERVICE_KEY,
      person: { email: "prospect@example.com" },
      occurredAt: "2026-10-15T11:58:00.000Z",
    });
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ status: "delivered", sourceEventId: "silver-1", leadEmail: "prospect@example.com" });
    expect(rows[0].settlement).toMatchObject({ campaignEventId: "evt-1", replayed: false, triggerId: "website_visited" });
    expect(errorSpy).not.toHaveBeenCalled();
  });

  it("a second click of the same person (another step, another link) records no new event", async () => {
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();
    await noteWebsiteVisitFromClick(CAMPAIGN, "PROSPECT@example.com", new Date(CLICKED_AT.getTime() + 60_000), "silver-2", { store, now: NOW });
    await settleDetached();

    expect(rows).toHaveLength(1);
    expect(doorCalls).toHaveLength(1);
    expect(doorEvents.size).toBe(1);
  });

  it("a replay of the delivery (the drain passing again) re-sends nothing", async () => {
    const { store } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    const counts = await drainOutcomeTriggerFires({ store, now: new Date(NOW.getTime() + 3_600_000) });

    expect(counts).toEqual({ delivered: 0, skipped: 0, refused: 0, retry: 0 });
    expect(doorCalls).toHaveLength(1);
  });

  it("a person lead-service already fired website_visited for (its tracker) stays ONE event: the door replays it", async () => {
    // lead-service rang the door first with its own key for the same outcome, brand, offer and lead.
    doorEvents.set(`org-1|${LEAD_SERVICE_KEY}`, { id: "evt-lead-service", body: {} });
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    expect(doorEvents.size).toBe(1);
    expect(rows[0].settlement).toMatchObject({ status: "delivered", campaignEventId: "evt-lead-service", replayed: true });
  });

  it("the same person clicked from two campaigns of the same brand and offer is one event", async () => {
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();
    await noteWebsiteVisitFromClick({ ...CAMPAIGN, campaignId: "camp-2", instantlyCampaignId: "self:def" }, "prospect@example.com", CLICKED_AT, "silver-9", { store, now: NOW });
    await settleDetached();

    expect(rows).toHaveLength(2);
    expect(doorEvents.size).toBe(1);
    expect(rows[1].settlement).toMatchObject({ replayed: true, campaignEventId: "evt-1" });
  });

  it("the trigger is READ off the catalogue by outcomeKey, never hard-coded", async () => {
    mockGetOutcomeTriggers.mockResolvedValue([{ id: "site_reached_v2", outcomeKey: "website_visit" }]);
    const { store } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    expect(doorCalls[0].triggerId).toBe("site_reached_v2");
  });
});

// ── the key is lead-service's own ────────────────────────────────────────────────────────────

describe("outcomeIdempotencyKey mirrors lead-service byte for byte", () => {
  it("a person with a lead id", () => {
    expect(outcomeIdempotencyKey({ outcomeKey: "website_visit", brandId: "brand-1", offerId: "offer-1", leadId: "lead-1", email: "p@x.com" })).toBe(
      LEAD_SERVICE_KEY,
    );
  });

  it("a person known only by address: the hashed canonical address, as lead-service keys it", () => {
    // lead-service: personKeyOf(null, email) = `email:${email}`, then hash(personKey).slice(0, 32).
    const hash = createHash("sha256").update("email:p@x.com").digest("hex").slice(0, 32);
    expect(outcomeIdempotencyKey({ outcomeKey: "website_visit", brandId: "b", offerId: "o", leadId: null, email: "p@x.com" })).toBe(
      `lead-service:outcome:website_visit:b:o:email:${hash}`,
    );
  });

  it("stays inside campaign-service's 200-character limit", () => {
    const key = outcomeIdempotencyKey({
      outcomeKey: "website_visit",
      brandId: "8c7a4a40-0000-4000-8000-000000000001",
      offerId: "8c7a4a40-0000-4000-8000-000000000002",
      leadId: "8c7a4a40-0000-4000-8000-000000000003",
      email: "p@x.com",
    });
    expect(key.length).toBeLessThanOrEqual(200);
  });
});

// ── never swallowed, never blocking, retried ─────────────────────────────────────────────────

describe("a door that cannot be reached is logged loud and retried", () => {
  it("door down → the fire stays pending with a backoff, logged; the drain delivers it once when due", async () => {
    doorFailures.push(new TriggerDoorError("campaign-service POST /internal/trigger-events failed: 503 - down", 503));
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    expect(rows[0].status).toBe("pending");
    expect(rows[0].attempts).toBe(1);
    expect(doorEvents.size).toBe(0);
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("NOT delivered (attempt 1)"));
    const retryAt = rows[0].nextAttemptAt.getTime();

    // Not due yet: nothing is taken.
    expect(await drainOutcomeTriggerFires({ store, now: new Date(retryAt - 1) })).toEqual({ delivered: 0, skipped: 0, refused: 0, retry: 0 });
    // Due: delivered, once.
    expect(await drainOutcomeTriggerFires({ store, now: new Date(retryAt) })).toEqual({ delivered: 1, skipped: 0, refused: 0, retry: 0 });
    expect(rows[0].status).toBe("delivered");
    expect(doorEvents.size).toBe(1);
    expect(await drainOutcomeTriggerFires({ store, now: new Date(retryAt + 3_600_000) })).toEqual({ delivered: 0, skipped: 0, refused: 0, retry: 0 });
    expect(doorEvents.size).toBe(1);
  });

  it("a network error is retried like an outage", async () => {
    doorFailures.push(new Error("fetch failed"));
    const { store, rows } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();
    expect(rows[0]).toMatchObject({ status: "pending", attempts: 1 });
  });

  it("an unreadable catalogue or campaign is retried, never skipped", async () => {
    mockGetOutcomeTriggers.mockRejectedValueOnce(new Error("features-service 502"));
    mockGetScope.mockRejectedValueOnce(new Error("campaign-service 500"));
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();
    expect(rows[0]).toMatchObject({ status: "pending", attempts: 1 });
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("catalogue unreadable"));

    await drainOutcomeTriggerFires({ store, now: rows[0].nextAttemptAt });
    expect(rows[0]).toMatchObject({ status: "pending", attempts: 2 });
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("campaign scope unreadable"));

    await drainOutcomeTriggerFires({ store, now: rows[0].nextAttemptAt });
    expect(rows[0].status).toBe("delivered");
    expect(doorEvents.size).toBe(1);
  });

  it("a 400 is refused, logged and kept, never re-sent", async () => {
    doorFailures.push(new TriggerDoorError("campaign-service POST /internal/trigger-events failed: 400 - unknown_trigger", 400));
    const { store, rows } = memoryStore();

    await noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW });
    await settleDetached();

    expect(rows[0].status).toBe("refused");
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("REFUSED"));
    await drainOutcomeTriggerFires({ store, now: new Date(NOW.getTime() + 86_400_000) });
    expect(doorCalls).toHaveLength(1);
  });

  it("a fire that cannot be written is logged, and the click is never failed by it", async () => {
    const store: Store = {
      insertOnce: () => Promise.reject(new Error("db down")),
      claimDue: () => Promise.resolve([]),
      settle: () => Promise.resolve(),
    };
    await expect(noteWebsiteVisitFromClick(CAMPAIGN, "prospect@example.com", CLICKED_AT, "silver-1", { store, now: NOW })).resolves.toBeUndefined();
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("could NOT record the fire"));
    expect(doorCalls).toHaveLength(0);
  });

  it("the writer leaves its own row to itself for a lease; the drain does not double-send it", async () => {
    // A writer that died before delivering: the drain takes the row once the lease runs out.
    const { store, rows } = memoryStore();
    const fire = await store.insertOnce(
      { outcomeKey: "website_visit", orgId: "org-1", campaignId: "camp-1", instantlyCampaignId: "self:abc", leadEmail: "p@x.com", leadId: "lead-1", sourceEventId: "s", occurredAt: CLICKED_AT },
      new Date(NOW.getTime() + WRITER_DELIVERY_LEASE_MS),
    );
    expect(fire).not.toBeNull();
    expect((await drainOutcomeTriggerFires({ store, now: NOW })).delivered).toBe(0);
    expect((await drainOutcomeTriggerFires({ store, now: new Date(NOW.getTime() + WRITER_DELIVERY_LEASE_MS) })).delivered).toBe(1);
    expect(rows[0].status).toBe("delivered");
  });
});

// ── what fires nothing ───────────────────────────────────────────────────────────────────────

describe("what fires nothing", () => {
  it("a platform send (no caller campaign, or no org) writes nothing", async () => {
    const { store, rows } = memoryStore();
    await noteWebsiteVisitFromClick({ ...CAMPAIGN, campaignId: null }, "p@x.com", CLICKED_AT, "s", { store, now: NOW });
    await noteWebsiteVisitFromClick({ ...CAMPAIGN, orgId: null }, "p@x.com", CLICKED_AT, "s", { store, now: NOW });
    expect(rows).toHaveLength(0);
  });

  it("a click older than the window (a re-sync of an old snapshot) writes nothing: no back-fill", async () => {
    const { store, rows } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "p@x.com", new Date(NOW.getTime() - OUTCOME_FRESH_WINDOW_MS - 1), "s", { store, now: NOW });
    expect(rows).toHaveLength(0);
    expect(isFreshClick(new Date(NOW.getTime() - OUTCOME_FRESH_WINDOW_MS), NOW)).toBe(true);
  });

  it("a campaign stating no offer is skipped (nothing can answer it), never sent", async () => {
    mockGetScope.mockResolvedValue({ brandId: "brand-1", offerId: null });
    const { store, rows } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "p@x.com", CLICKED_AT, "s", { store, now: NOW });
    await settleDetached();
    expect(rows[0]).toMatchObject({ status: "skipped" });
    expect(rows[0].settlement).toMatchObject({ skipReason: "no_offer" });
    expect(doorCalls).toHaveLength(0);
  });

  it("a catalogue stating no trigger for the outcome is skipped loudly", async () => {
    mockGetOutcomeTriggers.mockResolvedValue([{ id: "positive_reply_received", outcomeKey: "conversation" }]);
    const { store, rows } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "p@x.com", CLICKED_AT, "s", { store, now: NOW });
    await settleDetached();
    expect(rows[0].settlement).toMatchObject({ status: "skipped", skipReason: "no_outcome_trigger" });
    expect(warnSpy).toHaveBeenCalled();
    expect(doorCalls).toHaveLength(0);
  });

  it("a click stamped ahead of our clock is due now, never planned", async () => {
    const { store } = memoryStore();
    await noteWebsiteVisitFromClick(CAMPAIGN, "p@x.com", new Date(NOW.getTime() + 60_000), "s", { store, now: NOW });
    await settleDetached();
    expect(doorCalls[0].occurredAt).toBe(NOW.toISOString());
  });
});

describe("backoffMs", () => {
  it("1 min, doubling, capped at an hour", () => {
    expect(backoffMs(1)).toBe(60_000);
    expect(backoffMs(2)).toBe(120_000);
    expect(backoffMs(20)).toBe(3_600_000);
  });
});

describe("deliverOutcomeFire never throws", () => {
  it("a store that cannot settle is logged, the row stays pending", async () => {
    const store: Store = {
      insertOnce: () => Promise.resolve(null),
      claimDue: () => Promise.resolve([]),
      settle: () => Promise.reject(new Error("db down")),
    };
    const result = await deliverOutcomeFire(
      { id: "f", outcomeKey: "website_visit", orgId: "org-1", campaignId: "camp-1", instantlyCampaignId: "self:abc", leadEmail: "p@x.com", leadId: null, sourceEventId: null, occurredAt: CLICKED_AT, attempts: 0 },
      store,
      NOW,
    );
    expect(result).toBe("retry");
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining("row stays pending"));
  });
});
