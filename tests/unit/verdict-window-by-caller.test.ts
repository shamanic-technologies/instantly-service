import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import express from "express";
import request from "supertest";

// VERDICT-WINDOW-BY-CALLER-1126 (ENGINE-GET-ASK-1123): a caller of POST /orgs/send
// and POST /orgs/send-checks states the oldest address verdict it accepts
// (`maxVerdictAgeDays`). It reaches apollo-service's POST /email-verifications
// verbatim (apollo decides reuse vs verify now), and sets when a refusal may be
// re-asked. Stated nothing = byte-identical to before.

process.env.APOLLO_SERVICE_URL = "http://apollo.test";
process.env.APOLLO_SERVICE_API_KEY = "apollo-key";

const mockInsertValues = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: (v: unknown) => mockInsertValues(v) }),
    execute: vi.fn(),
    select: vi.fn(),
  },
}));
vi.mock("../../src/lib/lead-optouts", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findStandingOptOut: async () => null,
}));
vi.mock("../../src/lib/not-a-prospect", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findNotAProspect: async () => null,
}));
vi.mock("../../src/lib/stated-not-to-brand", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findStatedNotToBrand: async () => null,
}));
vi.mock("../../src/lib/trace-event", () => ({ traceEvent: vi.fn(async () => {}) }));

const NOW = new Date("2026-11-26T12:00:00.000Z");
const HELD_AT = "2026-11-16T12:00:00.000Z"; // a verdict held for 10 days

/**
 * A stand-in for apollo-service with ITS documented rule: a held verdict is
 * reused when younger than `maxVerdictAgeDays` (absent = 30), else verified now.
 */
let apolloBodies: Record<string, unknown>[] = [];
let heldVerdict = "valid";
function fakeApollo() {
  return vi.fn(async (_url: string, init: RequestInit) => {
    const body = JSON.parse(String(init.body)) as { emails: string[]; maxVerdictAgeDays?: number };
    apolloBodies.push(body);
    const window = body.maxVerdictAgeDays ?? 30;
    const ageDays = (NOW.getTime() - new Date(HELD_AT).getTime()) / 86_400_000;
    const reused = ageDays < window;
    return new Response(
      JSON.stringify({
        results: body.emails.map((email) => ({
          email,
          verdict: reused ? heldVerdict : "valid",
          verificationId: reused ? "held" : "fresh",
          verifiedAt: reused ? HELD_AT : NOW.toISOString(),
          reused,
        })),
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  });
}

const headers = {
  "x-api-key": "test-api-key",
  "x-org-id": "org-1",
  "x-user-id": "user-1",
  "x-run-id": "run-1",
  "x-brand-id": "brand-1",
};

async function app() {
  process.env.INSTANTLY_SERVICE_API_KEY = headers["x-api-key"];
  const { serviceAuth } = await import("../../src/middleware/serviceAuth");
  const { requireOrgId } = await import("../../src/middleware/requireOrgId");
  const routes = (await import("../../src/routes/send-checks")).default;
  const a = express();
  a.use(express.json());
  a.use("/orgs/send-checks", serviceAuth, requireOrgId, routes);
  return a;
}

describe("VERDICT-WINDOW-BY-CALLER-1126", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(NOW);
    apolloBodies = [];
    heldVerdict = "valid";
    mockInsertValues.mockReset().mockResolvedValue(undefined);
    vi.stubGlobal("fetch", fakeApollo());
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("stating nothing: apollo is asked exactly as before (no window field), the 10-day verdict is reused", async () => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(200);
    expect(apolloBodies).toEqual([{ emails: ["lead@example.com"], source: "instantly-send-gate" }]);
    expect(JSON.stringify(apolloBodies[0])).toBe('{"emails":["lead@example.com"],"source":"instantly-send-gate"}');
    expect(res.body).toMatchObject({ reused: true, verifiedAt: HELD_AT });
  });

  it("a window SHORTER than the held verdict's age gets the address verified again", async () => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: 5 });
    expect(res.status).toBe(200);
    expect(apolloBodies[0]).toEqual({ emails: ["lead@example.com"], source: "instantly-send-gate", maxVerdictAgeDays: 5 });
    expect(res.body).toMatchObject({ reused: false, verifiedAt: NOW.toISOString() });
  });

  it("a window LONGER than the held verdict's age reuses it", async () => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: 60 });
    expect(res.status).toBe(200);
    expect(apolloBodies[0]).toMatchObject({ maxVerdictAgeDays: 60 });
    expect(res.body).toMatchObject({ reused: true, verifiedAt: HELD_AT });
  });

  it("0 = verify now, forwarded as 0 (never dropped as falsy)", async () => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: 0 });
    expect(res.status).toBe(200);
    expect(apolloBodies[0]).toMatchObject({ maxVerdictAgeDays: 0 });
    expect(res.body.reused).toBe(false);
  });

  it("the window never lets a bad verdict through: invalid is refused whatever the window", async () => {
    heldVerdict = "invalid";
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: 60 });
    expect(res.status).toBe(409);
    expect(res.body.code).toBe("email_not_verified");
  });

  it("a refusal may be re-asked once the caller's window has passed, not after 30 days", async () => {
    heldVerdict = "invalid";
    const stated = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: 60 });
    expect(stated.body.until).toBe("2027-01-15T12:00:00.000Z"); // HELD_AT + 60 d
    const unstated = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(unstated.body.until).toBe("2026-12-16T12:00:00.000Z"); // HELD_AT + 30 d, as before
  });

  it.each([-1, 2.5, 3651, "5"])("maxVerdictAgeDays %s is a 400, nothing verified", async (bad) => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com", maxVerdictAgeDays: bad });
    expect(res.status).toBe(400);
    expect(apolloBodies).toHaveLength(0);
  });
});
