import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";

// SEND-CHECK-BEFORE-WRITE-1120: POST /orgs/send-checks answers the send's own
// refusals on the person, before a writer is asked. The refusal bodies are the
// real modules' (optOutRefusal, notAProspectRefusal, emailNotVerifiedRefusal),
// never a fixture agreeing with itself.

vi.mock("../../src/db", () => ({ db: { execute: vi.fn(), insert: vi.fn(), select: vi.fn() } }));

const mockFindStandingOptOut = vi.hoisted(() => vi.fn());
vi.mock("../../src/lib/lead-optouts", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findStandingOptOut: (...a: unknown[]) => mockFindStandingOptOut(...a),
}));
const mockFindNotAProspect = vi.hoisted(() => vi.fn());
vi.mock("../../src/lib/not-a-prospect", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findNotAProspect: (...a: unknown[]) => mockFindNotAProspect(...a),
}));
const mockFindStated = vi.hoisted(() => vi.fn());
vi.mock("../../src/lib/stated-not-to-brand", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findStatedNotToBrand: (...a: unknown[]) => mockFindStated(...a),
}));
const mockGateAddresses = vi.hoisted(() => vi.fn());
vi.mock("../../src/lib/email-verification-gate", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  gateAddresses: (...a: unknown[]) => mockGateAddresses(...a),
}));
vi.mock("../../src/lib/trace-event", () => ({ traceEvent: vi.fn(async () => {}) }));

const decision = (over: Record<string, unknown>) => ({
  email: "lead@example.com",
  verdict: "valid",
  accepted: true,
  reason: "verdict_valid",
  verificationId: "ver-1",
  verifiedAt: "2026-11-10T08:00:00.000Z",
  reused: true,
  ...over,
});

const headers = {
  "x-api-key": process.env.INSTANTLY_SERVICE_API_KEY || "test-api-key",
  "x-org-id": "org-1",
  "x-user-id": "user-1",
  "x-run-id": "run-1",
  "x-campaign-id": "camp-1",
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

describe("POST /orgs/send-checks", () => {
  beforeEach(() => {
    mockFindStandingOptOut.mockReset().mockResolvedValue(null);
    mockFindNotAProspect.mockReset().mockResolvedValue(null);
    mockFindStated.mockReset().mockResolvedValue(null);
    mockGateAddresses.mockReset().mockImplementation(async () => new Map([["lead@example.com", decision({})]]));
  });

  it("a person the send would mail: 200 sendable with the verdict, verified on the caller's run with context `check`", async () => {
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "Lead@Example.com" });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ sendable: true, to: "Lead@Example.com", verdict: "valid", reused: true, verifiedAt: "2026-11-10T08:00:00.000Z" });
    const [emails, identity, context] = mockGateAddresses.mock.calls[0];
    expect(emails).toEqual(["Lead@Example.com"]);
    expect(context).toBe("check");
    expect(identity).toMatchObject({ orgId: "org-1", userId: "user-1", runId: "run-1", brandIds: ["brand-1"], campaignId: "camp-1" });
  });

  it.each(["invalid", "risky"])("an %s verdict answers the send's 409 email_not_verified", async (verdict) => {
    mockGateAddresses.mockImplementation(async () => new Map([["lead@example.com", decision({ verdict, accepted: false, reason: `verdict_${verdict}` })]]));
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    const { emailNotVerifiedRefusal } = await import("../../src/lib/email-verification-gate");
    expect(res.status).toBe(409);
    expect(res.body).toEqual(emailNotVerifiedRefusal("lead@example.com", decision({ verdict, accepted: false, reason: `verdict_${verdict}` }) as never));
    expect(res.body.code).toBe("email_not_verified");
    expect(res.body.until).toBe("2026-12-10T08:00:00.000Z");
  });

  it("an opted-out person is refused 409 lead_opted_out before any verification is paid", async () => {
    mockFindStandingOptOut.mockResolvedValue({ channel: "email", statedAt: new Date("2026-11-01T00:00:00Z"), id: "o1" });
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(409);
    expect(res.body.code).toBe("lead_opted_out");
    expect(mockGateAddresses).not.toHaveBeenCalled();
  });

  it("a person stated not to the brand is refused 409 stated_not_to_brand, unverified", async () => {
    mockFindStated.mockResolvedValue({ brandId: "brand-1", correctionId: "c1" });
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(409);
    expect(res.body.code).toBe("stated_not_to_brand");
    expect(mockGateAddresses).not.toHaveBeenCalled();
  });

  it("crm-service unreadable answers the send's 502 stated_not_to_brand_unreadable", async () => {
    mockFindStated.mockRejectedValue(new Error("crm down"));
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(502);
    expect(res.body.code).toBe("stated_not_to_brand_unreadable");
  });

  it("no verdict (verifier down) answers 502 email_verification_unavailable", async () => {
    mockGateAddresses.mockImplementation(async () => new Map([["lead@example.com", decision({ verdict: null, accepted: false, reason: "verifier_failed", verifiedAt: null, reused: null })]]));
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(502);
    expect(res.body.code).toBe("email_verification_unavailable");
  });

  it("the verifier ANSWERED it has no verdict (SEND-CHECK-502-1125): the send's 409 email_not_verified, never a 502", async () => {
    const d = decision({ verdict: null, accepted: false, reason: "verifier_failed", verifiedAt: null, reused: null, vendorNoVerdict: true });
    mockGateAddresses.mockImplementation(async () => new Map([["lead@example.com", d]]));
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    const { emailNotVerifiedRefusal } = await import("../../src/lib/email-verification-gate");
    expect(res.status).toBe(409);
    expect(res.body).toEqual(emailNotVerifiedRefusal("lead@example.com", d as never));
    expect(res.body.code).toBe("email_not_verified");
    expect(res.body.verdict).toBeNull();
    expect(res.body.until).toBeNull();
    expect(res.body.details).toMatch(/could give no verdict/);
  });

  it("an org that cannot pay to verify answers 402 insufficient_credits", async () => {
    mockGateAddresses.mockImplementation(async () => new Map([["lead@example.com", decision({ verdict: null, accepted: false, reason: "insufficient_credits", verifiedAt: null, reused: null })]]));
    const res = await request(await app()).post("/orgs/send-checks").set(headers).send({ to: "lead@example.com" });
    expect(res.status).toBe(402);
    expect(res.body.code).toBe("insufficient_credits");
  });

  it("refuses a check with no run (a verification may be billed on it)", async () => {
    const { "x-run-id": _r, ...noRun } = headers;
    const res = await request(await app()).post("/orgs/send-checks").set(noRun).send({ to: "lead@example.com" });
    expect(res.status).toBe(400);
    expect(res.body.code).toBe("send_check_requires_run");
    expect(mockGateAddresses).not.toHaveBeenCalled();
  });
});
