import { describe, it, expect, vi, beforeEach } from "vitest";

const mockInsertValues = vi.fn();
const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({ values: (v: unknown) => mockInsertValues(v) }),
    execute: (...a: unknown[]) => mockExecute(...a),
  },
}));

const mockVerify = vi.fn();
vi.mock("../../src/lib/apollo-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  verifyEmailAddresses: (...a: unknown[]) => mockVerify(...a),
}));

import {
  findEmailNotVerifiedRefusals,
  gateAddresses,
  gateAddressForSend,
  isVendorNoVerdict,
  judgeVerdict,
  setVerifyRetrySleepForTests,
  VERIFY_RETRY_WAITS_MS,
} from "../../src/lib/email-verification-gate";
import { EmailVerifierError } from "../../src/lib/apollo-client";

const IDENTITY = { orgId: "org-1", userId: "user-1", runId: "run-1" };

function verdictRow(email: string, verdict: string, reused = false) {
  return { email, verdict, verificationId: `ver-${email}`, verifiedAt: "2026-10-14T08:00:00.000Z", reused };
}

describe("judgeVerdict — what the send door does with a verdict", () => {
  it.each(["valid", "catch_all", "unknown"])("%s is sent", (v) => {
    expect(judgeVerdict(v)).toEqual({ accepted: true, reason: `verdict_${v}` });
  });
  it.each(["invalid", "risky"])("%s is never sent", (v) => {
    expect(judgeVerdict(v)).toEqual({ accepted: false, reason: `verdict_${v}` });
  });
  it("a verdict the code does not know is never sent", () => {
    expect(judgeVerdict("deliverable_maybe")).toEqual({ accepted: false, reason: "verdict_unrecognised" });
  });
});

const sleeps: number[] = [];

describe("gateAddresses — verify, decide, record", () => {
  beforeEach(() => {
    mockInsertValues.mockReset().mockResolvedValue(undefined);
    mockVerify.mockReset();
    mockExecute.mockReset();
    sleeps.length = 0;
    setVerifyRetrySleepForTests(async (ms) => {
      sleeps.push(ms);
    });
  });

  it("asks apollo with the sending run's identity and records every decision with its reason", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "valid", true), verdictRow("b@x.com", "invalid")]);
    const out = await gateAddresses(["A@x.com", "b@x.com"], IDENTITY, "send");

    expect(mockVerify).toHaveBeenCalledWith(["a@x.com", "b@x.com"], IDENTITY, "instantly-send-gate");
    expect(out.get("a@x.com")).toMatchObject({ accepted: true, reason: "verdict_valid", reused: true });
    expect(out.get("b@x.com")).toMatchObject({ accepted: false, reason: "verdict_invalid", reused: false });
    const rows = mockInsertValues.mock.calls[0][0] as Record<string, unknown>[];
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({
      orgId: "org-1",
      email: "b@x.com",
      verdict: "invalid",
      accepted: false,
      reason: "verdict_invalid",
      context: "send",
      runId: "run-1",
    });
  });

  it("no verdict from the verifier is a RECORDED refusal, never a pass", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("502 email_verification", 502, false));
    const d = await gateAddressForSend("a@x.com", IDENTITY);

    expect(d).toMatchObject({ accepted: false, reason: "verifier_failed", verdict: null });
    expect((mockInsertValues.mock.calls[0][0] as Record<string, unknown>[])[0]).toMatchObject({
      accepted: false,
      reason: "verifier_failed",
    });
  });

  it("a transient verifier failure is asked again with backoff, and the verdict of the next ask is the decision (SEND-CHECK-502-1125)", async () => {
    mockVerify
      .mockRejectedValueOnce(new EmailVerifierError('502 - {"error":"billing-service POST /v1/customer_balance/authorize failed: 502 - Failed to compute balance"}', 502, false))
      .mockRejectedValueOnce(new EmailVerifierError("apollo-service POST /email-verifications unreachable: fetch failed", null, false))
      .mockResolvedValueOnce([verdictRow("a@x.com", "valid")]);
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d).toMatchObject({ accepted: true, reason: "verdict_valid" });
    expect(mockVerify).toHaveBeenCalledTimes(3);
    expect(sleeps).toEqual([...VERIFY_RETRY_WAITS_MS]);
    // ONE decision recorded, the final one: a retry is never a second ledger row.
    expect(mockInsertValues).toHaveBeenCalledTimes(1);
  });

  it("a BounceBan task still verifying is RESUMED by the next ask (free poll), and still pending after the retries is a vendor no-verdict", async () => {
    const pending = new EmailVerifierError(
      'apollo-service POST /email-verifications failed: 502 - {"error":"email verification failed: 1 of 1 address(es) could not be verified — a@x.com: email verification failed: bounceban: bounceban still verifying after 98s (task fri-1); the next ask resumes it"}',
      502,
      false,
    );
    mockVerify.mockRejectedValue(pending);
    const d = await gateAddresses(["a@x.com"], IDENTITY, "check");
    expect(mockVerify).toHaveBeenCalledTimes(1 + VERIFY_RETRY_WAITS_MS.length);
    expect(d.get("a@x.com")).toMatchObject({ accepted: false, reason: "verifier_failed", verdict: null, vendorNoVerdict: true });
    // The ledger keeps `verifier_failed` (a fact about no person), never the in-memory flag.
    expect((mockInsertValues.mock.calls[0][0] as Record<string, unknown>[])[0]).toMatchObject({ reason: "verifier_failed", accepted: false });
  });

  it("an outage that outlasts the retries stays an outage (no vendorNoVerdict)", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("apollo-service POST /email-verifications unreachable: fetch failed", null, false));
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d).toMatchObject({ reason: "verifier_failed" });
    expect(d.vendorNoVerdict).toBeUndefined();
  });

  it("never retries a credit refusal, a 4xx, or the dispatch sweep (`queued` asks again next tick)", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("insufficient balance", 502, true));
    await gateAddressForSend("a@x.com", IDENTITY);
    expect(mockVerify).toHaveBeenCalledTimes(1);

    mockVerify.mockReset().mockRejectedValue(new EmailVerifierError("400 bad request", 400, false));
    await gateAddressForSend("a@x.com", IDENTITY);
    expect(mockVerify).toHaveBeenCalledTimes(1);

    mockVerify.mockReset().mockRejectedValue(new EmailVerifierError("502 boom", 502, false));
    await gateAddresses(["a@x.com"], IDENTITY, "queued");
    expect(mockVerify).toHaveBeenCalledTimes(1);
    expect(sleeps).toEqual([]);
  });

  it("isVendorNoVerdict reads apollo's no-verdict answers, never an outage or a credit refusal", () => {
    const e = (m: string, credits = false) => new EmailVerifierError(m, 502, credits);
    expect(isVendorNoVerdict(e("…: every step-1 verifier failed — contactout: treg contactout responded 404"))).toBe(true);
    expect(isVendorNoVerdict(e("bounceban task ended with status \"failed\""))).toBe(true);
    expect(isVendorNoVerdict(e("bounceban: treg bounceban.people.email.verify responded 503: pool saturated"))).toBe(false);
    expect(isVendorNoVerdict(e("billing-service POST /v1/customer_balance/authorize failed: 502"))).toBe(false);
    expect(isVendorNoVerdict(e("the next ask resumes it", true))).toBe(false);
  });

  it("an org that cannot pay is told apart from an outage", async () => {
    mockVerify.mockRejectedValue(new EmailVerifierError("insufficient balance", 502, true));
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d.reason).toBe("insufficient_credits");
  });

  it("a sales funnel max-budget refusal relayed by apollo keeps billing's named cause on the decision", async () => {
    const cause = { reason: "sales_funnel_max_budget_reached", sales_funnel_cap: { salesFunnelId: "sf-1" } };
    mockVerify.mockRejectedValue(new EmailVerifierError("402", 402, true, cause));
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d.reason).toBe("insufficient_credits");
    expect(d.fundingCause).toEqual(cause);
  });

  it("an address missing from apollo's answer is no verdict", async () => {
    mockVerify.mockResolvedValue([]);
    const d = await gateAddressForSend("a@x.com", IDENTITY);
    expect(d).toMatchObject({ accepted: false, reason: "verifier_failed" });
  });

  it("a ledger that cannot be written fails LOUD (a decision we cannot record is not taken)", async () => {
    mockVerify.mockResolvedValue([verdictRow("a@x.com", "valid")]);
    mockInsertValues.mockRejectedValue(new Error("db down"));
    await expect(gateAddressForSend("a@x.com", IDENTITY)).rejects.toThrow(/db down/);
  });

  it("a non-verifier error propagates", async () => {
    mockVerify.mockRejectedValue(new TypeError("boom"));
    await expect(gateAddressForSend("a@x.com", IDENTITY)).rejects.toThrow(/boom/);
  });

  it("splits into apollo's 50-address batches", async () => {
    const emails = Array.from({ length: 120 }, (_, i) => `p${i}@x.com`);
    mockVerify.mockImplementation(async (chunk: string[]) => chunk.map((e) => verdictRow(e, "valid")));
    const out = await gateAddresses(emails, IDENTITY, "queued");
    expect(mockVerify.mock.calls.map((c) => (c[0] as string[]).length)).toEqual([50, 50, 20]);
    expect(out.size).toBe(120);
  });
});

describe("findEmailNotVerifiedRefusals — the status read's refusal", () => {
  beforeEach(() => mockExecute.mockReset());

  it("states a standing refusal on a verdict, skips an accepted latest decision and a lapsed one", async () => {
    const future = new Date(Date.now() + 86_400_000).toISOString();
    const past = new Date(Date.now() - 86_400_000).toISOString();
    mockExecute.mockResolvedValue({
      rows: [
        { email: "bad@x.com", accepted: false, reason: "verdict_invalid", until: future },
        { email: "ok@x.com", accepted: true, reason: "verdict_valid", until: future },
        { email: "old@x.com", accepted: false, reason: "verdict_risky", until: past },
      ],
    });
    const out = await findEmailNotVerifiedRefusals("org-1", ["Bad@x.com", "ok@x.com", "old@x.com"]);
    expect([...out.keys()]).toEqual(["bad@x.com"]);
    expect(out.get("bad@x.com")!.toISOString()).toBe(future);
  });

  it("reads nothing for no addresses", async () => {
    const out = await findEmailNotVerifiedRefusals("org-1", []);
    expect(out.size).toBe(0);
    expect(mockExecute).not.toHaveBeenCalled();
  });
});
