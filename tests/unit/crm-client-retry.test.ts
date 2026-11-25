import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CrmStatedReadError, readStatedNotToBrand, readStatedNotToBrandMany, clearStatedSetsCache, setCrmRetrySleepForTests } from "../../src/lib/crm-client";

const ORG = "6a1c0e2d-3b4f-4a5e-8f60-71829304a5b1";
const BRAND = "6a1c0e2d-3b4f-4a5e-8f60-71829304a5c1";
const ok = (stated: boolean) =>
  new Response(JSON.stringify({ orgId: ORG, results: [{ email: "a@b.co", stated, brandId: stated ? BRAND : null, correctionId: stated ? "c1" : null }] }), { status: 200 });
const transport = () => Object.assign(new TypeError("fetch failed"), { cause: { code: "UND_ERR_SOCKET" } });

describe("readStatedNotToBrand transport retry", () => {
  beforeEach(() => {
    process.env.CRM_SERVICE_URL = "http://crm.test";
    process.env.CRM_SERVICE_API_KEY = "k";
    setCrmRetrySleepForTests(async () => {});
    clearStatedSetsCache();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("retries a bare transport failure and answers from the next attempt", async () => {
    const f = vi.fn().mockRejectedValueOnce(transport()).mockResolvedValueOnce(ok(true));
    vi.stubGlobal("fetch", f);
    const out = await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] });
    expect(f).toHaveBeenCalledTimes(2);
    expect(out.get("a@b.co")).toEqual({ brandId: BRAND, correctionId: "c1" });
  });

  it("fails loud after the retries, naming the cause", async () => {
    const f = vi.fn().mockRejectedValue(transport());
    vi.stubGlobal("fetch", f);
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] })).rejects.toThrow(/UND_ERR_SOCKET/);
    expect(f).toHaveBeenCalledTimes(3);
  });

  it("never retries an HTTP answer", async () => {
    const http = vi.fn().mockResolvedValue(new Response("boom", { status: 500 }));
    vi.stubGlobal("fetch", http);
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] })).rejects.toBeInstanceOf(CrmStatedReadError);
    expect(http).toHaveBeenCalledTimes(1);
  });

  it("the send gate's read asks again after a timeout (SEND-CHECK-502-1125: crm-service stalls, the next ask answers)", async () => {
    const timedOut = () => Object.assign(new Error("The operation was aborted due to timeout"), { name: "TimeoutError" });
    const f = vi.fn().mockRejectedValueOnce(timedOut()).mockResolvedValueOnce(ok(false));
    vi.stubGlobal("fetch", f);
    const out = await readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] });
    expect(f).toHaveBeenCalledTimes(2);
    expect(out.size).toBe(0);

    const always = vi.fn().mockRejectedValue(timedOut());
    vi.stubGlobal("fetch", always);
    await expect(readStatedNotToBrand({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] })).rejects.toThrow(/unreachable: The operation was aborted due to timeout/);
    expect(always).toHaveBeenCalledTimes(3);
  });

  it("the many-address read never re-asks a slow crm-service (load)", async () => {
    const timeout = vi.fn().mockRejectedValue(Object.assign(new Error("timed out"), { name: "TimeoutError" }));
    vi.stubGlobal("fetch", timeout);
    await expect(readStatedNotToBrandMany({ orgId: ORG, brandIds: [BRAND], emails: ["a@b.co"] })).rejects.toBeInstanceOf(CrmStatedReadError);
    expect(timeout).toHaveBeenCalledTimes(1);
  });
});
