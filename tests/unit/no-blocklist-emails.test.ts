/**
 * Owner 2026-10-18: « Arrête de m'envoyer des emails pour ça ». A new blocklist
 * listing (SURBL, Spamhaus DBL, URIBL) emails nobody; the SURBL removal request
 * is still filed, to whitelist@surbl.org only. The admin chart is the surface.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { readFileSync, existsSync } from "node:fs";
import path from "node:path";

vi.mock("../../src/db", () => ({ db: {} }));

const runBlocklistCheck = vi.fn();
const lastCheckAt = vi.fn();
const fileDueRemovals = vi.fn();
vi.mock("../../src/lib/domain-blocklist-sync", () => ({ runBlocklistCheck, lastCheckAt }));
vi.mock("../../src/lib/blocklist-removal", () => ({ fileDueRemovals }));
vi.mock("../../src/lib/owner-identity-client", () => ({ loadOwnerContact: vi.fn() }));

const { blocklistTick } = await import("../../src/lib/domain-blocklist-worker");

const SRC = path.join(__dirname, "..", "..", "src");
const read = (rel: string) => readFileSync(path.join(SRC, rel), "utf8");

describe("a blocklist listing emails nobody", () => {
  const fetchSpy = vi.fn();
  beforeEach(() => {
    vi.stubGlobal("fetch", fetchSpy);
    lastCheckAt.mockResolvedValue(null);
    runBlocklistCheck.mockResolvedValue({ listed: 3, delisted: 0, lists: [] });
    fileDueRemovals.mockResolvedValue({ due: 1, sent: 1, refused: 0, notListed: 0, failed: 0, skippedCoolingDown: 0 });
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it("the daily tick with new listings sends no staff alert, and still files the removal requests", async () => {
    await blocklistTick(new Date("2026-10-18T06:00:00Z"));
    expect(runBlocklistCheck).toHaveBeenCalledTimes(1);
    expect(fileDueRemovals).toHaveBeenCalledTimes(1);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("no code path left that posts a blocklist staff alert", () => {
    expect(existsSync(path.join(SRC, "lib", "domain-blocklist-alert.ts"))).toBe(false);
    expect(read("lib/email-client.ts")).not.toContain("sending_domain_blocklisted");
    expect(read("lib/email-client.ts")).not.toContain("sendDomainBlocklistedAlert");
    expect(read("lib/domain-blocklist-worker.ts")).not.toContain("domain-blocklist-alert");
    expect(read("routes/domain-blocklist.ts")).not.toContain("domain-blocklist-alert");
  });

  it("a new listing is written with no alert to deliver", () => {
    expect(read("lib/domain-blocklist-sync.ts")).not.toMatch(/alertStatus:\s*"pending"/);
  });
});
