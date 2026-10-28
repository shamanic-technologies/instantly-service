import { describe, it, expect } from "vitest";

// BOX-LOAD-1028-PG-CONNS: a Postgres backend keeps the memory of the biggest
// query it ever ran until it exits, so the pool recycles its connections every
// 5 min (node-postgres has no lifetime by default).
describe("db pool recycles its connections", () => {
  it("sets maxLifetimeSeconds 300 beside max 20 and idleTimeoutMillis 60 s", async () => {
    const { db } = await import("../../src/db/index");
    const options = (db as unknown as { $client: { options: Record<string, unknown> } }).$client.options;
    expect(options.maxLifetimeSeconds).toBe(300);
    expect(options.idleTimeoutMillis).toBe(60_000);
    expect(options.max).toBe(20);
  });
});
