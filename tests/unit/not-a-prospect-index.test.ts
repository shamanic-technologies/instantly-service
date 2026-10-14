import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { NOT_A_PROSPECT_REPLY_KINDS } from "../../src/lib/reply-kind";

// DB-LOAD-1014: the not-a-prospect gate is served by a PARTIAL index whose
// predicate names the kinds as literals. A kind added to the code but not to
// the index keeps the answer right and silently brings back the full scan of
// gold on every status read. Change both together.
describe("not-a-prospect partial index", () => {
  it("covers exactly NOT_A_PROSPECT_REPLY_KINDS", () => {
    const migration = readFileSync(
      join(__dirname, "../../drizzle/0076_not_a_prospect_gold_index.sql"),
      "utf8",
    );
    const predicate = /WHERE reply_kind IN \(([^)]*)\)/.exec(migration)?.[1] ?? "";
    const kinds = [...predicate.matchAll(/'([a-z_]+)'/g)].map((m) => m[1]).sort();
    expect(kinds).toEqual([...NOT_A_PROSPECT_REPLY_KINDS].sort());
  });

  it("the query inlines the kinds as literals (a bound kind cannot use a partial index)", () => {
    const source = readFileSync(join(__dirname, "../../src/lib/not-a-prospect.ts"), "utf8");
    expect(source).toContain("sql.raw(NOT_A_PROSPECT_REPLY_KINDS.map");
  });
});
