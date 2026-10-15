/**
 * The two sends actually ASK for the copy list.
 *
 * A resolver every send could reach is the feature entirely absent if no send
 * calls it, and that failure is silent: the module is correct, its own tests are
 * green, and the rep is simply never copied. These read the call sites.
 *
 * They also pin the two properties that make the copy safe, which no unit test
 * of the resolver can see: the rep is resolved from state this service already
 * holds rather than from a caller's argument, and the copy is VISIBLE rather
 * than blind on both transports.
 */

import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const read = (p: string) => readFileSync(join(__dirname, "../../src/lib", p), "utf8");

/**
 * Source with its comments removed.
 *
 * A `not.toContain` over raw source trips on the fix's OWN explanation: the
 * comment that says WHY the brand is read off the row rather than off the
 * inbound header has to name that header to be worth reading. Asserting against
 * a comment-stripped copy keeps the guard about the CODE and lets the code stay
 * explained.
 */
function stripComments(src: string): string {
  return src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/.*$/gm, "$1");
}

const REPLY = read("reply-to-lead.ts");
const CLIENT = read("brand-client.ts");

describe("the one-to-one reply copies the rep", () => {
  it("asks the shared resolver on BOTH transports and on the answer from our person's own address", () => {
    expect(REPLY).toContain('import { salesRepCopyList } from "./sales-rep-copy"');
    const asks = REPLY.match(/salesRepCopyList\(campaign\.brandId, input\.orgId\)/g) ?? [];
    expect(asks).toHaveLength(3);
  });

  it("puts the rep on the same VISIBLE cc the agency inbox already rides, on both transports", () => {
    // ONE default copy rule (agency inbox + rep, visible), read by BOTH
    // prepare sites; only a hand-over overrides it (lib/escalate-reply).
    expect(REPLY).toContain("cc: replyCcList(agencyInbox(), salesRepCopy)");
    const sites = REPLY.match(/\.\.\.replyCopy\(input, salesRepCopy\)/g) ?? [];
    expect(sites).toHaveLength(2);
    // The bare agency-only cc must be gone from both prepare sites.
    expect(REPLY).not.toContain("cc: agencyInbox(),");
  });

  it("resolves the brand from the campaign row, never from a caller-supplied header", () => {
    // The row carries it (selected from the campaign this reply belongs to)…
    expect(REPLY).toContain('c.brand_ids[1]          AS "brandId"');
    expect(REPLY).toContain("brandId: string | null;");
    // …and no CODE reads a brand off the request. Comment-stripped: the doc
    // comment explaining this rule necessarily names the header it forbids.
    const code = stripComments(REPLY);
    expect(code).not.toContain("headerBrandId");
    expect(code).not.toContain("x-brand-id");
  });
});

describe("one reader asks brand-service who the rep is", () => {
  it("the phone-only reader is GONE — two readers is how the two sends name two people", () => {
    expect(CLIENT).not.toContain("getSalesRepPhone");
  });

  it("the client reads the rep route, not the deprecated phone alias", () => {
    expect(CLIENT).toContain("/sales-rep`");
    expect(CLIENT).not.toContain("sales-rep-phone");
  });
});
