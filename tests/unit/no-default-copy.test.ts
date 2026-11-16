/**
 * NO-DEFAULT-COPY-1116 (owner 2026-11-16): the one-to-one reply door adds no
 * recipient by itself. Who is copied is the writer's decision (a script, a
 * person in the Unibox); this service sends exactly what it is handed.
 *
 * These read the source: a default copy coming back is a few lines anywhere in
 * the reply module, and its behaviour tests would only cover the paths they
 * thought of.
 */

import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const LIB = join(__dirname, "../../src/lib");
const read = (p: string) => readFileSync(join(LIB, p), "utf8");

function stripComments(src: string): string {
  return src.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:])\/\/.*$/gm, "$1");
}

const REPLY = stripComments(read("reply-to-lead.ts"));

describe("the reply door copies nobody the caller did not name", () => {
  it("never resolves the brand's rep, nor puts the agency inbox on a header", () => {
    expect(REPLY).not.toMatch(/salesRep|getSalesRep/);
    expect(REPLY).not.toMatch(/cc:\s*[^,\n]*agencyInbox\(\)/);
    expect(REPLY).not.toMatch(/bcc:\s*[^,\n]*agencyInbox\(\)/);
    expect(existsSync(join(LIB, "sales-rep-copy.ts"))).toBe(false);
  });

  it("has no 'someone wrote on the thread' rule deciding who is copied", () => {
    expect(REPLY).not.toContain("someoneWrote");
  });

  it("the default copy is empty: only the hand-over's own list fills it", () => {
    expect(REPLY).toContain('return { cc: "", bcc: "" };');
  });
});
