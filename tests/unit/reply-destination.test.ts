import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import { findProspectWroteElsewhere, ownRecipient } from "../../src/lib/reply-destination";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

beforeEach(() => mockDbExecute.mockReset());

describe("findProspectWroteElsewhere", () => {
  it("their newest message went to one of our people's own mailbox: named", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          at: new Date("2026-10-13T15:24:38Z"),
          own: true,
          to: "kevin@distribute.you",
          messageId: "<lukas-2@mail.gmail.com>",
          subject: "Re: Following up",
          refs: ["<46c4e5ef@mtasv.net>", "<kevin-1@mail.gmail.com>"],
        },
      ]),
    );
    await expect(findProspectWroteElsewhere("org-1", "Lukas@Stankamera.com")).resolves.toEqual({
      ownAddress: "kevin@distribute.you",
      to: "kevin@distribute.you",
      at: "2026-10-13T15:24:38.000Z",
      messageId: "<lukas-2@mail.gmail.com>",
      references: ["<46c4e5ef@mtasv.net>", "<kevin-1@mail.gmail.com>", "<lukas-2@mail.gmail.com>"],
      subject: "Re: Following up",
    });
  });

  it("their newest message went to the sending mailbox: nothing to refuse", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([{ at: new Date("2026-10-06T17:08:29Z"), own: false, to: null }]));
    await expect(findProspectWroteElsewhere("org-1", "lukas@stankamera.com")).resolves.toBeNull();
  });

  it("they never wrote: nothing to refuse", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    await expect(findProspectWroteElsewhere("org-1", "lukas@stankamera.com")).resolves.toBeNull();
  });

  it("fails loud when it cannot read", async () => {
    mockDbExecute.mockRejectedValueOnce(new Error("db down"));
    await expect(findProspectWroteElsewhere("org-1", "lukas@stankamera.com")).rejects.toThrow("db down");
  });
});

describe("ownRecipient", () => {
  it("picks our person among the recipients", () => {
    expect(ownRecipient("partner@x.com, Kevin Lourd <kevin@distribute.you>", "lukas@stankamera.com")).toBe(
      "kevin@distribute.you",
    );
  });
  it("none of ours: null", () => {
    expect(ownRecipient("allie@leansignalio.com", "lukas@stankamera.com")).toBeNull();
  });
});

describe("a client's own address recorded at fold-in", () => {
  it("names the address the prospect wrote to, whoever's it is", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          at: new Date("2026-10-13T10:00:00Z"),
          own: true,
          to: "john@client.com",
          ourAddress: "john@client.com",
          messageId: "<p-2@mail>",
          subject: "Re: hi",
          refs: [],
        },
      ]),
    );
    const out = await findProspectWroteElsewhere("org-2", "prospect@x.com");
    expect(out?.ownAddress).toBe("john@client.com");
    expect(out?.references).toEqual(["<p-2@mail>"]);
  });
});
