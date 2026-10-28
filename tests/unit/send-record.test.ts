import { createHash } from "node:crypto";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mockValues = vi.fn();
// A plain function decides the outcome (a rejecting vi.fn result is reported by
// vitest as a test error even once handled).
let insertFails = false;
vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: async (row: unknown) => {
        mockValues(row);
        if (insertFails) throw new Error("connection reset");
      },
    }),
  },
}));

const { recordSentMessage } = await import("../../src/lib/self-send/send-record");

const record = {
  dispatchId: "d-1",
  instantlyCampaignId: "self:1",
  leadEmail: "p@lead.test",
  accountEmail: "amy@a.test",
  step: 1,
  kind: "sequence_step" as const,
  messageId: "<m1@a.test>",
  rawMessage: Buffer.from("Subject: Hi\r\n\r\nbody\r\n"),
  messageInputs: { subject: "Hi" },
  decision: { selectedAt: new Date("2026-10-28T10:00:00Z") },
  sentAt: new Date("2026-10-28T10:00:05Z"),
};

beforeEach(() => {
  mockValues.mockReset();
  insertFails = false;
});

describe("recordSentMessage (FACTORY-RECORD-SEND-1028)", () => {
  it("stores the exact bytes with their sha256 and length, inputs as JSON", async () => {

    await expect(recordSentMessage(record)).resolves.toBe(true);

    const row = mockValues.mock.calls[0]![0] as Record<string, unknown>;
    expect(row.rawMessage).toBe(record.rawMessage);
    expect(row.rawSha256).toBe(createHash("sha256").update(record.rawMessage).digest("hex"));
    expect(row.rawBytes).toBe(record.rawMessage.length);
    expect(row.decision).toEqual({ selectedAt: "2026-10-28T10:00:00.000Z" });
  });

  it("never throws: the email already left", async () => {
    insertFails = true;
    const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

    await expect(recordSentMessage(record)).resolves.toBe(false);
    expect(error.mock.calls[0]![0]).toMatch(/NOT recorded campaign=self:1 step=1/);
    error.mockRestore();
  });
});
