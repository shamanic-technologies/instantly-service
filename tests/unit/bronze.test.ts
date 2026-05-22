import { describe, it, expect, vi, beforeEach } from "vitest";

const insertedValues: unknown[] = [];
const mockReturning = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: (v: unknown) => {
        insertedValues.push(v);
        return {
          returning: () => mockReturning(),
          onConflictDoNothing: () => ({
            returning: () => mockReturning(),
          }),
        };
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyWebhookPayloadsRaw: { id: "id" },
  instantlyAnalyticsRaw: { id: "id" },
  instantlyEmailsRaw: { id: "id", instantlyEmailId: "instantly_email_id" },
  instantlyLeadsRaw: { id: "id" },
  instantlyCampaignsConfigRaw: { id: "id" },
}));

import { insertCampaignConfigSnapshot } from "../../src/lib/bronze";

describe("insertCampaignConfigSnapshot", () => {
  beforeEach(() => {
    insertedValues.length = 0;
    mockReturning.mockReset();
  });

  it("inserts a row with payload preserved and returns the row id", async () => {
    mockReturning.mockResolvedValueOnce([{ id: "bronze-cfg-1" }]);

    const payload = {
      id: "inst-camp-1",
      name: "Campaign 1",
      status: 1,
      not_sending_status: 4,
      nested: { foo: "bar" },
    };

    const ref = await insertCampaignConfigSnapshot(
      "inst-camp-1",
      "org-uuid",
      payload,
    );

    expect(ref).toEqual({ id: "bronze-cfg-1" });

    const inserted = insertedValues[0] as Record<string, unknown>;
    expect(inserted.instantlyCampaignId).toBe("inst-camp-1");
    expect(inserted.orgId).toBe("org-uuid");
    expect(inserted.payload).toEqual(payload);
  });

  it("accepts a null orgId (campaigns missing org context)", async () => {
    mockReturning.mockResolvedValueOnce([{ id: "bronze-cfg-2" }]);

    const ref = await insertCampaignConfigSnapshot(
      "inst-camp-2",
      null,
      { id: "inst-camp-2", not_sending_status: null },
    );

    expect(ref.id).toBe("bronze-cfg-2");
    const inserted = insertedValues[0] as Record<string, unknown>;
    expect(inserted.orgId).toBeNull();
    expect(inserted.instantlyCampaignId).toBe("inst-camp-2");
  });
});
