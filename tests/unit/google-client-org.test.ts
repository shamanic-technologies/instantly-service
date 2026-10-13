import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

process.env.GOOGLE_SERVICE_URL = "http://google";
process.env.GOOGLE_SERVICE_API_KEY = "k";

const { fetchOrgMailForAddresses } = await import("../../src/lib/google-client");

const identity = { orgId: "org-1", userId: "u-1", runId: "r-1" };
const fetchMock = vi.fn();

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => vi.unstubAllGlobals());

function json(status: number, body: unknown) {
  return { status, ok: status >= 200 && status < 300, json: async () => body, text: async () => JSON.stringify(body) };
}

const wire = (id: string) => ({
  id: `john@client.com:${id}`,
  gmailMessageId: id,
  messageId: `<${id}@mail>`,
  inReplyTo: null,
  references: [],
  fromEmail: "prospect@x.com",
  to: ["john@client.com"],
  cc: [],
  subject: "Re: hi",
  sentAt: "2026-10-13T10:00:00Z",
  automation: null,
  mimeType: "text/plain",
  bodyText: "sure",
});

describe("fetchOrgMailForAddresses: a client's own connected Gmail", () => {
  it("pages to the end, scoped by the caller's org, and reports the org's own addresses", async () => {
    fetchMock
      .mockResolvedValueOnce(json(200, { ownerAddresses: ["John@Client.com"], messages: [wire("a")], nextCursor: "c1" }))
      .mockResolvedValueOnce(json(200, { ownerAddresses: ["john@client.com"], messages: [wire("b")], nextCursor: null }));
    const out = await fetchOrgMailForAddresses(["prospect@x.com"], identity, new Date("2026-10-10T00:00:00Z"));
    expect(out?.ownerAddresses).toEqual(["john@client.com"]);
    expect(out?.messages.map((m) => m.gmailMessageId)).toEqual(["a", "b"]);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://google/orgs/google/addressed-messages");
    expect(init.headers["x-org-id"]).toBe("org-1");
    expect(JSON.parse(init.body)).toMatchObject({ addresses: ["prospect@x.com"], since: "2026-10-10T00:00:00.000Z" });
    expect(JSON.parse(fetchMock.mock.calls[1][1].body).cursor).toBe("c1");
  });

  it("an org that connected no Gmail is null, not a failure", async () => {
    fetchMock.mockResolvedValueOnce(json(404, { reason: "no_google_account_connected" }));
    await expect(fetchOrgMailForAddresses(["p@x.com"], identity)).resolves.toBeNull();
  });

  it("fails loud on anything else", async () => {
    fetchMock.mockResolvedValueOnce(json(500, { error: "boom" }));
    await expect(fetchOrgMailForAddresses(["p@x.com"], identity)).rejects.toThrow(/500/);
  });
});
