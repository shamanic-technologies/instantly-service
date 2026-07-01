import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockFetch = vi.fn();
global.fetch = mockFetch;

const TEST_API_KEY = "test-api-key";

function page(items: unknown[], cursor?: string) {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(cursor ? { items, next_starting_after: cursor } : { items }),
  };
}

describe("instantly-client — inbox-placement pagination", () => {
  beforeEach(() => {
    mockFetch.mockClear();
    vi.resetModules();
  });
  afterEach(() => vi.useRealTimers());

  it("listInboxPlacementTests: traverses pages via next_starting_after, concatenates in order", async () => {
    mockFetch
      .mockResolvedValueOnce(page([{ id: "t1", name: "a", type: 2 }], "cur-1"))
      .mockResolvedValueOnce(page([{ id: "t2", name: "b", type: 2 }]));

    const { listInboxPlacementTests } = await import("../../src/lib/instantly-client");
    const tests = await listInboxPlacementTests(TEST_API_KEY);

    expect(tests.map((t) => t.id)).toEqual(["t1", "t2"]);
    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(String(mockFetch.mock.calls[0][0])).toContain("/inbox-placement-tests?");
    expect(String(mockFetch.mock.calls[1][0])).toContain("starting_after=cur-1");
  });

  it("listInboxPlacementTests: terminates when next_starting_after is null even if items remain", async () => {
    mockFetch.mockResolvedValueOnce(page([{ id: "t1", name: "a", type: 2 }])); // no cursor
    const { listInboxPlacementTests } = await import("../../src/lib/instantly-client");
    const tests = await listInboxPlacementTests(TEST_API_KEY);
    expect(tests).toHaveLength(1);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("listInboxPlacementTests: terminates on empty items (no infinite loop)", async () => {
    mockFetch.mockResolvedValueOnce(page([], "cur-1")); // empty items but a cursor
    const { listInboxPlacementTests } = await import("../../src/lib/instantly-client");
    const tests = await listInboxPlacementTests(TEST_API_KEY);
    expect(tests).toHaveLength(0);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("listInboxPlacementAnalytics: paginates and passes test_id + cursor", async () => {
    mockFetch
      .mockResolvedValueOnce(page([{ id: "a1", test_id: "t1", is_spam: false }], "cur-1"))
      .mockResolvedValueOnce(page([{ id: "a2", test_id: "t1", is_spam: true }], "cur-2"))
      .mockResolvedValueOnce(page([]));

    const { listInboxPlacementAnalytics } = await import("../../src/lib/instantly-client");
    const rows = await listInboxPlacementAnalytics(TEST_API_KEY, "t1");

    expect(rows.map((r) => r.id)).toEqual(["a1", "a2"]);
    expect(mockFetch).toHaveBeenCalledTimes(3);
    for (const call of mockFetch.mock.calls) {
      expect(String(call[0])).toContain("test_id=t1");
    }
    expect(String(mockFetch.mock.calls[1][0])).toContain("starting_after=cur-1");
    expect(String(mockFetch.mock.calls[2][0])).toContain("starting_after=cur-2");
  });

  it("listInboxPlacementAnalytics: terminates on empty items", async () => {
    mockFetch.mockResolvedValueOnce(page([], "cur-1"));
    const { listInboxPlacementAnalytics } = await import("../../src/lib/instantly-client");
    const rows = await listInboxPlacementAnalytics(TEST_API_KEY, "t1");
    expect(rows).toHaveLength(0);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("createInboxPlacementTest: POSTs the body and returns the test", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      json: () => Promise.resolve({ id: "new-test", name: "Fleet", type: 2 }),
    });
    const { createInboxPlacementTest } = await import("../../src/lib/instantly-client");
    const test = await createInboxPlacementTest(TEST_API_KEY, {
      name: "Fleet",
      type: 2,
      sending_method: 1,
      email_subject: "s",
      email_body: "b",
      emails: [],
      recipients_labels: [],
    });
    expect(test.id).toBe("new-test");
    const [, opts] = mockFetch.mock.calls[0];
    expect(opts.method).toBe("POST");
    expect(JSON.parse(opts.body).type).toBe(2);
  });
});
