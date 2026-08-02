import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import {
  listGandiDomains,
  normalizeGandiDomain,
  normalizeGandiMailbox,
  GandiApiError,
} from "../../src/lib/providers/gandi-client";
import {
  listMailforgeDomains,
  normalizeMailforgeDomain,
} from "../../src/lib/providers/mailforge-client";
import {
  fetchPrimeforgeInventory,
  normalizePrimeforgeDomain,
  normalizePrimeforgeMailbox,
} from "../../src/lib/providers/primeforge-client";
import { normalizeDfyOrder } from "../../src/lib/providers/instantly-dfy-client";
import { parseProviderDate } from "../../src/lib/providers/types";

const originalFetch = global.fetch;

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response;
}

beforeEach(() => {
  vi.resetAllMocks();
});

afterEach(() => {
  global.fetch = originalFetch;
});

describe("parseProviderDate", () => {
  it("returns null for absent / unparseable values rather than inventing a date", () => {
    expect(parseProviderDate(undefined)).toBeNull();
    expect(parseProviderDate(null)).toBeNull();
    expect(parseProviderDate("")).toBeNull();
    expect(parseProviderDate("not-a-date")).toBeNull();
  });

  it("parses an ISO timestamp", () => {
    expect(parseProviderDate("2027-02-03T01:00:13Z")?.toISOString()).toBe(
      "2027-02-03T01:00:13.000Z",
    );
  });
});

describe("normalizeGandiDomain", () => {
  it("maps the registry expiry, the autorenew flag and the reporting organisation", () => {
    const row = normalizeGandiDomain(
      {
        fqdn: "GrowthAgency.dev",
        id: "gandi-id-1",
        autorenew: true,
        dates: {
          created_at: "2026-02-03T02:00:13Z",
          registry_ends_at: "2027-02-03T01:00:13Z",
        },
        status: ["clientTransferProhibited"],
      },
      "org2",
    );

    expect(row.provider).toBe("gandi");
    expect(row.providerAccount).toBe("org2");
    expect(row.domain).toBe("growthagency.dev");
    expect(row.role).toBe("registrar");
    expect(row.autorenew).toBe(true);
    expect(row.expiresAt?.toISOString()).toBe("2027-02-03T01:00:13.000Z");
    expect(row.status).toBe("clientTransferProhibited");
  });

  it("reports a missing autorenew flag as null, never as false", () => {
    const row = normalizeGandiDomain({ fqdn: "x.com" }, "org1");
    expect(row.autorenew).toBeNull();
  });

  it("never invents a price — Gandi exposes renewal pricing on a separate call", () => {
    const row = normalizeGandiDomain({ fqdn: "x.com" }, "org1");
    expect(row.priceCents).toBeNull();
    expect(row.priceCurrency).toBeNull();
  });
});

describe("normalizeGandiMailbox", () => {
  it("keys on the full address and carries the mailbox type as the vendor status", () => {
    const row = normalizeGandiMailbox(
      {
        id: "mbx-1",
        address: "Kevin@GrowthAgency.dev",
        domain: "GrowthAgency.dev",
        mailbox_type: "standard_2023",
      },
      "org2",
    );

    expect(row.email).toBe("kevin@growthagency.dev");
    expect(row.domain).toBe("growthagency.dev");
    expect(row.status).toBe("standard_2023");
    expect(row.providerAccount).toBe("org2");
  });
});

describe("listGandiDomains", () => {
  it("walks `page` until a short page ends the sweep", async () => {
    const fullPage = Array.from({ length: 100 }, (_, i) => ({ fqdn: `d${i}.com` }));
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(fullPage))
      .mockResolvedValueOnce(jsonResponse([{ fqdn: "last.com" }]));
    global.fetch = mockFetch as unknown as typeof fetch;

    const domains = await listGandiDomains({ account: "org2", token: "tok" });

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(domains).toHaveLength(101);
    expect(domains[100].domain).toBe("last.com");
    expect(String(mockFetch.mock.calls[0][0])).toContain("page=1");
    expect(String(mockFetch.mock.calls[1][0])).toContain("page=2");
  });

  it("stops on an empty page without looping forever", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce(jsonResponse([]));
    global.fetch = mockFetch as unknown as typeof fetch;

    const domains = await listGandiDomains({ account: "org1", token: "tok" });

    expect(domains).toEqual([]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("fails loud on a non-2xx instead of reporting an empty estate", async () => {
    global.fetch = vi
      .fn()
      .mockResolvedValue(jsonResponse({ message: "Forbidden" }, 403)) as unknown as typeof fetch;

    await expect(listGandiDomains({ account: "org1", token: "tok" })).rejects.toBeInstanceOf(
      GandiApiError,
    );
  });

  it("sends a browser user-agent — the default agent string is edge-rejected", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce(jsonResponse([]));
    global.fetch = mockFetch as unknown as typeof fetch;

    await listGandiDomains({ account: "org1", token: "tok" });

    const headers = mockFetch.mock.calls[0][1].headers as Record<string, string>;
    expect(headers["User-Agent"]).toContain("Mozilla/5.0");
    expect(headers.Authorization).toBe("Bearer tok");
  });
});

describe("normalizeMailforgeDomain", () => {
  it("joins sld+tld and carries the vendor-reported price", () => {
    const row = normalizeMailforgeDomain({
      id: "dom_1",
      workspaceId: "wks_1",
      sld: "JoinDistribute",
      tld: "com",
      status: "active",
      priceCents: 1400,
      expiresAt: "2027-06-25T00:00:00Z",
    });

    expect(row.domain).toBe("joindistribute.com");
    expect(row.priceCents).toBe(1400);
    expect(row.priceCurrency).toBe("USD");
    expect(row.role).toBe("mailbox");
    expect(row.expiresAt?.toISOString()).toBe("2027-06-25T00:00:00.000Z");
  });

  it("treats an empty autoRenewStatus as unknown, not as disabled", () => {
    const row = normalizeMailforgeDomain({ sld: "a", tld: "com", autoRenewStatus: "" });
    expect(row.autorenew).toBeNull();
  });

  it("reports no currency when the vendor reports no price", () => {
    const row = normalizeMailforgeDomain({ sld: "a", tld: "com" });
    expect(row.priceCents).toBeNull();
    expect(row.priceCurrency).toBeNull();
  });
});

describe("listMailforgeDomains", () => {
  it("uses the raw Authorization header — Bearer and X-Mailforge-Key both fail here", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce(jsonResponse([]));
    global.fetch = mockFetch as unknown as typeof fetch;

    await listMailforgeDomains("mf-key");

    const headers = mockFetch.mock.calls[0][1].headers as Record<string, string>;
    expect(headers.Authorization).toBe("mf-key");
    expect(headers["X-Mailforge-Key"]).toBeUndefined();
  });

  it("paginates on offset and caps the page at 100", async () => {
    const fullPage = Array.from({ length: 100 }, (_, i) => ({ sld: `d${i}`, tld: "com" }));
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(fullPage))
      .mockResolvedValueOnce(jsonResponse([{ sld: "last", tld: "com" }]));
    global.fetch = mockFetch as unknown as typeof fetch;

    const domains = await listMailforgeDomains("mf-key");

    expect(domains).toHaveLength(101);
    expect(String(mockFetch.mock.calls[0][0])).toContain("limit=100&offset=0");
    expect(String(mockFetch.mock.calls[1][0])).toContain("offset=100");
  });
});

describe("normalizePrimeforgeDomain", () => {
  it("skips a pending purchase with no domain name rather than storing a phantom", () => {
    expect(normalizePrimeforgeDomain({ sld: "", tld: "", status: "pending" })).toBeNull();
  });

  it("carries deletionScheduled and leaves the price null — Primeforge exposes no billing", () => {
    const row = normalizePrimeforgeDomain({
      id: "dom_1",
      sld: "AgileConsultCo",
      tld: "com",
      status: "active",
      deletionScheduled: true,
      expiresAt: "2027-07-07T02:30:16Z",
    });

    expect(row?.domain).toBe("agileconsultco.com");
    expect(row?.deletionScheduled).toBe(true);
    expect(row?.priceCents).toBeNull();
  });
});

describe("normalizePrimeforgeMailbox", () => {
  it("resolves the domain from domainId when the row carries only a username", () => {
    const row = normalizePrimeforgeMailbox(
      { id: "mbox_1", username: "michaela", domainId: "dom_1" },
      new Map([["dom_1", "agileconsultco.com"]]),
    );

    expect(row?.email).toBe("michaela@agileconsultco.com");
    expect(row?.domain).toBe("agileconsultco.com");
  });

  it("returns null when nothing resolves to an address", () => {
    expect(normalizePrimeforgeMailbox({ id: "mbox_2" }, new Map())).toBeNull();
  });
});

describe("fetchPrimeforgeInventory", () => {
  it("never requests more than 100 per page — limit=200 silently returns nothing", async () => {
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ results: [{ id: "d1", sld: "a", tld: "com" }] }))
      .mockResolvedValueOnce(jsonResponse({ results: [{ id: "m1", username: "u", domainId: "d1" }] }))
      .mockResolvedValueOnce(jsonResponse({ results: [{ id: "wks_1" }] }));
    global.fetch = mockFetch as unknown as typeof fetch;

    const inventory = await fetchPrimeforgeInventory("pf-key");

    for (const call of mockFetch.mock.calls) {
      expect(String(call[0])).toContain("limit=100");
    }
    expect(inventory.domains).toHaveLength(1);
    expect(inventory.mailboxes[0].email).toBe("u@a.com");
    expect(inventory.accountScopes[0].scope).toBe("workspace:wks_1");
  });
});

describe("normalizeDfyOrder", () => {
  it("marks a cancelled order rather than dropping it — a dead domain is still a cost", () => {
    const row = normalizeDfyOrder({
      domain: "ArcadiaQuest.org",
      timestamp_created: "2025-12-26T22:01:51Z",
      timestamp_cancelled: "2026-05-02T10:00:00Z",
    });

    expect(row.domain).toBe("arcadiaquest.org");
    expect(row.role).toBe("prewarm");
    expect(row.status).toBe("cancelled");
    expect(row.cancelledAt?.toISOString()).toBe("2026-05-02T10:00:00.000Z");
  });

  it("treats a live order as active", () => {
    const row = normalizeDfyOrder({ domain: "resilientnirvana.com", timestamp_cancelled: null });
    expect(row.status).toBe("active");
    expect(row.cancelledAt).toBeNull();
  });
});
