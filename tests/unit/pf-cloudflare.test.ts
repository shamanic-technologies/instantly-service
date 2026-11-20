/**
 * PF-CLOUDFLARE-1120: the Primeforge domains on Cloudflare send through SES,
 * keep receiving replies after their Google mailboxes are gone (Cloudflare Email
 * Routing to the staff Gmail mirror), and stay real senders without a login.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const { execute } = vi.hoisted(() => ({ execute: vi.fn() }));
vi.mock("../../src/db", () => ({ db: { execute } }));

const { sesApi, snsApi } = vi.hoisted(() => ({ sesApi: vi.fn(), snsApi: vi.fn() }));
vi.mock("../../src/lib/aws/ses-account", async (orig) => {
  const real = await orig<typeof import("../../src/lib/aws/ses-account")>();
  return { ...real, sesApi, snsApi };
});

import { dnsHostOf } from "../../src/lib/dns/dns-host";
import { toCloudflareRecord, upsertRecord, type CloudflareClient } from "../../src/lib/dns/cloudflare";
import {
  assertSesRecordAllowed,
  dnsHostBlocker,
  onboardSesDomains,
  sesDnsRecords,
} from "../../src/lib/self-send/ses-domains";
import {
  catchAllIsOurs,
  foreignRootRecords,
  REPLY_ROUTING_DESTINATION,
  switchDue,
  syncEmailRouting,
} from "../../src/lib/self-send/email-routing";
import { withRoutedInboundRead, selectInboundBlindMailboxes } from "../../src/lib/self-send/inbound-blindness";
import {
  coldTopicArn,
  SES_COLD_CONFIGURATION_SET,
  SES_COLD_EVENT_DESTINATION,
  SES_COLD_WEBHOOK_URL,
  type SesAccount,
} from "../../src/lib/aws/ses-account";

const account: SesAccount = {
  accessKeyId: "AKIAEXAMPLE",
  secretAccessKey: "secret",
  region: "us-east-1",
  account: "168197326881",
  smtpUser: "AKIAEXAMPLE",
  smtpPassword: "pw",
  smtpHost: "email-smtp.us-east-1.amazonaws.com",
  topicArn: coldTopicArn("us-east-1", "168197326881"),
};

/** A fake Cloudflare: zones, DNS records, Email Routing; records every call. */
function fakeCloudflare(state: {
  zones: Record<string, string>;
  records?: { id: string; type: string; name: string; content: string; priority?: number }[];
  catchAll?: unknown;
  routing?: { enabled: boolean; status: string };
  verified?: boolean;
}) {
  const calls: { method: string; path: string; body?: unknown }[] = [];
  const records = state.records ?? [];
  let next = 1;
  const client: CloudflareClient = {
    async call<T>(method: string, path: string, body?: unknown): Promise<T> {
      calls.push({ method, path, body });
      const url = new URL(`https://x${path}`);
      if (path.startsWith("/zones?name=")) {
        const name = url.searchParams.get("name")!;
        return (state.zones[name] ? [{ id: state.zones[name], name, status: "active" }] : []) as T;
      }
      if (/\/dns_records$/.test(url.pathname) && method === "GET") {
        const type = url.searchParams.get("type");
        const name = url.searchParams.get("name");
        return records.filter((r) => (!type || r.type === type) && (!name || r.name === name)) as T;
      }
      if (/\/dns_records$/.test(url.pathname) && method === "POST") {
        records.push({ id: `r${next++}`, ...(body as { type: string; name: string; content: string }) });
        return {} as T;
      }
      if (/\/dns_records\/[^/]+$/.test(url.pathname) && method === "PUT") return {} as T;
      if (/\/dns_records\/[^/]+$/.test(url.pathname) && method === "DELETE") {
        const id = url.pathname.split("/").pop();
        const i = records.findIndex((r) => r.id === id);
        if (i >= 0) records.splice(i, 1);
        return {} as T;
      }
      if (url.pathname.endsWith("/email/routing/rules/catch_all")) {
        if (method === "PUT") state.catchAll = { ...(body as object) };
        return (state.catchAll ?? null) as T;
      }
      if (url.pathname.endsWith("/email/routing/dns") && method === "POST") {
        state.routing = { enabled: true, status: "ready" };
        return {} as T;
      }
      if (url.pathname.endsWith("/email/routing")) return (state.routing ?? { enabled: false, status: "unconfigured" }) as T;
      if (url.pathname.endsWith("/email/routing/addresses")) {
        return [{ email: REPLY_ROUTING_DESTINATION, verified: state.verified === false ? null : "2026-11-20T07:30:38Z" }] as T;
      }
      throw new Error(`unexpected ${method} ${path}`);
    },
  };
  return { client, calls, records, state };
}

describe("where a domain's DNS lives is read from its NS, never a list", () => {
  it("cloudflare, gandi, anything else unsupported, a half-moved domain unsupported", () => {
    expect(dnsHostOf(["nico.ns.cloudflare.com.", "shaz.ns.cloudflare.com"]).host).toBe("cloudflare");
    expect(dnsHostOf(["ns-29-a.gandi.net", "ns-140-b.gandi.net", "ns-178-c.gandi.net"]).host).toBe("gandi");
    expect(dnsHostOf(["ns1.primeforge.ai", "ns2.primeforge.ai"]).host).toBe("unsupported");
    expect(dnsHostOf(["nico.ns.cloudflare.com", "ns-29-a.gandi.net"]).host).toBe("unsupported");
    expect(dnsHostOf([]).host).toBe("unsupported");
  });

  it("an unsupported host or a Gandi domain without its organisation blocks; Cloudflare does not need one", () => {
    expect(dnsHostBlocker({ host: "unsupported", nameservers: ["ns1.primeforge.ai"] }, null)).toBe(
      "dns_host_unsupported:ns1.primeforge.ai",
    );
    expect(dnsHostBlocker({ host: "gandi", nameservers: [] }, null)).toBe("dns_host_gandi_without_gandi_org");
    expect(dnsHostBlocker({ host: "cloudflare", nameservers: [] }, null)).toBeNull();
  });
});

describe("SES records on Cloudflare", () => {
  it("maps our zone-relative records to Cloudflare's shape, MX priority split out", () => {
    const records = sesDnsRecords("us-east-1", ["tok1", "tok2", "tok3"]);
    const mapped = records.map((r) => toCloudflareRecord("acutezoneco.com", r));
    expect(mapped).toContainEqual({ type: "CNAME", name: "tok1._domainkey.acutezoneco.com", content: "tok1.dkim.amazonses.com" });
    expect(mapped).toContainEqual({ type: "MX", name: "ses.acutezoneco.com", content: "feedback-smtp.us-east-1.amazonses.com", priority: 10 });
    expect(mapped).toContainEqual({ type: "TXT", name: "ses.acutezoneco.com", content: '"v=spf1 include:amazonses.com ~all"' });
    expect(mapped.some((r) => r.name === "acutezoneco.com")).toBe(false);
  });

  it("the apex and any other MX are refused on every host", () => {
    expect(() => assertSesRecordAllowed("x.com", { name: "@", type: "MX", value: "1 smtp.google.com." })).toThrow(/refused/);
    expect(() => assertSesRecordAllowed("x.com", { name: "mail", type: "MX", value: "1 a." })).toThrow(/refused/);
  });

  it("upsert creates, leaves an equal record alone, updates a different one", async () => {
    const cf = fakeCloudflare({ zones: {} });
    const rec = { type: "CNAME", name: "t._domainkey.x.com", content: "t.dkim.amazonses.com" };
    expect(await upsertRecord(cf.client, "z", rec)).toBe("created");
    expect(await upsertRecord(cf.client, "z", { ...rec, content: "t.dkim.amazonses.com." })).toBe("unchanged");
    expect(await upsertRecord(cf.client, "z", { ...rec, content: "other.dkim.amazonses.com" })).toBe("updated");
  });
});

function sesFake(identity: Record<string, unknown> | null) {
  sesApi.mockImplementation(async (_a: unknown, method: string, path: string) => {
    if (path === `/v2/email/configuration-sets/${SES_COLD_CONFIGURATION_SET}`) return { status: 200, text: "{}", json: {} };
    if (path.endsWith("/event-destinations") && method === "GET") {
      const d = {
        Name: SES_COLD_EVENT_DESTINATION,
        Enabled: true,
        MatchingEventTypes: ["BOUNCE", "COMPLAINT", "DELIVERY_DELAY", "REJECT"],
        SnsDestination: { TopicArn: account.topicArn },
      };
      return { status: 200, text: "{}", json: { EventDestinations: [d] } };
    }
    if (path.startsWith("/v2/email/identities/") && method === "GET") {
      return identity ? { status: 200, text: "{}", json: identity } : { status: 404, text: "", json: {} };
    }
    return { status: 200, text: "{}", json: {} };
  });
  snsApi.mockImplementation(async (_a: unknown, params: Record<string, string>) => {
    if (params.Action === "CreateTopic") return { status: 200, text: `<TopicArn>${account.topicArn}</TopicArn>` };
    return {
      status: 200,
      text: `<member><Endpoint>${SES_COLD_WEBHOOK_URL}</Endpoint><SubscriptionArn>arn:aws:sns:x</SubscriptionArn></member>`,
    };
  });
}

const pendingIdentity = {
  VerifiedForSendingStatus: false,
  ConfigurationSetName: SES_COLD_CONFIGURATION_SET,
  DkimAttributes: { Status: "PENDING", Tokens: ["tok1", "tok2", "tok3"] },
  MailFromAttributes: { MailFromDomain: "ses.acutezoneco.com", MailFromDomainStatus: "PENDING" },
};

describe("SES onboarding of a Cloudflare-hosted domain", () => {
  beforeEach(() => {
    execute.mockReset();
    sesApi.mockReset();
    snsApi.mockReset();
  });

  it("writes the 5 SES records into the Cloudflare zone and nothing at the apex", async () => {
    execute
      .mockResolvedValueOnce({ rows: [{ domain: "acutezoneco.com", org: null }] }) // candidates
      .mockResolvedValue({ rows: [] }); // wasLive, saveState
    sesFake(pendingIdentity);
    const cf = fakeCloudflare({ zones: { "acutezoneco.com": "zone1" } });
    const summary = await onboardSesDomains({
      dryRun: false,
      account,
      cloudflare: cf.client,
      resolveDnsHost: async () => ({ host: "cloudflare", nameservers: ["nico.ns.cloudflare.com"] }),
    });
    expect(summary.failed).toEqual([]);
    expect(summary.pending).toEqual([{ domain: "acutezoneco.com", reason: "dkim_pending" }]);
    expect(cf.records.map((r) => `${r.type} ${r.name}`).sort()).toEqual([
      "CNAME tok1._domainkey.acutezoneco.com",
      "CNAME tok2._domainkey.acutezoneco.com",
      "CNAME tok3._domainkey.acutezoneco.com",
      "MX ses.acutezoneco.com",
      "TXT ses.acutezoneco.com",
    ]);
    expect(cf.records.some((r) => r.name === "acutezoneco.com")).toBe(false);
    // The DNS host is stored with the state.
    const save = execute.mock.calls.map((c) => JSON.stringify(c[0])).find((q) => q.includes("dns_host"));
    expect(save).toBeDefined();
  });

  it("a domain whose NS are elsewhere is pending, never created at SES", async () => {
    execute.mockResolvedValueOnce({ rows: [{ domain: "senddistribute.com", org: null }] }).mockResolvedValue({ rows: [] });
    sesFake(null);
    const cf = fakeCloudflare({ zones: {} });
    const summary = await onboardSesDomains({
      dryRun: false,
      account,
      cloudflare: cf.client,
      resolveDnsHost: async () => ({ host: "unsupported", nameservers: ["ns1.primeforge.ai"] }),
    });
    expect(summary.pending).toEqual([{ domain: "senddistribute.com", reason: "dns_host_unsupported:ns1.primeforge.ai" }]);
    expect(sesApi.mock.calls.some((c) => c[1] === "POST" && c[2] === "/v2/email/identities")).toBe(false);
    expect(cf.calls).toEqual([]);
  });
});

describe("Email Routing: replies come back after the mailboxes are gone", () => {
  beforeEach(() => execute.mockReset());

  it("deletes only the foreign ROOT MX and foreign root SPF", () => {
    const recs = [
      { id: "1", type: "MX", name: "x.com", content: "smtp.google.com" },
      { id: "2", type: "MX", name: "x.com", content: "route1.mx.cloudflare.net" },
      { id: "3", type: "TXT", name: "x.com", content: '"v=spf1 include:_spf.google.com ~all"' },
      { id: "4", type: "TXT", name: "x.com", content: '"google-site-verification=abc"' },
      { id: "5", type: "MX", name: "ses.x.com", content: "feedback-smtp.us-east-1.amazonses.com" },
      { id: "6", type: "TXT", name: "ses.x.com", content: '"v=spf1 include:amazonses.com ~all"' },
      { id: "7", type: "TXT", name: "x.com", content: '"v=spf1 include:_spf.mx.cloudflare.net ~all"' },
    ];
    expect(foreignRootRecords("x.com", recs).map((r) => r.id)).toEqual(["1", "3"]);
  });

  it("the catch-all is ours only when enabled and forwarding to the mirrored staff Gmail", () => {
    expect(catchAllIsOurs({ enabled: true, actions: [{ type: "forward", value: [REPLY_ROUTING_DESTINATION] }] })).toBe(true);
    expect(catchAllIsOurs({ enabled: false, actions: [{ type: "forward", value: [REPLY_ROUTING_DESTINATION] }] })).toBe(false);
    expect(catchAllIsOurs({ enabled: true, actions: [{ type: "drop" }] })).toBe(false);
    expect(catchAllIsOurs(null)).toBe(false);
  });

  it("the switch waits for its stored date and never runs twice", () => {
    const at = new Date("2026-11-28T12:00:00Z");
    expect(switchDue({ mxSwitchAfter: at, mxSwitchedAt: null }, new Date("2026-11-28T11:59:00Z"))).toBe(false);
    expect(switchDue({ mxSwitchAfter: at, mxSwitchedAt: null }, new Date("2026-11-28T12:00:00Z"))).toBe(true);
    expect(switchDue({ mxSwitchAfter: at, mxSwitchedAt: at }, new Date("2026-11-29T00:00:00Z"))).toBe(false);
    expect(switchDue({ mxSwitchAfter: null, mxSwitchedAt: null }, new Date())).toBe(false);
  });

  it("before the date: catch-all prepared, Google MX kept; after it: MX moved and the route is live", async () => {
    const cf = fakeCloudflare({
      zones: { "acutezoneco.com": "z1" },
      records: [
        { id: "g", type: "MX", name: "acutezoneco.com", content: "smtp.google.com", priority: 1 },
        { id: "s", type: "TXT", name: "acutezoneco.com", content: '"v=spf1 include:_spf.google.com ~all"' },
        { id: "d", type: "TXT", name: "_dmarc.acutezoneco.com", content: '"v=DMARC1; p=reject"' },
      ],
    });
    const row = { domain: "acutezoneco.com", mxSwitchAfter: "2026-11-28T12:00:00Z", mxSwitchedAt: null, liveSince: null };
    const ns = async () => ({ host: "cloudflare" as const, nameservers: ["nico.ns.cloudflare.com"] });

    execute.mockResolvedValueOnce({ rows: [row] }).mockResolvedValue({ rows: [] });
    const before = await syncEmailRouting({ dryRun: false, asOf: new Date("2026-11-21T00:00:00Z"), cloudflare: cf.client, resolveDnsHost: ns });
    expect(before.switched).toEqual([]);
    expect(before.waitingForSwitchDate).toEqual([{ domain: "acutezoneco.com", mxSwitchAfter: "2026-11-28T12:00:00.000Z" }]);
    expect(catchAllIsOurs(cf.state.catchAll as never)).toBe(true);
    expect(cf.records.map((r) => r.id).sort()).toEqual(["d", "g", "s"]);
    expect(before.live).toEqual([]);

    execute.mockReset();
    execute.mockResolvedValueOnce({ rows: [row] }).mockResolvedValue({ rows: [] });
    const after = await syncEmailRouting({ dryRun: false, asOf: new Date("2026-11-28T12:15:00Z"), cloudflare: cf.client, resolveDnsHost: ns });
    expect(after.switched).toEqual(["acutezoneco.com"]);
    expect(after.live).toEqual(["acutezoneco.com"]);
    expect(cf.records.map((r) => r.id)).toEqual(["d"]); // DMARC untouched
  });

  it("a domain not on Cloudflare is never touched", async () => {
    const cf = fakeCloudflare({ zones: {} });
    execute
      .mockResolvedValueOnce({ rows: [{ domain: "x.com", mxSwitchAfter: "2026-11-01T00:00:00Z", mxSwitchedAt: null, liveSince: null }] })
      .mockResolvedValue({ rows: [] });
    const summary = await syncEmailRouting({
      dryRun: false,
      cloudflare: cf.client,
      resolveDnsHost: async () => ({ host: "gandi", nameservers: ["ns-1-a.gandi.net"] }),
    });
    expect(summary.pending[0]?.reason).toMatch(/^dns_host_not_cloudflare/);
    expect(cf.calls.filter((c) => c.method !== "GET")).toEqual([]);
  });

  it("an unverified destination holds the MX switch", async () => {
    const cf = fakeCloudflare({
      zones: { "x.com": "z" },
      records: [{ id: "g", type: "MX", name: "x.com", content: "smtp.google.com" }],
      verified: false,
    });
    execute
      .mockResolvedValueOnce({ rows: [{ domain: "x.com", mxSwitchAfter: "2026-11-01T00:00:00Z", mxSwitchedAt: null, liveSince: null }] })
      .mockResolvedValue({ rows: [] });
    const summary = await syncEmailRouting({
      dryRun: false,
      cloudflare: cf.client,
      resolveDnsHost: async () => ({ host: "cloudflare", nameservers: ["a.ns.cloudflare.com"] }),
    });
    expect(summary.pending).toEqual([{ domain: "x.com", reason: "destination_unverified" }]);
    expect(cf.records.map((r) => r.id)).toEqual(["g"]);
  });

  it("an address on a live routed domain is READ (its mailbox may be gone): never blind", () => {
    const rows = [
      { accountEmail: "fallon@acutezoneco.com", sent: 40, hasInbound: false },
      { accountEmail: "kevin@growthagency.ch", sent: 40, hasInbound: false },
    ];
    const blind = selectInboundBlindMailboxes(withRoutedInboundRead(rows, new Set(["acutezoneco.com"])), (a) => a);
    expect([...blind]).toEqual(["kevin@growthagency.ch"]);
  });
});
