/**
 * PF-CLOUDFLARE-1120: the weekly seed test after Primeforge's Google mailboxes
 * are gone (2026-11-29): no receiver whose inbound moved away (its seeds would
 * read MISSING), and receive-only Google mailboxes from the manual key.
 */
import { describe, it, expect } from "vitest";
import { selectGoogleSeedReceivers } from "../../src/lib/seed-placement/receivers";
import { parseManualCredentials, type MailboxCredential } from "../../src/lib/self-send/mailbox-credentials";

const gmail = (address: string): MailboxCredential => ({
  address,
  appPassword: "x",
  smtpHost: "smtp.gmail.com",
  imapHost: "imap.gmail.com",
});

describe("seed receivers after Primeforge", () => {
  const pool = ["fallon@acutezoneco.com", "alina@helixmeshio.com", "kevin@growthagency.ch"];
  const resolve = (e: string) => (e === "kevin@growthagency.ch" ? { ...gmail(e), imapHost: "mail.gandi.net" } : gmail(e));

  it("a mailbox whose domain's inbound is routed away is no receiver", () => {
    const receivers = selectGoogleSeedReceivers(pool, resolve, new Set(["acutezoneco.com"]), []);
    expect(receivers.map((r) => r.email)).toEqual(["alina@helixmeshio.com"]);
  });

  it("every fleet mailbox routed away: only the receive-only Gmail inboxes, each its own receiver", () => {
    const receivers = selectGoogleSeedReceivers(pool, resolve, new Set(["acutezoneco.com", "helixmeshio.com"]), [
      { address: "seed.b@gmail.com", imapHost: "imap.gmail.com" },
      { address: "seed.a@gmail.com", imapHost: "imap.gmail.com" },
    ]);
    expect(receivers).toEqual([
      { email: "seed.a@gmail.com", recipientEsp: 1 },
      { email: "seed.b@gmail.com", recipientEsp: 1 },
    ]);
  });

  it("a send-only address (no login) is no receiver", () => {
    const receivers = selectGoogleSeedReceivers(["fallon@acutezoneco.com"], () => null, new Set(), []);
    expect(receivers).toEqual([]);
  });

  it("the manual key marks a receive-only mailbox with seedReceiver: true, nothing else does", () => {
    const parsed = parseManualCredentials(
      JSON.stringify([
        { address: "Seed.A@gmail.com", appPassword: "p", smtpHost: "smtp.gmail.com", imapHost: "imap.gmail.com", seedReceiver: true },
        { address: "b@gmail.com", appPassword: "p", smtpHost: "smtp.gmail.com", imapHost: "imap.gmail.com", seedReceiver: "yes" },
      ]),
    );
    expect(parsed[0]).toMatchObject({ address: "seed.a@gmail.com", seedReceiver: true });
    expect(parsed[1]!.seedReceiver).toBeUndefined();
  });
});
