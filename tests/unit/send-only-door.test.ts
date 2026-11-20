/**
 * PF-CLOUDFLARE-1120: a send-only address (domain live on SES, no login held)
 * leaves through SES; with SES off it has no way out and says so.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const { execute } = vi.hoisted(() => ({ execute: vi.fn() }));
vi.mock("../../src/db", () => ({ db: { execute } }));

const { resolveMailboxCredential } = vi.hoisted(() => ({ resolveMailboxCredential: vi.fn() }));
vi.mock("../../src/lib/self-send/mailbox-credentials", async (orig) => {
  const real = await orig<typeof import("../../src/lib/self-send/mailbox-credentials")>();
  return { ...real, resolveMailboxCredential };
});

const { sendThroughSes } = vi.hoisted(() => ({ sendThroughSes: vi.fn() }));
vi.mock("../../src/lib/self-send/ses-transport", () => ({ sendThroughSes }));

import { MailboxCredentialError } from "../../src/lib/self-send/mailbox-credentials";
import {
  clearLivePipesCache,
  resolveSendCredential,
  sendFromMailbox,
  SES_KILL_SWITCH_ENV,
} from "../../src/lib/self-send/outbound";
import { loadSendOnlyCandidates } from "../../src/lib/self-send/send-only-senders";
import type { BuiltMessage } from "../../src/lib/self-send/message";

const CALLER = { method: "POST", path: "/t" };
const message = { from: "Fallon <fallon@acutezoneco.com>", to: "lead@x.com" } as unknown as BuiltMessage;

beforeEach(() => {
  clearLivePipesCache();
  execute.mockReset();
  execute.mockResolvedValue({ rows: [{ pipe: "ses", domain: "acutezoneco.com" }] });
  resolveMailboxCredential.mockReset();
  resolveMailboxCredential.mockRejectedValue(new MailboxCredentialError("No Primeforge mailbox found"));
  sendThroughSes.mockReset();
  sendThroughSes.mockResolvedValue({ messageId: "<a@email.amazonses.com>", response: "250 Ok a", accepted: [], rejected: [], rawMessage: Buffer.from("") });
});
afterEach(() => {
  delete process.env[SES_KILL_SWITCH_ENV];
});

describe("send-only addresses", () => {
  it("no login + live SES domain = null credential, and the email leaves through SES", async () => {
    const credential = await resolveSendCredential("fallon@acutezoneco.com", CALLER);
    expect(credential).toBeNull();
    const sent = await sendFromMailbox(credential, message);
    expect(sent.transport).toBe("ses");
    expect(sendThroughSes).toHaveBeenCalledWith("fallon@acutezoneco.com", message);
  });

  it("no login + no SES pipe = the credential error, as before (never sent from elsewhere)", async () => {
    await expect(resolveSendCredential("kevin@growthagency.ch", CALLER)).rejects.toBeInstanceOf(MailboxCredentialError);
  });

  it("SES switched off: a send-only address has no way out, loudly", async () => {
    process.env[SES_KILL_SWITCH_ENV] = "true";
    await expect(resolveSendCredential("fallon@acutezoneco.com", CALLER)).rejects.toBeInstanceOf(MailboxCredentialError);
    await expect(sendFromMailbox(null, message)).rejects.toBeInstanceOf(MailboxCredentialError);
    expect(await loadSendOnlyCandidates()).toEqual([]);
  });

  it("the send-only set = present accounts on live SES domains", async () => {
    execute.mockResolvedValueOnce({ rows: [{ email: "fallon@acutezoneco.com" }] });
    expect(await loadSendOnlyCandidates()).toEqual(["fallon@acutezoneco.com"]);
    const q = JSON.stringify(execute.mock.calls[0]![0]);
    expect(q).toContain("live_since IS NOT NULL");
    expect(q).toContain("absent_since IS NULL");
  });
});
