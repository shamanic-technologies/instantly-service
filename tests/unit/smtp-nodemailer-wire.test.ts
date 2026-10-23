/**
 * Every SMTP send path, driven through the REAL nodemailer client against an
 * in-process SMTP server.
 *
 * The other send tests mock the transport, so a nodemailer major bump (9 -> 10:
 * TypeScript rewrite, new addressparser, hardened SMTP connection) would pass
 * them unseen. Here only the network target moves: host/port point at a local
 * fake server and STARTTLS is skipped (the fake cannot present a trusted
 * certificate). Everything else, the options we build, the message nodemailer
 * composes, the reply codes it surfaces on a refusal, is the real thing.
 */
import net from "node:net";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

const fake = vi.hoisted(() => ({ port: 0 }));

vi.mock("nodemailer", async (importOriginal) => {
  const real = (await importOriginal()) as { default: { createTransport: (...a: unknown[]) => unknown } };
  const createTransport = (options: Record<string, unknown>) =>
    real.default.createTransport({
      ...options,
      host: "127.0.0.1",
      port: fake.port,
      requireTLS: false,
      ignoreTLS: true,
    });
  return { ...real, default: { ...real.default, createTransport } };
});

vi.mock("../../src/lib/self-send/mailbox-credentials", async (importOriginal) => {
  const real = (await importOriginal()) as Record<string, unknown>;
  return {
    ...real,
    resolveMailboxCredential: vi.fn(async (address: string) => ({
      address,
      appPassword: "app-pass",
      smtpHost: "smtp.example.test",
      imapHost: "imap.example.test",
      authUser: "login@example.test",
    })),
  };
});

import { dispatchMessage, SmtpDispatchError } from "../../src/lib/self-send/smtp";
import { smtpSender } from "../../src/lib/blocklist-removal";

interface Session {
  auth: string[];
  mailFrom: string[];
  rcptTo: string[];
  data: string;
}

let server: net.Server;
let sessions: Session[] = [];
/** Recipients the fake refuses at RCPT with a 550. */
let rejectRcpt = new Set<string>();
/** When set, MAIL FROM is answered with this reply instead of 250. */
let mailFromReply: string | null = null;

function startFakeSmtp(): Promise<number> {
  server = net.createServer((socket) => {
    const session: Session = { auth: [], mailFrom: [], rcptTo: [], data: "" };
    sessions.push(session);
    let buffer = "";
    let inData = false;
    let awaitingAuth = false;
    const reply = (line: string) => socket.write(`${line}\r\n`);
    reply("220 fake.test ESMTP");

    socket.on("data", (chunk) => {
      buffer += chunk.toString("utf8");
      for (;;) {
        if (inData) {
          const end = buffer.indexOf("\r\n.\r\n");
          if (end === -1) return;
          session.data += buffer.slice(0, end);
          buffer = buffer.slice(end + 5);
          inData = false;
          reply("250 2.0.0 OK queued as FAKE123");
          continue;
        }
        const nl = buffer.indexOf("\r\n");
        if (nl === -1) return;
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 2);
        const upper = line.toUpperCase();
        if (awaitingAuth) {
          session.auth.push(line);
          awaitingAuth = false;
          reply("235 2.7.0 Authentication successful");
        } else if (upper.startsWith("EHLO")) {
          reply("250-fake.test");
          reply("250-AUTH PLAIN LOGIN");
          reply("250 8BITMIME");
        } else if (upper.startsWith("AUTH PLAIN")) {
          const arg = line.slice("AUTH PLAIN".length).trim();
          if (arg) {
            session.auth.push(arg);
            reply("235 2.7.0 Authentication successful");
          } else {
            awaitingAuth = true;
            reply("334 ");
          }
        } else if (upper.startsWith("MAIL FROM")) {
          session.mailFrom.push(line);
          reply(mailFromReply ?? "250 2.1.0 OK");
        } else if (upper.startsWith("RCPT TO")) {
          const address = /<([^>]*)>/.exec(line)?.[1]?.toLowerCase() ?? "";
          session.rcptTo.push(address);
          reply(rejectRcpt.has(address) ? "550 5.1.1 No such user" : "250 2.1.5 OK");
        } else if (upper === "DATA") {
          inData = true;
          reply("354 Go ahead");
        } else if (upper === "RSET" || upper === "NOOP") {
          reply("250 OK");
        } else if (upper === "QUIT") {
          reply("221 Bye");
          socket.end();
        } else {
          reply("502 Command not implemented");
        }
      }
    });
    socket.on("error", () => undefined);
  });
  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve((server.address() as net.AddressInfo).port));
  });
}

const credential = {
  address: "kevin@sender.test",
  appPassword: "app-pass",
  smtpHost: "smtp.sender.test",
  imapHost: "imap.sender.test",
  authUser: "mailbox@sender.test",
};

function decodePlain(arg: string): string[] {
  return Buffer.from(arg, "base64").toString("utf8").split("\u0000");
}

beforeAll(async () => {
  fake.port = await startFakeSmtp();
});

afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => {
  sessions = [];
  rejectRcpt = new Set();
  mailFromReply = null;
});

describe("dispatchMessage over a real SMTP conversation", () => {
  it("authenticates with the alias-aware login and delivers the composed message", async () => {
    const result = await dispatchMessage(credential, {
      from: "Kevin Lourd <kevin@sender.test>",
      to: "prospect@lead.test",
      cc: "colleague@lead.test",
      bcc: "observer@agency.test",
      subject: "Quick question",
      html: "<p>Hello there</p>",
      headers: { "List-Unsubscribe": "<https://unsub.test/x>", "X-Step": "2" },
      inReplyTo: "<prev@sender.test>",
      references: ["<first@sender.test>", "<prev@sender.test>"],
      attachments: [{ filename: "offer.pdf", content: Buffer.from("%PDF-1.4 test"), contentType: "application/pdf" }],
    });

    expect(sessions).toHaveLength(1);
    const [session] = sessions;
    expect(decodePlain(session.auth[0]!)).toEqual(["", "mailbox@sender.test", "app-pass"]);
    expect(session.mailFrom[0]).toContain("<kevin@sender.test>");
    expect(session.rcptTo.sort()).toEqual(["colleague@lead.test", "observer@agency.test", "prospect@lead.test"]);

    const raw = session.data;
    expect(raw).toMatch(/^Subject: Quick question$/m);
    expect(raw).toMatch(/^To: prospect@lead\.test$/m);
    expect(raw).toMatch(/^Cc: colleague@lead\.test$/m);
    expect(raw).not.toMatch(/^Bcc:/im);
    expect(raw).toMatch(/^In-Reply-To: <prev@sender\.test>$/m);
    expect(raw).toMatch(/^References: <first@sender\.test> <prev@sender\.test>$/m);
    expect(raw).toMatch(/^List-Unsubscribe: <https:\/\/unsub\.test\/x>$/m);
    expect(raw).toMatch(/^X-Step: 2$/m);
    expect(raw).toContain('filename=offer.pdf');
    expect(raw).toContain("Hello there");

    expect(result.response).toContain("250 2.0.0 OK queued as FAKE123");
    expect(result.accepted.map((a) => a.toLowerCase())).toContain("prospect@lead.test");
    expect(result.rejected).toEqual([]);
    expect(raw).toContain(`Message-ID: ${result.messageId}`);
  });

  it("a 550 on the prospect is a PERMANENT failure", async () => {
    rejectRcpt.add("dead@lead.test");
    const error = await dispatchMessage(credential, {
      from: "kevin@sender.test",
      to: "dead@lead.test",
      subject: "s",
      html: "<p>x</p>",
      headers: {},
    }).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(SmtpDispatchError);
    expect((error as SmtpDispatchError).kind).toBe("permanent");
  });

  it("a 4xx from the server is TRANSIENT and keeps its reply code", async () => {
    mailFromReply = "451 4.7.1 Try again later";
    const error = await dispatchMessage(credential, {
      from: "kevin@sender.test",
      to: "prospect@lead.test",
      subject: "s",
      html: "<p>x</p>",
      headers: {},
    }).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(SmtpDispatchError);
    expect((error as SmtpDispatchError).kind).toBe("transient");
    expect((error as SmtpDispatchError).responseCode).toBe(451);
  });
});

describe("blocklist removal smtpSender over a real SMTP conversation", () => {
  it("sends the plain-text request and returns the server's 250", async () => {
    const answer = await smtpSender("ops@sender.test")({
      from: "ops@sender.test",
      to: "removals@surbl.test",
      subject: "Removal request",
      text: "Please remove example.com",
    });

    expect(answer.code).toBe(250);
    expect(answer.response).toContain("queued as FAKE123");
    const [session] = sessions;
    expect(decodePlain(session!.auth[0]!)).toEqual(["", "login@example.test", "app-pass"]);
    expect(session!.rcptTo).toEqual(["removals@surbl.test"]);
    expect(session!.data).toContain("Please remove example.com");
  });
});
