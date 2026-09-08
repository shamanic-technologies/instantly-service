import { describe, it, expect, vi, afterEach } from "vitest";

import { createImapClient } from "../../src/lib/self-send/imap-client";

// No mock: constructing an ImapFlow opens no connection (that is `.connect()`),
// so this exercises the real object — including the real EventEmitter semantics,
// which are the entire point of the module under test.

afterEach(() => {
  vi.restoreAllMocks();
});

const OPTIONS = {
  host: "imap.gmail.com",
  port: 993,
  secure: true,
  auth: { user: "amy@saviolabsco.com", pass: "secret" },
  logger: false,
} as const;

describe("createImapClient", () => {
  // ⚠️ This is the whole reason the module exists. ImapFlow emits `error`
  // asynchronously from a socket timer, outside any promise a caller awaits, so
  // a try/catch around the poll cannot catch it — and an unhandled `error` on an
  // EventEmitter terminates the Node process. It took this service down
  // repeatedly on 2026-09-06 (`code: 'ETIMEOUT'`, exit 0, Docker restart),
  // killing three warmup polls mid-sweep and an unknown number of self-send
  // polls before the stack was read.
  it("survives an asynchronous socket error instead of terminating", () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const client = createImapClient({ ...OPTIONS }, "amy@saviolabsco.com");

    expect(() => {
      client.emit(
        "error",
        Object.assign(new Error("Socket timeout"), { code: "ETIMEOUT" }),
      );
    }).not.toThrow();
  });

  // The proof that the guard is what saves it: the same emission on a client
  // built WITHOUT the handler throws, which in production is process death.
  it("confirms the unguarded emission really is fatal", () => {
    const bare = createImapClient({ ...OPTIONS }, "amy@saviolabsco.com");
    bare.removeAllListeners("error");

    expect(() => bare.emit("error", new Error("Socket timeout"))).toThrow(
      "Socket timeout",
    );
  });

  it("names the failing mailbox, so this is not a silent swallow", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const client = createImapClient({ ...OPTIONS }, "kevin@ga.forum");

    client.emit("error", new Error("Socket timeout"));

    expect(warn).toHaveBeenCalledWith(expect.stringContaining("kevin@ga.forum"));
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("Socket timeout"));
  });

  it("handles a non-Error emission without throwing", () => {
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const client = createImapClient({ ...OPTIONS }, "x@y.com");
    expect(() => client.emit("error", "just a string")).not.toThrow();
  });
});
