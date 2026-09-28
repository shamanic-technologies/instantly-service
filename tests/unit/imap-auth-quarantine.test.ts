import { describe, it, expect, beforeEach, vi } from "vitest";

import {
  AUTH_QUARANTINE_RETRY_MS,
  ImapAuthQuarantinedError,
  assertNotAuthQuarantined,
  recordImapLoginOutcome,
  resetImapAuthQuarantine,
} from "../../src/lib/self-send/imap-auth-quarantine";
import { connectImapClient, createImapClient } from "../../src/lib/self-send/imap-client";

// The exact shape ImapFlow rejects with on `NO [ALERT] Invalid credentials`,
// captured from bailey@fuseconnectio.com on 2026-09-28.
const authError = () =>
  Object.assign(new Error("Command failed"), {
    authenticationFailed: true,
    responseText: "Invalid credentials (Failure)",
    serverResponseCode: "ALERT",
  });

const LOGIN = "bailey@fuseconnectio.com";
const T0 = Date.parse("2026-09-28T06:00:00Z");

beforeEach(() => resetImapAuthQuarantine());

describe("imap auth quarantine", () => {
  it("lets an unknown login through", () => {
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", T0)).not.toThrow();
  });

  it("refuses the same password after Google rejected it, without a login", () => {
    recordImapLoginOutcome(LOGIN, "pw", authError(), T0);
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", T0 + 60_000)).toThrow(
      ImapAuthQuarantinedError,
    );
  });

  it("tries a CHANGED password immediately (a reissued app password recovers at once)", () => {
    recordImapLoginOutcome(LOGIN, "old", authError(), T0);
    expect(() => assertNotAuthQuarantined(LOGIN, "new", T0 + 60_000)).not.toThrow();
  });

  it("allows exactly one retry per window, then holds again", () => {
    recordImapLoginOutcome(LOGIN, "pw", authError(), T0);
    const due = T0 + AUTH_QUARANTINE_RETRY_MS;
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", due)).not.toThrow();
    // A concurrent opener in the same instant does not get a second login.
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", due + 1)).toThrow(ImapAuthQuarantinedError);
  });

  it("keeps the original `since` across failed retries", () => {
    recordImapLoginOutcome(LOGIN, "pw", authError(), T0);
    recordImapLoginOutcome(LOGIN, "pw", authError(), T0 + AUTH_QUARANTINE_RETRY_MS);
    try {
      assertNotAuthQuarantined(LOGIN, "pw", T0 + AUTH_QUARANTINE_RETRY_MS + 1);
      throw new Error("expected quarantine");
    } catch (error) {
      expect((error as ImapAuthQuarantinedError).since.getTime()).toBe(T0);
    }
  });

  it("never quarantines on a timeout or network error — those say nothing about the password", () => {
    recordImapLoginOutcome(LOGIN, "pw", Object.assign(new Error("Socket timeout"), { code: "ETIMEOUT" }), T0);
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", T0 + 1)).not.toThrow();
  });

  it("a successful login clears the hold", () => {
    recordImapLoginOutcome(LOGIN, "pw", authError(), T0);
    recordImapLoginOutcome(LOGIN, "pw", null, T0 + AUTH_QUARANTINE_RETRY_MS);
    expect(() => assertNotAuthQuarantined(LOGIN, "pw", T0 + AUTH_QUARANTINE_RETRY_MS + 1)).not.toThrow();
  });
});

describe("connectImapClient", () => {
  const client = () =>
    createImapClient(
      { host: "imap.gmail.com", port: 993, secure: true, auth: { user: LOGIN, pass: "pw" }, logger: false },
      LOGIN,
    );

  it("closes the client when the login is rejected (no orphan socket), rethrows, then refuses the next attempt without connecting", async () => {
    const c1 = client();
    vi.spyOn(c1, "connect").mockRejectedValue(authError());
    const close1 = vi.spyOn(c1, "close");
    await expect(connectImapClient(c1, LOGIN, "pw")).rejects.toMatchObject({ authenticationFailed: true });
    expect(close1).toHaveBeenCalled();

    const c2 = client();
    const connect2 = vi.spyOn(c2, "connect");
    await expect(connectImapClient(c2, LOGIN, "pw")).rejects.toBeInstanceOf(ImapAuthQuarantinedError);
    expect(connect2).not.toHaveBeenCalled();
  });

  it("connects normally for a healthy mailbox", async () => {
    const c = client();
    const connect = vi.spyOn(c, "connect").mockResolvedValue(undefined);
    await connectImapClient(c, "emily@fuseconnectio.com", "pw");
    expect(connect).toHaveBeenCalledTimes(1);
  });
});
