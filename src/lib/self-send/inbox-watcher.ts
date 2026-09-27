/**
 * Reading a reply the moment it lands — one IMAP IDLE session per real mailbox.
 *
 * ⚠️ WHY THIS EXISTS. The self-send mailboxes used to be read ONLY as a side
 * effect of a sending tick, and a tick with nothing due skips the read (the
 * probe in `runDispatch`). Prod 2026-09-24: a prospect replied positively at
 * 20:20:46 UTC and the reply was read at 21:43:37 — twelve idle ticks in a row
 * had nothing to send, so nobody looked. A buyer who just wrote back is at their
 * inbox NOW, and the answer only starts once we have read them.
 *
 * ⚠️ PUSH, NOT A FASTER POLL. A full sweep of the fleet takes ~3 minutes and
 * reads ~80,000 message headers across ~250 logins; running it every two minutes
 * would be continuous, and continuous logins are how a provider throttles the
 * very mailboxes the volume ramp protects. IDLE inverts the cost: ONE long-lived
 * login per real mailbox (~123 of them), and the server tells us when something
 * arrives. The read that follows goes through that same session and fetches only
 * the new UIDs — no fresh login, no window re-read.
 *
 * THE BOUND THIS MODULE PROMISES:
 *   - watcher connected: a new message is ingested within seconds of the server
 *     announcing it (`INBOX_EVENT_DEBOUNCE_MS` + the read itself);
 *   - watcher down (provider hiccup, deploy, credential trouble): the refresh
 *     loop reads that mailbox directly every `INBOX_WATCH_REFRESH_MS` (5 min)
 *     until the watcher is back, and a reconnect always reads the window it
 *     missed. So the worst case is one refresh interval plus one read.
 * A server that does not offer IDLE is handled by imapflow itself, which falls
 * back to a NOOP poll at `IDLE_RESTART_MS`.
 *
 * ⚠️ WHAT IT DOES NOT CHANGE. The dispatch run still reads every mailbox before
 * it selects what to send (read-before-send); this is an additional, earlier
 * reader. Two readers of one message are safe by construction: bronze
 * `imap_messages_raw` is unique on `(account_email, message_id)` and a poll only
 * promotes a message ITS insert created, so the second reader skips it.
 *
 * ⚠️ Grain is the real MAILBOX login, never the sending address — a Gandi domain
 * is one mailbox behind several aliases, and one session per alias would open
 * several simultaneous sessions as the same SASL user. Each alias is still
 * correlated against its own sends (`pollMailboxGroup`).
 *
 * Kill switch: `SELF_SEND_INBOX_WATCH_ENABLED=false`, read at every refresh — it
 * closes every session. The dispatch-time read keeps working without it.
 */

import { createImapClient } from "./imap-client";
import {
  GMAIL_IMAP_PORT,
  loadMailboxLogins,
  loginFor,
  resolveMailboxCredential,
  type MailboxCredential,
} from "./mailbox-credentials";
import {
  loadSelfSendAccounts,
  pollMailboxGroup,
  type PollQuery,
  type PollSummary,
} from "./imap-poller";
import type { CallerInfo } from "../key-client";

// Same caller identity as the fleet sweep: it is the same read, triggered sooner.
const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/poll" };

/** How often the set of watched mailboxes is reconciled — and the fallback bound. */
export const INBOX_WATCH_REFRESH_MS = 5 * 60_000;

/**
 * Arrivals are coalesced for this long before reading, so a burst (a warmup
 * batch, a digest) costs one read, not one per message.
 */
export const INBOX_EVENT_DEBOUNCE_MS = 3_000;

/**
 * IDLE is broken and re-issued this often. Under imapflow's 5-minute socket
 * timeout (a silent IDLE would otherwise be torn down as dead), and far under
 * the ~29 minutes after which servers drop an IDLE unilaterally (RFC 2177).
 */
export const IDLE_RESTART_MS = 4 * 60_000;

/** Reconnect backoff: first retry, and the ceiling it doubles up to. */
export const RECONNECT_MIN_MS = 30_000;
export const RECONNECT_MAX_MS = 15 * 60_000;

/**
 * How many mailboxes may be CONNECTING or READING at once, fleet-wide. The same
 * 8 the fleet sweep uses: ~123 simultaneous logins at boot is the shape
 * providers throttle. Established IDLE sessions do not count against it.
 */
export const INBOX_WATCH_CONCURRENCY = 8;

/** How far back a (re)connect reads — the same window the fleet sweep reads. */
const CATCH_UP_WINDOW_MS = 3 * 24 * 60 * 60 * 1000;

export function isInboxWatchEnabled(): boolean {
  return process.env.SELF_SEND_INBOX_WATCH_ENABLED !== "false";
}

type ImapClient = ReturnType<typeof createImapClient>;

interface Watcher {
  login: string;
  accounts: string[];
  client: ImapClient | null;
  /** The credential the live session authenticated with — reused for its reads. */
  credential: MailboxCredential | null;
  connected: boolean;
  connecting: boolean;
  stopped: boolean;
  /** Next UID to read from; 0 until the catch-up read has established it. */
  nextUid: number;
  backoffMs: number;
  reconnectTimer: NodeJS.Timeout | null;
  debounceTimer: NodeJS.Timeout | null;
  reading: boolean;
  readAgain: boolean;
}

const watchers = new Map<string, Watcher>();
let refreshTimer: NodeJS.Timeout | null = null;
let refreshing = false;

// ── A tiny fleet-wide semaphore ────────────────────────────────────────────
let active = 0;
const waiting: Array<() => void> = [];
async function withSlot<T>(work: () => Promise<T>): Promise<T> {
  if (active >= INBOX_WATCH_CONCURRENCY) {
    await new Promise<void>((resolve) => waiting.push(resolve));
  }
  active += 1;
  try {
    return await work();
  } finally {
    active -= 1;
    waiting.shift()?.();
  }
}

/**
 * Group the self-send sending addresses by the real mailbox they authenticate
 * as. An address the login map does not know is its own mailbox — the same
 * reading every other consumer of that map takes. Pure.
 */
export function groupAccountsByLogin(
  accounts: readonly string[],
  logins: ReadonlyMap<string, string>,
): Map<string, string[]> {
  const groups = new Map<string, string[]>();
  for (const account of accounts) {
    const key = account.trim().toLowerCase();
    const login = logins.get(key) ?? key;
    const group = groups.get(login) ?? [];
    group.push(account);
    groups.set(login, group);
  }
  return groups;
}

/**
 * What to read next: the whole catch-up window until a read has told us where
 * the mailbox stands, then only what came after the last UID we saw. Pure.
 */
export function nextPollQuery(nextUid: number, asOf: Date): PollQuery {
  return nextUid > 0
    ? { uidFrom: nextUid }
    : { since: new Date(asOf.getTime() - CATCH_UP_WINDOW_MS) };
}

/** Double the backoff up to the ceiling. Pure. */
export function nextBackoff(current: number): number {
  return Math.min(Math.max(current, RECONNECT_MIN_MS) * 2, RECONNECT_MAX_MS);
}

function logRead(login: string, reason: string, summary: PollSummary): void {
  // Quiet unless the read found something that matters — most arrivals on a
  // real mailbox are ordinary mail, and a line per newsletter drowns the signal.
  if (
    summary.replies + summary.autoReplies + summary.bounces + summary.accountsFailed ===
    0
  ) {
    return;
  }
  console.log(
    `[instantly-service] inbox-watch: login=${login} reason=${reason} ${JSON.stringify(summary)}`,
  );
}

/**
 * Read one mailbox now. Coalesces: a read requested while one is running runs
 * once more when it finishes, never in parallel with it.
 */
async function readMailbox(watcher: Watcher, reason: string): Promise<void> {
  if (watcher.reading) {
    watcher.readAgain = true;
    return;
  }
  watcher.reading = true;
  try {
    do {
      watcher.readAgain = false;
      await withSlot(async () => {
        const shared = watcher.connected && watcher.client ? watcher.client : undefined;
        // A read through the live session reuses the credential it logged in
        // with: resolving per arrival would be a key-service read plus a vendor
        // pagination for every newsletter the mailbox receives.
        const credential =
          shared && watcher.credential
            ? watcher.credential
            : await resolveMailboxCredential(watcher.accounts[0], CALLER);
        const query = nextPollQuery(shared ? watcher.nextUid : 0, new Date());
        const { summary, maxUid } = await pollMailboxGroup(
          watcher.accounts,
          credential,
          query,
          shared,
        );
        // Only a read through the watched session advances its cursor — a
        // standalone read says nothing about where THAT session stands.
        if (shared && maxUid >= watcher.nextUid) watcher.nextUid = maxUid + 1;
        logRead(watcher.login, reason, summary);
      });
    } while (watcher.readAgain && !watcher.stopped);
  } catch (error) {
    console.error(
      `[instantly-service] inbox-watch: read of login=${watcher.login} failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  } finally {
    watcher.reading = false;
  }
}

function requestRead(watcher: Watcher, reason: string): void {
  if (watcher.stopped) return;
  if (watcher.debounceTimer) return;
  watcher.debounceTimer = setTimeout(() => {
    watcher.debounceTimer = null;
    void readMailbox(watcher, reason);
  }, INBOX_EVENT_DEBOUNCE_MS);
  watcher.debounceTimer.unref?.();
}

function scheduleReconnect(watcher: Watcher): void {
  if (watcher.stopped || watcher.reconnectTimer) return;
  const delay = watcher.backoffMs;
  watcher.backoffMs = nextBackoff(watcher.backoffMs);
  watcher.reconnectTimer = setTimeout(() => {
    watcher.reconnectTimer = null;
    void connect(watcher);
  }, delay);
  watcher.reconnectTimer.unref?.();
}

async function connect(watcher: Watcher): Promise<void> {
  if (watcher.stopped || watcher.connected || watcher.connecting) return;
  watcher.connecting = true;
  let client: ImapClient | null = null;
  try {
    await withSlot(async () => {
      // Re-resolved on every connect, so a rotated password is picked up by the
      // next reconnect rather than retried forever.
      const credential = await resolveMailboxCredential(watcher.accounts[0], CALLER);
      const session = createImapClient(
        {
          host: credential.imapHost,
          port: GMAIL_IMAP_PORT,
          secure: true,
          auth: { user: loginFor(credential), pass: credential.appPassword },
          logger: false,
          maxIdleTime: IDLE_RESTART_MS,
        },
        `inbox-watch:${watcher.login}`,
      );

      client = session;

      session.on("exists", () => requestRead(watcher, "exists"));
      session.on("close", () => {
        if (watcher.client !== session) return;
        watcher.client = null;
        watcher.connected = false;
        scheduleReconnect(watcher);
      });

      await session.connect();
      await session.mailboxOpen("INBOX");
      if (watcher.stopped) {
        await session.logout().catch(() => {});
        return;
      }
      watcher.client = session;
      watcher.credential = credential;
      watcher.connected = true;
      watcher.backoffMs = RECONNECT_MIN_MS;
      // Anything that arrived while nobody was watching (a deploy, an outage)
      // is read now, over the full window; that read also sets the UID cursor.
      watcher.nextUid = 0;
    });
  } catch (error) {
    console.warn(
      `[instantly-service] inbox-watch: connect login=${watcher.login} failed, retrying in ${Math.round(
        watcher.backoffMs / 1000,
      )}s: ${error instanceof Error ? error.message : String(error)}`,
    );
    watcher.connecting = false;
    // A session that connected but could not open the INBOX is still a login.
    const orphan = client as ImapClient | null;
    if (orphan && watcher.client !== orphan) void orphan.logout().catch(() => {});
    scheduleReconnect(watcher);
    return;
  }
  watcher.connecting = false;
  if (watcher.connected) void readMailbox(watcher, "connect");
}

function stopWatcher(watcher: Watcher): void {
  watcher.stopped = true;
  if (watcher.reconnectTimer) clearTimeout(watcher.reconnectTimer);
  if (watcher.debounceTimer) clearTimeout(watcher.debounceTimer);
  const client = watcher.client;
  watcher.client = null;
  watcher.connected = false;
  if (client) void client.logout().catch(() => {});
}

/**
 * Reconcile the watched set with the fleet, and read directly any mailbox whose
 * watcher is not up — the fallback that keeps the bound when push is down.
 */
export async function refreshInboxWatchers(): Promise<{
  watched: number;
  connected: number;
  fallbackReads: number;
}> {
  if (!isInboxWatchEnabled()) {
    for (const watcher of watchers.values()) stopWatcher(watcher);
    watchers.clear();
    return { watched: 0, connected: 0, fallbackReads: 0 };
  }

  const [accounts, logins] = await Promise.all([
    loadSelfSendAccounts(),
    loadMailboxLogins(CALLER),
  ]);
  const groups = groupAccountsByLogin(accounts, logins);

  for (const [login, watcher] of watchers) {
    if (!groups.has(login)) {
      stopWatcher(watcher);
      watchers.delete(login);
    }
  }

  let fallbackReads = 0;
  for (const [login, group] of groups) {
    const existing = watchers.get(login);
    if (existing) {
      existing.accounts = group;
      if (!existing.connected && !existing.connecting) {
        fallbackReads += 1;
        void readMailbox(existing, "fallback");
      }
      continue;
    }
    const watcher: Watcher = {
      login,
      accounts: group,
      client: null,
      credential: null,
      connected: false,
      connecting: false,
      stopped: false,
      nextUid: 0,
      backoffMs: RECONNECT_MIN_MS,
      reconnectTimer: null,
      debounceTimer: null,
      reading: false,
      readAgain: false,
    };
    watchers.set(login, watcher);
    void connect(watcher);
  }

  const connected = [...watchers.values()].filter((w) => w.connected).length;
  return { watched: watchers.size, connected, fallbackReads };
}

async function tick(): Promise<void> {
  if (refreshing) return;
  refreshing = true;
  try {
    const state = await refreshInboxWatchers();
    console.log(`[instantly-service] inbox-watch: ${JSON.stringify(state)}`);
  } catch (error) {
    console.error(
      `[instantly-service] inbox-watch: refresh failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  } finally {
    refreshing = false;
  }
}

/** Armed after `listen`. Idempotent. */
export function startInboxWatcher(): void {
  if (refreshTimer) return;
  refreshTimer = setInterval(() => void tick(), INBOX_WATCH_REFRESH_MS);
  refreshTimer.unref?.();
  void tick();
  console.log(
    `[instantly-service] inbox watcher armed (IDLE per mailbox, refresh every ${INBOX_WATCH_REFRESH_MS}ms)`,
  );
}

/** Tests and shutdown only. */
export function stopInboxWatcher(): void {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  for (const watcher of watchers.values()) stopWatcher(watcher);
  watchers.clear();
}
