/**
 * In-process drive of the mailbox-messages feed (lib/mailbox-messages).
 *
 * The DB triggers mark every bronze row written or changed; this drains them
 * into versions. First pass ~60 s after boot (deploys recreate the container
 * more often than any long interval fires), then every minute, a mutex so a
 * slow tick never stacks. All state is in the DB: a deploy mid-tick loses
 * nothing (a drain is one transaction).
 *
 * On the very first boot of the feed (nothing versioned, nothing waiting) the
 * whole history is marked once, so the CRM reads every past message too.
 */
import { errorMessage } from "./db-error.js";
import { drainMailboxMessages, mailboxFeedIsEmpty, markMailboxHistory } from "./mailbox-messages";

export const MAILBOX_MESSAGES_INTERVAL_MS = 60 * 1000;
const FIRST_TICK_MS = 60 * 1000;
/** Batches of 500 per tick: the history backfill drains in minutes, a normal tick in one batch. */
const BATCHES_PER_TICK = 20;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

export async function runMailboxMessagesTick(): Promise<void> {
  if (inFlight) return;
  inFlight = true;
  try {
    if (await mailboxFeedIsEmpty()) {
      const marked = await markMailboxHistory(null);
      console.log(`[instantly-service] mailbox-messages: first boot, ${marked} historical rows marked`);
    }
    let drained = 0;
    let versioned = 0;
    for (let i = 0; i < BATCHES_PER_TICK; i += 1) {
      const d = await drainMailboxMessages(500);
      drained += d.drained;
      versioned += d.versioned;
      if (d.drained < 500) break;
    }
    if (drained > 0) {
      console.log(`[instantly-service] mailbox-messages: drained=${drained} versioned=${versioned}`);
    }
  } catch (error: unknown) {
    console.error(`[instantly-service] mailbox-messages tick failed: ${errorMessage(error)}`);
  } finally {
    inFlight = false;
  }
}

export function startMailboxMessagesWorker(): void {
  if (timer) return;
  setTimeout(() => void runMailboxMessagesTick(), FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void runMailboxMessagesTick(), MAILBOX_MESSAGES_INTERVAL_MS);
  timer.unref?.();
  console.log(`[instantly-service] mailbox-messages worker armed, every ${MAILBOX_MESSAGES_INTERVAL_MS}ms`);
}
