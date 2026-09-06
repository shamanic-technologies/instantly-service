/**
 * Opening an IMAP connection without letting it kill the process.
 *
 * ⚠️ `ImapFlow` EMITS AN `'error'` EVENT ASYNCHRONOUSLY, AND AN UNHANDLED
 * `'error'` ON AN EventEmitter IS AN UNCAUGHT EXCEPTION THAT TERMINATES NODE.
 * A `try/catch` around the poll cannot catch it: the event is emitted from a
 * socket timer, outside any promise chain the caller is awaiting.
 *
 * That is not theoretical — it is what took this service down repeatedly on
 * 2026-09-06:
 *
 *     Error: Socket timeout
 *         at TLSSocket._socketTimeout (imapflow/lib/imap-flow.js:1467:29)
 *     Emitted 'error' event on ImapFlow instance at:
 *         at ImapFlow.emitError (imapflow/lib/imap-flow.js:663:14)
 *       code: 'ETIMEOUT'
 *     Node.js v20.20.2
 *
 * The process exits 0 and Docker restarts it, so nothing looks broken: no OOM,
 * no crash loop, `/health` answers again within seconds, and the only tell is a
 * `RestartCount` that keeps climbing and a long-running sweep that never
 * reports `done`. Three warmup polls and an unknown number of self-send polls
 * were killed mid-run this way before the stack was read.
 *
 * A single unreachable mailbox out of ~200 is ordinary — mail servers time out.
 * It must cost that mailbox's turn, never the whole sweep and certainly never
 * the service.
 *
 * The handler deliberately only LOGS. Every caller already wraps its per-mailbox
 * work in a try/catch that records the failure and moves on, so the awaited path
 * still fails loudly in the caller's own summary; this exists purely to stop the
 * asynchronous emit from being fatal.
 */

import { ImapFlow, type ImapFlowOptions } from "imapflow";

/**
 * An `ImapFlow` whose asynchronous `'error'` events cannot terminate the
 * process. Use this instead of `new ImapFlow(...)` everywhere.
 */
export function createImapClient(
  options: ImapFlowOptions,
  label: string,
): ImapFlow {
  const client = new ImapFlow(options);

  client.on("error", (error: unknown) => {
    console.warn(
      `[imap] ${label}: connection error (non-fatal): ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  });

  return client;
}
