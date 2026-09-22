/**
 * The `step` a one-to-one reply is recorded under in `smtp_dispatch_raw`.
 *
 * Sequence steps are 1-based everywhere in this repo (`sequence_costs.step`,
 * `sequence_steps.step`), so 0 is unambiguously "not a step of the sequence".
 * The row still has to exist: it is what lets the IMAP poller correlate the
 * prospect's answer to OUR answer back to this lead, and what keeps the
 * forwarded thread complete. It is deliberately NOT a `sequence_steps` row and
 * carries no hold — a reply is not a scheduled step and must never enter the
 * dispatch queue.
 *
 * It lives in its own module, rather than beside the code that writes it, so
 * that the human-takeover gate can read it without importing `reply-to-lead` —
 * which imports the gate, and a cycle over a module-scope const is a temporal
 * dead zone waiting to happen. `reply-to-lead` re-exports it, so every existing
 * importer is unchanged.
 */
export const MANUAL_REPLY_STEP = 0;
