/**
 * Pure decision helpers for the RECURRING finished-contact deletion (reconcile
 * path). Decision logic only — the Instantly `DELETE /leads` + local status
 * write live in `reconcile.ts` (`deleteFinishedContact`). See CLAUDE.md
 * "Finished-contact cleanup — reclaim quota".
 *
 * Rule (locked, option A): a campaign is finished iff its Instantly status is
 * PAUSED (2) or COMPLETED (3). ACTIVE (1) is never deleted. No pause grace
 * period — a pause means "done" (e.g. operator paused after the prospect
 * replied off-Instantly). Deletion is gated by an env kill-switch, default OFF.
 */

/** Instantly campaign status codes meaning "finished" → contact deletable. */
export const FINISHED_INSTANTLY_STATUSES = new Set<number>([2, 3]); // 2 = paused, 3 = completed

/** Local `instantly_campaigns.status` values marking a row terminal (lead deleted). */
export const LOCAL_TERMINAL_STATUSES = new Set<string>(["paused", "completed"]);

/**
 * Env kill-switch. Deletion runs ONLY when `DELETE_FINISHED_CONTACTS_ENABLED`
 * is exactly "true". Any other value — including unset — means OFF, so reconcile
 * behaves exactly as before (read-only). Fail-safe: a typo never enables deletes.
 */
export function isDeleteFinishedEnabled(): boolean {
  return process.env.DELETE_FINISHED_CONTACTS_ENABLED === "true";
}

/** Coerce Instantly's campaign status (number or numeric string) to a number, or null. */
export function parseInstantlyStatus(raw: unknown): number | null {
  if (raw === null || raw === undefined) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/** True iff the (already-parsed) Instantly status is paused (2) or completed (3). */
export function isFinishedInstantlyStatus(status: number | null): boolean {
  return status !== null && FINISHED_INSTANTLY_STATUSES.has(status);
}

/** Map a finished Instantly status to the local terminal status we persist. */
export function localTerminalStatus(instantlyStatus: number): "completed" | "paused" {
  return instantlyStatus === 3 ? "completed" : "paused";
}

/** True if a local row's status is already terminal (don't re-poll / re-delete). */
export function isLocallyTerminal(status: string): boolean {
  return LOCAL_TERMINAL_STATUSES.has(status);
}

/**
 * True if an Instantly error message is a 404 (lead already deleted). Tolerable
 * during deletion — the op is idempotent. `instantlyRequest` formats errors as
 * `instantly-api DELETE /leads failed: 404 - <body>`.
 */
export function isLeadAlreadyGone(message: string): boolean {
  return /failed: 404\b/.test(message);
}
