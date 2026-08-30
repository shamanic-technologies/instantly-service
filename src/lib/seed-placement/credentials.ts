/**
 * Batch credential loading for the seed harness.
 *
 * The credential resolution itself lives in `self-send/mailbox-credentials` —
 * manual list first, then Primeforge — and is shared with the sender so that a
 * mailbox we can MEASURE is also a mailbox we can SEND from. This module only
 * changes the SHAPE of that resolution, for two reasons the sender does not
 * have:
 *
 *   - It resolves a whole pool at once, so it reads each source ONCE and hands
 *     back a pure lookup. The per-mailbox resolver re-reads the key and
 *     re-paginates the vendor's mailbox list on every call, which across a
 *     ~200-account pool fanning out to ~10 receivers would mean thousands of
 *     vendor paginations per run.
 *   - An unresolvable mailbox is SKIPPED here rather than fatal: the testable
 *     pool is the whole fleet and most of it has no credential, so "unmeasured"
 *     is the honest result and is already its state. On the send path the same
 *     fact must abort the send instead.
 */

import { type CallerInfo } from "../key-client";
import {
  MailboxCredentialError,
  loadManualCredentials,
  selectMailboxCredential,
  selectManualCredential,
  type MailboxCredential,
} from "../self-send/mailbox-credentials";
import { resolvePlatformKey } from "../key-client";
import { listPrimeforgeRawMailboxes } from "../providers/primeforge-client";

// Re-exported so the manual-credential vocabulary keeps ONE home while callers
// (and their tests) can reach it from either module.
export {
  MANUAL_CREDENTIALS_PROVIDER,
  loadManualCredentials,
  parseManualCredentials,
  selectManualCredential,
  type ManualMailboxEntry,
} from "../self-send/mailbox-credentials";

/**
 * A resolver over credentials already in memory.
 *
 * Returns null for a mailbox we hold no credential for — which is a NORMAL,
 * expected outcome here, not a failure: the testable pool is the whole fleet
 * (~200 accounts, most of them on the legacy Gandi/Mailforge infrastructure)
 * while credentials exist only for Primeforge plus whatever the manual key
 * lists. The caller SKIPS an unresolvable mailbox; it stays unmeasured, which is
 * the honest result and is already its state.
 */
export type SeedCredentialResolver = (email: string) => MailboxCredential | null;

/**
 * Load every credential source ONCE and hand back a pure resolver.
 *
 * ⚠️ This shape is load-bearing, not tidiness. `resolveMailboxCredential`
 * re-reads the key AND re-paginates the vendor's whole mailbox list on every
 * call, and it THROWS for a mailbox the vendor does not host. Calling it
 * per-seed against a ~200-account pool that fans out to ~10 receivers would
 * mean thousands of vendor paginations per run, the overwhelming majority of
 * them for mailboxes that were never going to resolve. Two reads up front, then
 * pure lookups.
 */
export async function loadSeedCredentialResolver(
  caller: CallerInfo,
): Promise<SeedCredentialResolver> {
  const manual = await loadManualCredentials(caller);
  const primeforgeKey = await resolvePlatformKey("primeforge", caller);
  const mailboxes = await listPrimeforgeRawMailboxes(primeforgeKey);

  return (email: string): MailboxCredential | null => {
    // Manual wins, so an operator can override a vendor-reported password
    // without waiting on the vendor — and so a non-Primeforge mailbox
    // (Gandi, Mailforge) resolves at all.
    const override = selectManualCredential(email, manual);
    if (override) return override;

    try {
      return selectMailboxCredential(email, mailboxes);
    } catch (error) {
      // `selectMailboxCredential` throws for "this vendor does not host that
      // mailbox", which here is the expected majority case. Anything else is a
      // real problem and must not be turned into a skip.
      if (error instanceof MailboxCredentialError) return null;
      throw error;
    }
  };
}
