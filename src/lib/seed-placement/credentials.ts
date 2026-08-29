/**
 * Mailbox credentials for the seed harness.
 *
 * `resolveMailboxCredential` (self-send) reads app passwords out of Primeforge's
 * REST API, which covers every mailbox the self-send transport dispatches. The
 * seed harness has to reach further: it measures the whole testable pool, and
 * that pool contains mailboxes on vendors whose API does NOT hand back an
 * existing mailbox's password — Gandi and Mailforge both. `klourd@pressbeat.ai`
 * is the live case: it is `in_production`, it is the head of the sending
 * fill order, and without a manual credential it becomes unmeasurable the moment
 * the paid Instantly test goes away.
 *
 * So a MANUAL layer sits in front of Primeforge: a platform key in key-service
 * holding a JSON array of hand-entered mailbox credentials. Hosts live in the
 * payload rather than being hardcoded, because the whole point of the layer is
 * that these mailboxes are NOT on Google.
 *
 * ⚠️ The manual key is OPTIONAL and its absence is a legitimate state, not an
 * error — most deployments have no non-Primeforge mailbox to measure. Only a
 * key-service 404 yields "none configured"; every other failure (401, 5xx, a
 * malformed payload) throws, because those mean we could not READ the
 * credentials, which is a different thing from there being none.
 */

import { KeyServiceError, resolvePlatformKey, type CallerInfo } from "../key-client";
import {
  MailboxCredentialError,
  selectMailboxCredential,
  type MailboxCredential,
} from "../self-send/mailbox-credentials";
import { listPrimeforgeRawMailboxes } from "../providers/primeforge-client";

/** key-service platform provider holding the hand-entered mailbox credentials. */
export const MANUAL_CREDENTIALS_PROVIDER = "mailbox-credentials";

export interface ManualMailboxEntry {
  address: string;
  appPassword: string;
  smtpHost: string;
  imapHost: string;
  /**
   * SMTP/IMAP login when it differs from the address (the Gandi alias case).
   * Optional — omitted means the address is the login.
   */
  authUser?: string;
}

/**
 * Parse the manual-credential payload.
 *
 * Fail loud on a shape we do not recognise: a typo in the stored JSON must not
 * silently degrade to "no manual mailboxes", which would look identical to the
 * key being absent and would send the caller down the Primeforge path for a
 * mailbox Primeforge has never heard of.
 */
export function parseManualCredentials(raw: string): ManualMailboxEntry[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new MailboxCredentialError(
      `${MANUAL_CREDENTIALS_PROVIDER} platform key is not valid JSON: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  if (!Array.isArray(parsed)) {
    throw new MailboxCredentialError(
      `${MANUAL_CREDENTIALS_PROVIDER} platform key must be a JSON array of mailbox credentials`,
    );
  }

  return parsed.map((entry, index) => {
    const e = entry as Partial<ManualMailboxEntry> | null;
    const address = String(e?.address ?? "").trim().toLowerCase();
    const appPassword = String(e?.appPassword ?? "").replace(/\s/g, "");
    const smtpHost = String(e?.smtpHost ?? "").trim();
    const imapHost = String(e?.imapHost ?? "").trim();
    const authUser = String(e?.authUser ?? "").trim().toLowerCase();

    if (!address || !appPassword || !smtpHost || !imapHost) {
      throw new MailboxCredentialError(
        `${MANUAL_CREDENTIALS_PROVIDER}[${index}] must carry address, appPassword, smtpHost and imapHost`,
      );
    }

    return {
      address,
      appPassword,
      smtpHost,
      imapHost,
      ...(authUser && authUser !== address ? { authUser } : {}),
    };
  });
}

/** Pure lookup of one address in the manual list. Null when it is not listed. */
export function selectManualCredential(
  email: string,
  entries: readonly ManualMailboxEntry[],
): MailboxCredential | null {
  const wanted = email.trim().toLowerCase();
  const entry = entries.find((e) => e.address === wanted);
  if (!entry) return null;

  return {
    address: entry.address,
    appPassword: entry.appPassword,
    smtpHost: entry.smtpHost,
    imapHost: entry.imapHost,
    ...(entry.authUser ? { authUser: entry.authUser } : {}),
  };
}

/**
 * Read the manual credential list. An absent key means "none configured".
 *
 * The 404 carve-out is deliberately narrow — see the module note. Anything else
 * propagates.
 */
export async function loadManualCredentials(
  caller: CallerInfo,
): Promise<ManualMailboxEntry[]> {
  try {
    const raw = await resolvePlatformKey(MANUAL_CREDENTIALS_PROVIDER, caller);
    return parseManualCredentials(raw);
  } catch (error) {
    if (error instanceof KeyServiceError && error.statusCode === 404) return [];
    throw error;
  }
}

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
