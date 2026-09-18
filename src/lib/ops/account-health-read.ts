/**
 * The account-health read, as a function.
 *
 * `GET /internal/audit/account-health` used to assemble its rows inline; the
 * ops `addresses` read serves the SAME rows plus projections, and two copies
 * of the assembly would drift about the same account. So the assembly lives
 * here and both routes call it. Byte-identical output for the audit route.
 */

import { listAccounts, type Account } from "../instantly-client";
import { resolvePlatformInstantlyApiKey, type CallerInfo } from "../key-client";
import { buildAccountHealth, type AccountHealth, type InboxPlacement } from "../account-health";
import {
  fetchLifecycleByEmail,
  fetchInProductionAccounts,
  type LifecycleView,
} from "../account-lifecycle-sync";
import { accountFillOrder } from "../send-lead";
import {
  fetchSentTodayByAccount,
  fetchSentYesterdayByAccount,
  fetchQueueSizeByAccount,
  fetchQueueBreakdownByAccount,
} from "../account-sending-stats";
import { fetchLatestPlacementByAccount } from "../placement-sync";
import { fetchRecentDailyVolume, sustainedFor, type DailyVolume } from "../recent-send-volume";

export interface AccountHealthRead {
  asOf: Date;
  accounts: AccountHealth[];
  /** The raw Instantly account list the rows were built from. */
  rawAccounts: Account[];
  placementByEmail: Map<string, InboxPlacement>;
  lifecycleByEmail: Map<string, LifecycleView>;
  recentVolume: DailyVolume;
}

export async function loadAccountHealth(caller: CallerInfo): Promise<AccountHealthRead> {
  const asOf = new Date();
  const apiKey = await resolvePlatformInstantlyApiKey(caller);

  // Account list (Instantly) + latest placement, sent-today, and queue-size
  // per account (our silver + cost holds) run independently — parallelize.
  // Placement/sent/queue are best-effort per contract (null/0 when absent); a
  // live account list is required (fail loud).
  const [
    accounts,
    placementByEmail,
    sentTodayByEmail,
    sentYesterdayByEmail,
    queueSizeByEmail,
    queueBreakdownByEmail,
    lifecycleByEmail,
    pool,
    recentVolume,
  ] = await Promise.all([
    listAccounts(apiKey),
    fetchLatestPlacementByAccount(),
    fetchSentTodayByAccount(),
    fetchSentYesterdayByAccount(),
    fetchQueueSizeByAccount(),
    fetchQueueBreakdownByAccount(asOf),
    fetchLifecycleByEmail(),
    // The selector's OWN pool read, slug-less — i.e. exactly the set an
    // unreserved send draws from. Ranking a set we assembled here instead
    // would be a second implementation of the selection gate, free to drift
    // from the one that actually picks the mailbox.
    fetchInProductionAccounts(null),
    // The SAME volume map the selector caps against, so the table cannot
    // report a cap the selector disagrees with for the same mailbox.
    fetchRecentDailyVolume(),
  ]);

  // Position in the fill order, 1-based. `accountFillOrder` is the selector's
  // own comparator, so rank 1 is by construction the mailbox a new sequence is
  // offered first. An account outside the pool is simply absent from the map
  // and reports a null rank — never a fabricated position.
  const fillRankByEmail = new Map<string, number>(
    accountFillOrder(pool).map((a, i) => [a.email, i + 1]),
  );

  const rows = buildAccountHealth(
    accounts,
    placementByEmail,
    sentTodayByEmail,
    queueSizeByEmail,
    lifecycleByEmail,
    sentYesterdayByEmail,
    queueBreakdownByEmail,
    {
      fillRankByEmail,
      recentSustainedByEmail: new Map(
        accounts.filter((a) => a.email).map((a) => [a.email, sustainedFor(recentVolume, a.email)]),
      ),
      asOf,
    },
  );

  return { asOf, accounts: rows, rawAccounts: accounts, placementByEmail, lifecycleByEmail, recentVolume };
}
