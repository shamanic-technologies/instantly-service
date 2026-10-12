// The status read's statement that `POST /orgs/send` would REFUSE a person.
//
// Why it exists (RECONTACT-LOOP-1011, Perky 2026-10-11): the send route refuses
// with 409 `recent_brand_contact`, `lead_opted_out` or `lead_not_a_prospect`,
// but none of that showed in `POST /orgs/status`. A refused person read
// "not contacted, not queued, not finished" — exactly like a stranded one — so
// lead-service's paid-pool retry handed them back every run, every send failed,
// and the campaign froze.
//
// The answer is read from the SAME lookups the send gates run (one query per
// rule, shared by the single-address gate and the batch read here), so the
// status answer and the send answer cannot disagree. Nothing is persisted.

import { findStandingOptOuts, OPT_OUT_REFUSAL_CODE } from "./lead-optouts";
import { findNotAProspects, NOT_A_PROSPECT_REFUSAL_CODE } from "./not-a-prospect";
import {
  findRecentBrandContacts,
  normalizeLeadEmail,
  RECONTACT_REFUSAL_CODE,
} from "./recontact-window";

export interface SendRefusal {
  /** The send route's own 409 `code`. */
  code: string;
  /** ISO instant the refusal lapses; null = it never lapses on its own. */
  until: string | null;
}

/**
 * Per address (keyed by the address AS ASKED), the refusal `POST /orgs/send`
 * would give for this org and these brands. Absent = no refusal.
 *
 * Several rules can stand at once; the one that lasts LONGEST is stated
 * (a never-lapsing rule beats a window), because the consumer's question is
 * "when may I hand this person back?" and the answer is when the last gate
 * opens. Fail loud: a DB error propagates (an unreadable refusal must not read
 * as "none").
 */
export async function findSendRefusals(
  orgId: string,
  brandIds: string[],
  emails: string[],
): Promise<Map<string, SendRefusal>> {
  const [optOuts, notAProspects, recentContacts] = await Promise.all([
    findStandingOptOuts(orgId, emails),
    findNotAProspects(emails, brandIds),
    findRecentBrandContacts(emails, brandIds),
  ]);

  const out = new Map<string, SendRefusal>();
  for (const email of emails) {
    const key = normalizeLeadEmail(email);
    if (optOuts.has(key)) {
      out.set(email, { code: OPT_OUT_REFUSAL_CODE, until: null });
      continue;
    }
    if (notAProspects.has(key)) {
      out.set(email, { code: NOT_A_PROSPECT_REFUSAL_CODE, until: null });
      continue;
    }
    const recent = recentContacts.get(key);
    if (recent) {
      out.set(email, { code: RECONTACT_REFUSAL_CODE, until: recent.lapsesAt.toISOString() });
    }
  }
  return out;
}
