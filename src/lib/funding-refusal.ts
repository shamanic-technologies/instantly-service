/**
 * A spend refusal that names its REAL cause (FUNNEL-CAP-NAMED-402-1016).
 *
 * billing-service `POST /v1/customer_balance/authorize` answers
 * `{ sufficient:false, balance_cents, required_cents, reason?, sales_funnel_cap? }`.
 * `reason` is present ONLY when the refusal is not a balance shortfall: today
 * `sales_funnel_max_budget_reached` / `sales_funnel_max_budget_unreadable` (the
 * sales funnel campaign hit the max budget its owner set; the org may well have
 * credit). Told "Insufficient credits", the client adds credit, which changes
 * nothing. So every refusal we answer or relay carries billing's `reason` and
 * `sales_funnel_cap` VERBATIM, under those names, and its `error` names the cap.
 *
 * Backward compatible on purpose: the status (402) and `code: "insufficient_credits"`
 * stay, so every caller keying on them (email-gateway relays byte-equal, workflows
 * back off) keeps its stop/back-off behaviour. A refusal WITHOUT `reason` (a real
 * shortfall) is byte-identical to before. We relay what billing decided; no logic.
 */

/** billing's cause for a refusal that is not a balance shortfall, verbatim. */
export interface FundingRefusalCause {
  reason: string;
  /** billing's `sales_funnel_cap` object, untouched (absent when billing sent none). */
  sales_funnel_cap?: unknown;
}

/** Read billing's (or a sibling relaying billing's) named cause off a refusal body. */
export function fundingRefusalCause(body: unknown): FundingRefusalCause | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const b = body as { reason?: unknown; sales_funnel_cap?: unknown };
  if (typeof b.reason !== "string" || b.reason.length === 0) return null;
  const cause: FundingRefusalCause = { reason: b.reason };
  if (b.sales_funnel_cap !== undefined) cause.sales_funnel_cap = b.sales_funnel_cap;
  return cause;
}

/**
 * Same, from a sibling's raw error text (apollo-service answers in JSON; a body
 * that is not JSON can still name a funnel-cap reason in its prose).
 */
export function fundingRefusalCauseFromText(text: string): FundingRefusalCause | null {
  try {
    const fromJson = fundingRefusalCause(JSON.parse(text));
    if (fromJson && isSalesFunnelCapReason(fromJson.reason)) return fromJson;
  } catch {
    // not JSON: fall through to the prose
  }
  const m = /sales_funnel_max_budget_[a-z_]+/.exec(text);
  return m ? { reason: m[0] } : null;
}

export function isSalesFunnelCapReason(reason: string): boolean {
  return reason.startsWith("sales_funnel_max_budget");
}

/** The `error` line of a refusal: names the cap, never "Insufficient credits". */
export function fundingRefusalError(cause: FundingRefusalCause): string {
  if (cause.reason === "sales_funnel_max_budget_reached") return "Sales funnel max budget reached";
  if (cause.reason === "sales_funnel_max_budget_unreadable") return "Sales funnel max budget unreadable";
  return `Spend refused by billing: ${cause.reason}`;
}

/** What the caller can do about it, in words (never "add credit" for a cap). */
export function fundingRefusalDetails(cause: FundingRefusalCause): string {
  if (cause.reason === "sales_funnel_max_budget_reached") {
    return "the sales funnel campaign reached the max budget its owner set; raise that campaign's max budget to continue (adding credit changes nothing)";
  }
  if (cause.reason === "sales_funnel_max_budget_unreadable") {
    return "billing could not read the sales funnel campaign's max budget, so it refused the spend; check that campaign's max budget (adding credit changes nothing)";
  }
  return `billing refused the spend (${cause.reason})`;
}

/**
 * The 402 body for a credit refusal. No cause → exactly `base` (today's body).
 * A cause → `error` names it, `reason` + `sales_funnel_cap` verbatim, `code` kept.
 */
export function creditRefusalBody<T extends { error: string; code: string; details?: string }>(
  base: T,
  cause: FundingRefusalCause | null,
  /** What was being paid for, prefixed to the cause's details (e.g. "verifying x@y.com"). */
  paidFor?: string,
): T | (Omit<T, "error" | "details"> & { error: string; details: string; reason: string; sales_funnel_cap?: unknown }) {
  if (!cause) return base;
  return {
    ...base,
    error: fundingRefusalError(cause),
    details: paidFor ? `${paidFor}: ${fundingRefusalDetails(cause)}` : fundingRefusalDetails(cause),
    reason: cause.reason,
    ...(cause.sales_funnel_cap !== undefined ? { sales_funnel_cap: cause.sales_funnel_cap } : {}),
  };
}
