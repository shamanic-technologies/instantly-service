/**
 * twilio-service client — ring a human and tell them what happened.
 *
 * The call is not a notification played at whoever picks up. twilio-service opens
 * with a summary and asks for a keypress; nothing else is spoken until that key
 * arrives, so an answering machine hears only the opener and the call is never
 * recorded as taken. The detail (who replied, at which company, what they wrote)
 * plays after the keypress, and a SECOND deliberate keypress bridges the rep to
 * the prospect — offered only when a number to connect to was supplied.
 *
 * ⚠️ OMITTING `connectTo` IS A SPOKEN OUTCOME, NOT A DEGRADED ONE. With no number
 * the call still happens and SAYS, in words, that we do not have the prospect's
 * number and cannot connect them. That is the required behaviour when the reveal
 * found nothing, ran out of time, or came back flagged do-not-call: the rep
 * learns a buyer is interested either way, and learns it in about a minute.
 *
 * FAILS LOUD on any non-2xx, with the status and body intact. The caller is a
 * fail-soft side effect and swallows it, but a call that was never placed must
 * never read as one that was.
 */

const TWILIO_SERVICE_URL = process.env.TWILIO_SERVICE_URL;
const TWILIO_SERVICE_API_KEY = process.env.TWILIO_SERVICE_API_KEY;

/** The reply the call is about — spoken after the accept keypress. */
export interface CallReply {
  /** Who replied. */
  name: string;
  /** Their company, when known. */
  company?: string;
  /** What they actually wrote. */
  message: string;
}

export interface PlaceCallParams {
  /** Identity: twilio-service scopes the call's run and cost to this org. */
  orgId: string;
  userId: string;
  /** Number to ring, E.164 — the brand's sales rep. */
  to: string;
  reply: CallReply;
  /** Brand whose campaign was replied to, spoken in the opener. */
  brandName?: string;
  /**
   * Number to bridge to on the second keypress, E.164. OMIT IT and the call says
   * the connect option is unavailable — never pass a number we are not certain
   * is the prospect's and safe to dial.
   */
  connectTo?: string;
  /** Who the connect keypress reaches, when not the replier. */
  connectName?: string;
  parentRunId?: string;
  brandId?: string;
  campaignId?: string;
}

export interface PlacedCall {
  success: boolean;
  callId: string;
  callSid?: string;
  status?: string;
  costName: string;
  /** Whether a number to connect to was supplied — i.e. whether a bridge is on offer. */
  connectOffered: boolean;
}

/** Place the outbound call. Throws on anything that is not a 2xx. */
export async function placeCall(params: PlaceCallParams): Promise<PlacedCall> {
  if (!TWILIO_SERVICE_URL || !TWILIO_SERVICE_API_KEY) {
    throw new Error("TWILIO_SERVICE_URL or TWILIO_SERVICE_API_KEY is not set");
  }

  const { orgId, userId, ...body } = params;
  const response = await fetch(`${TWILIO_SERVICE_URL}/calls`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": TWILIO_SERVICE_API_KEY,
      "x-org-id": orgId,
      "x-user-id": userId,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `twilio-service POST /calls failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  return (await response.json()) as PlacedCall;
}
