/**
 * Read where Microsoft put our seeds (Outlook.com, Microsoft 365) through Graph.
 * MS-SEED-PLACEMENT-1018.
 *
 * Only two well-known folders carry a verdict: `inbox` (Microsoft's Inbox, both
 * Focused and Other) and `junkemail`. Inside the Inbox Graph also says whether
 * the message was sorted to Focused or Other (`inferenceClassification`); both
 * count as Inbox, Other is recorded beside. The spam confidence level Microsoft
 * scored the message at (SCL, -1..9; 5+ goes to Junk) is kept for diagnosis.
 */

import { parseAuthResults, type AuthResults, type SeedPlacement } from "./classify";
import { graphGet } from "./microsoft-oauth";

/** Graph well-known folder names, in read order. */
export const MICROSOFT_SEED_FOLDERS = ["inbox", "junkemail"] as const;
export type MicrosoftSeedFolder = (typeof MICROSOFT_SEED_FOLDERS)[number];

export interface GraphMessage {
  internetMessageId?: string | null;
  inferenceClassification?: string | null;
  internetMessageHeaders?: { name?: string; value?: string }[] | null;
}

export interface MicrosoftSeedObservation extends AuthResults {
  messageId: string;
  folder: MicrosoftSeedFolder;
  placement: SeedPlacement;
  /** `focused` | `other` for an Inbox message; null in Junk or when Graph says nothing. */
  inference: "focused" | "other" | null;
  /** Microsoft's spam confidence level, null when the header is absent. */
  scl: number | null;
}

function header(message: GraphMessage, name: string): string | null {
  const wanted = name.toLowerCase();
  const found = (message.internetMessageHeaders ?? []).find((h) => (h.name ?? "").toLowerCase() === wanted);
  return found?.value ?? null;
}

/** SCL from `X-MS-Exchange-Organization-SCL`, else the `SCL:` field of the antispam report. */
export function sclOf(message: GraphMessage): number | null {
  const direct = header(message, "X-MS-Exchange-Organization-SCL");
  const fromReport = /(?:^|;)\s*SCL:(-?\d+)/i.exec(header(message, "X-Forefront-Antispam-Report") ?? "")?.[1];
  const raw = direct?.trim() ?? fromReport;
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  return Number.isInteger(n) ? n : null;
}

/** One Graph message read from one folder → an observation, or null if it carries no Message-Id. Pure. */
export function classifyGraphMessage(
  folder: MicrosoftSeedFolder,
  message: GraphMessage,
): MicrosoftSeedObservation | null {
  const messageId = message.internetMessageId?.trim();
  if (!messageId) return null;
  const inference = message.inferenceClassification?.toLowerCase();
  return {
    messageId,
    folder,
    placement: folder === "inbox" ? "inbox" : "spam",
    inference: folder === "inbox" && (inference === "focused" || inference === "other") ? inference : null,
    scl: sclOf(message),
    ...parseAuthResults(header(message, "Authentication-Results")),
  };
}

/** Every message received since `since` in the Inbox and Junk of one mailbox, classified. */
export async function readMicrosoftSeedFolders(
  accessToken: string,
  since: Date,
  get: (token: string, pathOrUrl: string) => Promise<unknown> = graphGet,
): Promise<MicrosoftSeedObservation[]> {
  const out: MicrosoftSeedObservation[] = [];
  for (const folder of MICROSOFT_SEED_FOLDERS) {
    const query = new URLSearchParams({
      $filter: `receivedDateTime ge ${since.toISOString()}`,
      $select: "internetMessageId,inferenceClassification,internetMessageHeaders,receivedDateTime",
      $top: "100",
    });
    let next: string | null = `/me/mailFolders/${folder}/messages?${query.toString()}`;
    while (next) {
      const page = (await get(accessToken, next)) as { value?: GraphMessage[]; "@odata.nextLink"?: string } | null;
      for (const message of page?.value ?? []) {
        const observation = classifyGraphMessage(folder, message);
        if (observation) out.push(observation);
      }
      next = page?.["@odata.nextLink"] ?? null;
    }
  }
  return out;
}
