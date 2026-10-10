/**
 * In-process drive for the outreach fact feed (lib/outreach-facts): judge the
 * replies still owed a Jev judgment (lib/reply-judgments), then emit. Armed
 * after `listen`, mutexed, state entirely in the DB — a deploy mid-tick loses
 * nothing. The first pass on an empty feed is the full backfill.
 */
import { feedHasReplyFacts, syncOutreachFacts } from "./outreach-facts";
import { judgePendingReplies } from "./reply-judgments";

export const OUTREACH_FACTS_INTERVAL_MS = 2 * 60 * 1000;
/** Events inserted in the last two days; the overlap is free (unique subject). */
const TICK_WINDOW_DAYS = 2;
/** Replies judged per tick (one chat-service call each). */
const JUDGE_PER_TICK = 100;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

export async function runOutreachFactsTick(): Promise<void> {
  // The backfill judges the whole history before its first reply fact, so the
  // feed does not open with hundreds of facts it corrects minutes later.
  const judged = await judgePendingReplies((await feedHasReplyFacts()) ? JUDGE_PER_TICK : 100_000);
  const summary = await syncOutreachFacts({ sinceDays: TICK_WINDOW_DAYS });
  const emitted =
    summary.eventFacts + summary.withdrawnEvents + summary.replyFacts + summary.replyCorrections + summary.withdrawnReplies + summary.repliesSent;
  if (emitted > 0 || judged.judged > 0) {
    console.log(`[instantly-service] outreach-facts: ${JSON.stringify({ ...summary, judged: judged.judged })}`);
  }
}

export function startOutreachFactsWorker(): void {
  if (timer) return;
  const tick = () => {
    if (inFlight) return;
    inFlight = true;
    runOutreachFactsTick()
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[instantly-service] outreach-facts tick failed: ${message}`);
      })
      .finally(() => {
        inFlight = false;
      });
  };
  timer = setInterval(tick, OUTREACH_FACTS_INTERVAL_MS);
  timer.unref?.();
  console.log(`[instantly-service] outreach-facts worker armed, every ${OUTREACH_FACTS_INTERVAL_MS}ms`);
}
