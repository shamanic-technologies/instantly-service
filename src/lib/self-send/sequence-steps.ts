/**
 * Sequence steps — SILVER, the one piece of genuinely new state.
 *
 * Today the body of each step lives at Instantly, and `instantly_campaigns_config_raw`
 * is only our BRONZE mirror of what Instantly holds. Once we dispatch a sequence
 * ourselves there is nothing upstream to mirror, so the steps have to be state we
 * own — which moves them UP a layer, from a mirror to canonical silver, rather
 * than creating a second bronze beside Instantly's.
 *
 * Everything else the self-send scheduler needs already exists and is reused as
 * is: the queue is the set of `sequence_costs` rows still `provisioned`, the
 * cadence is `delayForGap`, the caps are `rampCapForAge` / `dailyLimitForStatus`,
 * and the account is `accountFillOrder`. No schedule table, no new accumulator.
 *
 * INDEXING (load-bearing). `step` is 1-based, matching `sequence_costs.step`, and
 * a row's `delay_days` is the gap from ITS step to the next one — so the gap from
 * step k to k+1 is `row(step=k).delay_days`. Ordering the rows by step therefore
 * yields exactly the 0-based array `delayForGap` already expects, and the
 * self-send scheduler, the fleet forecast and the per-account queue breakdown all
 * keep resolving cadence through the same function.
 */

/** One step as persisted in silver. */
export interface SequenceStep {
  step: number;
  subject: string | null;
  bodyHtml: string;
  delayDays: number | null;
}

/**
 * Extract the steps from an Instantly-shaped sequence config.
 *
 * Kept on Instantly's own shape (`sequences[0].steps[]`) deliberately: it is the
 * shape `POST /orgs/send` already receives from email-gateway and the shape the
 * bronze config mirror already stores, so a campaign can be read from either
 * source with one parser during the cutover.
 *
 * A step with no body is dropped and the remainder renumbered contiguously — an
 * empty email is not dispatchable, and leaving a hole in the step numbering would
 * silently desynchronise these rows from the `sequence_costs` steps they pair with.
 */
export function stepsFromSequenceConfig(config: unknown): SequenceStep[] {
  const sequences = (config as { sequences?: unknown[] } | null)?.sequences;
  if (!Array.isArray(sequences) || sequences.length === 0) return [];

  const rawSteps = (sequences[0] as { steps?: unknown[] } | null)?.steps;
  if (!Array.isArray(rawSteps)) return [];

  const steps: SequenceStep[] = [];

  for (const raw of rawSteps) {
    const entry = raw as { subject?: unknown; body?: unknown; delay?: unknown };
    const bodyHtml = typeof entry.body === "string" ? entry.body : "";
    if (!bodyHtml) continue;

    const subject = typeof entry.subject === "string" && entry.subject !== "" ? entry.subject : null;
    const delay = entry.delay;
    const delayDays =
      typeof delay === "number" && Number.isFinite(delay) && delay >= 0 ? delay : null;

    steps.push({ step: steps.length + 1, subject, bodyHtml, delayDays });
  }

  return steps;
}

/**
 * Project persisted rows onto the 0-based delay array `delayForGap` indexes.
 *
 * A step absent from the rows yields `null` at its slot, which `delayForGap`
 * resolves to `STEP_GAP_CALENDAR_DAYS` — the same last-resort fallback the ops
 * views already use, so a missing row degrades a single gap instead of dropping
 * the step from the schedule.
 */
export function stepDelaysFromRows(
  rows: readonly Pick<SequenceStep, "step" | "delayDays">[],
): (number | null)[] {
  if (rows.length === 0) return [];

  const highest = Math.max(...rows.map((r) => r.step));
  const delays: (number | null)[] = new Array(highest).fill(null);

  for (const row of rows) {
    if (row.step >= 1) delays[row.step - 1] = row.delayDays;
  }

  return delays;
}
