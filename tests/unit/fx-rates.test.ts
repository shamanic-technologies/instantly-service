import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...args: unknown[]) => mockExecute(...args) } }));

import {
  parseEcbDailyXml,
  toUsdCents,
  describeFx,
  syncFxRates,
  loadLatestEurUsd,
  ECB_SOURCE,
  type FxRate,
} from "../../src/lib/fx-rates";

// The ECB daily envelope as it actually arrives (trimmed to three currencies).
const ECB_ENVELOPE = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
	<gesmes:subject>Reference rates</gesmes:subject>
	<gesmes:Sender><gesmes:name>European Central Bank</gesmes:name></gesmes:Sender>
	<Cube>
		<Cube time='2026-09-21'>
			<Cube currency='USD' rate='1.1490'/>
			<Cube currency='JPY' rate='168.21'/>
			<Cube currency='GBP' rate='0.84120'/>
		</Cube>
	</Cube>
</gesmes:Envelope>`;

const FX: FxRate = { base: "EUR", quote: "USD", rate: 1.149, asOf: "2026-09-21", source: ECB_SOURCE };

/** node-postgres resolves db.execute to a QueryResult object, never a bare array. */
function pgResult(rows: unknown[], rowCount = rows.length) {
  return { command: "SELECT", rowCount, oid: 0, fields: [], rows };
}

describe("parseEcbDailyXml", () => {
  it("reads the reference day and the USD rate, not the first currency listed", () => {
    expect(parseEcbDailyXml(ECB_ENVELOPE)).toEqual({ asOf: "2026-09-21", eurUsd: 1.149 });
  });

  it("throws rather than half-parse when the USD rate is missing", () => {
    expect(() => parseEcbDailyXml(ECB_ENVELOPE.replace(/<Cube currency='USD'[^>]*\/>/, ""))).toThrow(/USD/);
  });

  it("throws when the envelope carries no reference day", () => {
    expect(() => parseEcbDailyXml(ECB_ENVELOPE.replace("time='2026-09-21'", ""))).toThrow(/reference day/);
  });

  it("throws on a rate that is not a positive number", () => {
    expect(() => parseEcbDailyXml(ECB_ENVELOPE.replace("rate='1.1490'", "rate='0'"))).toThrow(/positive/);
  });
});

describe("toUsdCents", () => {
  it("passes USD through unchanged", () => {
    expect(toUsdCents(5125, "USD", FX)).toBe(5125);
  });

  it("converts EUR at the rate and rounds to whole cents", () => {
    // 3838 EUR cents (Gandi's yearly registration) × 1.149 = 4409.862
    expect(toUsdCents(3838, "EUR", FX)).toBe(4410);
  });

  it("returns null for EUR when no rate is on record — never a guessed rate", () => {
    expect(toUsdCents(3838, "EUR", null)).toBeNull();
  });

  it("returns null for a currency it has no rate for", () => {
    expect(toUsdCents(1000, "GBP", FX)).toBeNull();
  });

  it("returns null when there is no amount or no currency", () => {
    expect(toUsdCents(null, "EUR", FX)).toBeNull();
    expect(toUsdCents(100, null, FX)).toBeNull();
  });
});

describe("describeFx", () => {
  it("names the rate, its day and its source so every USD figure is traceable", () => {
    expect(describeFx(FX)).toEqual({ base: "EUR", quote: "USD", rate: 1.149, asOf: "2026-09-21", source: ECB_SOURCE });
  });

  it("is null when nothing is on record", () => {
    expect(describeFx(null)).toBeNull();
  });
});

describe("syncFxRates", () => {
  beforeEach(() => mockExecute.mockReset());

  it("records the ECB rate for its reference day, idempotently", async () => {
    mockExecute.mockResolvedValue(pgResult([], 1));
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true, status: 200, text: async () => ECB_ENVELOPE });

    const summary = await syncFxRates(fetchImpl as unknown as typeof fetch);

    expect(summary).toEqual({ asOf: "2026-09-21", eurUsd: 1.149, inserted: 1 });
    const sqlText = JSON.stringify(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("ON CONFLICT (base, quote, as_of) DO NOTHING");
  });

  it("fails loud when the ECB answers an error, and writes nothing", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 503, text: async () => "" });

    await expect(syncFxRates(fetchImpl as unknown as typeof fetch)).rejects.toThrow(/503/);
    expect(mockExecute).not.toHaveBeenCalled();
  });
});

describe("loadLatestEurUsd", () => {
  beforeEach(() => mockExecute.mockReset());

  it("reads the most recent rate, coercing node-postgres' numeric string", async () => {
    mockExecute.mockResolvedValue(pgResult([{ rate: "1.14900000", as_of: "2026-09-21", source: ECB_SOURCE }]));
    expect(await loadLatestEurUsd()).toEqual(FX);
  });

  it("is null when no rate has ever been fetched", async () => {
    mockExecute.mockResolvedValue(pgResult([]));
    expect(await loadLatestEurUsd()).toBeNull();
  });
});
