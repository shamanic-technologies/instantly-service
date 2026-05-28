import { db, closeDb } from "../src/db";
import { getCampaign } from "../src/lib/instantly-client";
import { resolveInstantlyApiKey } from "../src/lib/key-client";
import { sql } from "drizzle-orm";

async function main() {
  const rowId = process.argv[2];
  if (!rowId) {
    console.error("Usage: inspect-body.ts <row_id>");
    process.exit(1);
  }
  const result = await db.execute(sql`
    SELECT id, instantly_campaign_id, org_id
    FROM instantly_campaigns
    WHERE id = ${rowId}
  `);
  const rows = (result as { rows?: unknown[] }).rows ?? [];
  if (rows.length === 0) {
    console.log("row not found:", rowId);
    return;
  }
  const row = rows[0] as { id: string; instantly_campaign_id: string; org_id: string };
  console.log("row:", { id: row.id, instantly_campaign_id: row.instantly_campaign_id });
  const r = await resolveInstantlyApiKey(row.org_id, "system", {
    method: "POST",
    path: "/internal/inspect",
  });
  const live = (await getCampaign(r.key, row.instantly_campaign_id)) as {
    sequences?: Array<{ steps?: Array<{ variants?: Array<{ body?: string }> }> }>;
  };
  const body = live.sequences?.[0]?.steps?.[0]?.variants?.[0]?.body ?? "";
  console.log("BODY LENGTH:", body.length);
  console.log("MARKER COUNT (--):", (body.match(/--/g) ?? []).length);
  console.log("CONTAINS Kevin Lourd:", body.includes("Kevin Lourd"));
  console.log("CONTAINS distribute.you:", body.includes("distribute.you"));
  console.log("CONTAINS ❤:", body.includes("❤"));
  console.log("=== FULL BODY ===");
  console.log(body);
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error(e);
    await closeDb();
    process.exit(1);
  });
