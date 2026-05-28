import { db, closeDb } from "../src/db";
import { sql } from "drizzle-orm";

async function main() {
  const r = await db.execute(sql`
    SELECT count(*)::int AS n
    FROM instantly_campaigns c
    WHERE c.delivery_status='contacted' AND c.status='active' AND c.org_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM instantly_events e WHERE e.campaign_id = c.instantly_campaign_id AND e.event_type='email_sent')
  `);
  const rows = (r as { rows?: Array<{ n: number }> }).rows ?? [];
  console.log("eligible rows:", rows[0]?.n);
}

main().then(() => closeDb()).catch(async (e) => { console.error(e); await closeDb(); process.exit(1); });
