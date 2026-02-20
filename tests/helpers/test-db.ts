import { db, closeDb as closeDbConnection } from "../../src/db";
import {
  instantlyCampaigns,
  instantlyLeads,
  instantlyAccounts,
  instantlyEvents,
  instantlyAnalyticsSnapshots,
  sequenceCosts,
} from "../../src/db/schema";

export async function cleanTestData() {
  await db.delete(sequenceCosts);
  await db.delete(instantlyAnalyticsSnapshots);
  await db.delete(instantlyEvents);
  await db.delete(instantlyLeads);
  await db.delete(instantlyCampaigns);
  await db.delete(instantlyAccounts);
}

export async function closeDb() {
  await closeDbConnection();
}

export function randomUUID(): string {
  return crypto.randomUUID();
}
