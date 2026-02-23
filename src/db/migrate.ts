import { migrate } from "drizzle-orm/node-postgres/migrator";
import { db } from "./index";
import path from "path";

export async function runMigrations() {
  const migrationsFolder = path.join(__dirname, "..", "..", "drizzle");
  console.log("[db] Running migrations...");
  await migrate(db, { migrationsFolder });
  console.log("[db] Migrations complete");
}
