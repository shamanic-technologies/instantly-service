import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import * as dotenv from "dotenv";

dotenv.config();

const connectionString = process.env.INSTANTLY_SERVICE_DATABASE_URL;

const pool = new Pool({
  connectionString: connectionString?.includes("sslmode=")
    ? connectionString
    : `${connectionString}${connectionString?.includes("?") ? "&" : "?"}sslmode=verify-full`,
});

export const db = drizzle(pool);

export async function closeDb() {
  await pool.end();
}
