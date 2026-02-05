import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import * as dotenv from "dotenv";

dotenv.config();

const pool = new Pool({
  connectionString: process.env.INSTANTLY_SERVICE_DATABASE_URL,
});

export const db = drizzle(pool);

export async function closeDb() {
  await pool.end();
}
