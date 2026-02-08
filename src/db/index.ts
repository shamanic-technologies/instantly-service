import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import * as dotenv from "dotenv";

dotenv.config();

function enforceSslMode(url: string | undefined): string | undefined {
  if (!url) return url;
  if (url.includes("sslmode=")) {
    return url.replace(/sslmode=[^&]+/, "sslmode=verify-full");
  }
  return `${url}${url.includes("?") ? "&" : "?"}sslmode=verify-full`;
}

const pool = new Pool({
  connectionString: enforceSslMode(process.env.INSTANTLY_SERVICE_DATABASE_URL),
});

export const db = drizzle(pool);

export async function closeDb() {
  await pool.end();
}
