import { defineConfig } from "drizzle-kit";
import * as dotenv from "dotenv";

dotenv.config();

function enforceSslMode(url: string): string {
  if (url.includes("sslmode=")) {
    return url.replace(/sslmode=[^&]+/, "sslmode=verify-full");
  }
  return `${url}${url.includes("?") ? "&" : "?"}sslmode=verify-full`;
}

export default defineConfig({
  schema: "./src/db/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url: enforceSslMode(process.env.INSTANTLY_SERVICE_DATABASE_URL!),
  },
});
