import { OpenApiGeneratorV3 } from "@asteasolutions/zod-to-openapi";
import { registry } from "../src/schemas";
import * as fs from "fs";

const generator = new OpenApiGeneratorV3(registry.definitions);

const document = generator.generateDocument({
  openapi: "3.0.0",
  info: {
    title: "Instantly Service",
    description:
      "Cold email outreach service via Instantly.ai API. Manages campaigns, leads, accounts, analytics, and webhook events.",
    version: "1.0.0",
  },
  servers: [
    { url: process.env.SERVICE_URL || "http://localhost:3011" },
  ],
});

fs.writeFileSync("openapi.json", JSON.stringify(document, null, 2));
console.log("Generated openapi.json");
