# Project: instantly-service

Cold email outreach service via Instantly.ai API V2. Handles campaign management, lead upload, email accounts, warmup, analytics, and webhook processing.

## Commands

- `npm test` — run all tests (Vitest)
- `npm run test:unit` — unit tests only
- `npm run test:integration` — integration tests only
- `npm run build` — compile TypeScript + generate OpenAPI spec
- `npm run dev` — local dev server (nodemon)
- `npm run generate:openapi` — regenerate openapi.json
- `npm run db:generate` — generate Drizzle migrations
- `npm run db:migrate` — run migrations
- `npm run db:push` — push schema to database

## Architecture

- `src/index.ts` — Express app entry point
- `src/schemas.ts` — Zod schemas (source of truth for validation + OpenAPI)
- `src/routes/` — Route handlers (campaigns, leads, accounts, analytics, send, webhooks, health)
- `src/middleware/serviceAuth.ts` — API key auth middleware
- `src/lib/instantly-client.ts` — Instantly API V2 HTTP client
- `src/lib/runs-client.ts` — Runs service HTTP client for cost tracking
- `src/db/schema.ts` — Drizzle ORM table definitions (campaigns, leads, accounts, events, analytics_snapshots)
- `src/db/index.ts` — Database connection
- `tests/unit/` — Unit tests
- `tests/integration/` — Integration tests
- `tests/helpers/` — Test utilities (test app, test DB)
- `openapi.json` — Auto-generated, do NOT edit manually
