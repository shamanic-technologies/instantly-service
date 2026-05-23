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
- `src/zod-setup.ts` — Side-effect module that extends Zod with `.openapi()`. Import it BEFORE any module that creates `z.object(...).openapi("Name")` schemas.
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

## Shared contract

Cross-provider canonical shapes (`StatusScope`, `RecipientStats`, `EmailStats`, `StepStats`, `RepliesDetail`, `ChannelStats`, `ProviderStatus`, `GlobalStatus`, `ReplyClassification`) live in [`@shamanic-technologies/email-domain-contract`](https://github.com/shamanic-technologies/email-domain-contract). Do NOT redeclare these schemas locally — re-export from the package via `src/schemas.ts`. Instantly-specific schemas that wrap or extend these shapes keep their local declaration.

Two provider-specific fields are **optional in v1** of the contract: `cancelled` and `notSending`. Instantly-service is the source of truth for both — handlers always emit them as populated values. Contract v2 will tighten them to required once postmark-service ships its padding follow-up.

## Zod 4 caveat — contract schemas + `.openapi()`

`@asteasolutions/zod-to-openapi` attaches `.openapi()` to Zod schema instances at the time `extendZodWithOpenApi(z)` runs in the consumer. The contract package's schemas were instantiated before that point in the consumer's module graph, so they do NOT gain `.openapi()` retroactively. Re-export them without `.openapi(name)` and let the generator inline them (no `$ref` name). Local schemas defined in `src/schemas.ts` (after `import "./zod-setup"`) keep their `.openapi(name)` tagging.
