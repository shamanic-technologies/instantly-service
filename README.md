# Instantly Service

Cold email outreach service via [Instantly.ai](https://instantly.ai/) API V2. Handles campaign management, lead upload, email account warmup, and webhook processing. Integrates with runs-service for cost tracking (BLOCKING).

## API Endpoints

### Campaigns
- **POST /campaigns** - Create campaign in Instantly + DB, log run (BLOCKING)
- **GET /campaigns/:campaignId** - Get campaign details
- **GET /campaigns/by-org/:orgId** - List campaigns for org
- **PATCH /campaigns/:campaignId/status** - Update status (active/paused/completed)

### Leads
- **POST /campaigns/:campaignId/leads** - Add leads (bulk), log run + costs (BLOCKING)
- **GET /campaigns/:campaignId/leads** - List leads
- **DELETE /campaigns/:campaignId/leads** - Remove leads

### Accounts
- **GET /accounts** - List email accounts
- **POST /accounts/sync** - Sync from Instantly API to local DB
- **POST /accounts/:email/warmup** - Enable/disable warmup
- **GET /accounts/warmup-analytics** - Warmup stats

### Analytics
- **GET /campaigns/:campaignId/analytics** - Fetch from Instantly API, save snapshot
- **POST /stats** - Aggregated stats by runIds

### Webhooks
- **POST /webhooks/instantly** - Receives Instantly events (public, no auth)

### Health
- **GET /** - Service info
- **GET /health** - Health check

## Tech Stack

- **Runtime:** Node.js 20 + TypeScript
- **Framework:** Express
- **Database:** PostgreSQL (Neon) via Drizzle ORM
- **Email API:** Instantly.ai API V2
- **Testing:** Vitest + Supertest

## Database Schema

| Table | Purpose |
|-------|---------|
| `instantly_campaigns` | Campaign records |
| `instantly_leads` | Lead records per campaign |
| `instantly_accounts` | Email accounts + warmup status |
| `instantly_events` | Webhook events |
| `instantly_analytics_snapshots` | Campaign analytics snapshots |

## Setup

```bash
npm install
cp .env.example .env  # fill in values
npm run dev
```

## Scripts

| Command | Description |
|---------|-------------|
| `npm run dev` | Start dev server (nodemon) |
| `npm run build` | Compile TypeScript |
| `npm start` | Run compiled app |
| `npm test` | Run all tests |
| `npm run test:unit` | Unit tests only |
| `npm run test:integration` | Integration tests only |
| `npm run db:generate` | Generate Drizzle migrations |
| `npm run db:migrate` | Run migrations |
| `npm run db:push` | Push schema to database |

## Environment Variables

- `INSTANTLY_SERVICE_DATABASE_URL` - PostgreSQL connection string
- `KEY_SERVICE_URL` / `KEY_SERVICE_API_KEY` - Key service for API key decryption
- `INSTANTLY_SERVICE_API_KEY` - Service-to-service auth secret
- `RUNS_SERVICE_URL` / `RUNS_SERVICE_API_KEY` - Runs service integration
- `PORT` - Server port (default: 3011)

## Authentication

All endpoints require `X-API-Key` header except:
- `GET /` and `GET /health` (public)
- `POST /webhooks/instantly` (public webhook receiver)

## Project Structure

```
src/
  index.ts              # Express app entry point
  db/
    index.ts            # Database connection
    schema.ts           # All table definitions
  lib/
    instantly-client.ts # Instantly API V2 HTTP client
    runs-client.ts      # Runs service HTTP client (BLOCKING)
  middleware/
    serviceAuth.ts      # API key auth middleware
  routes/
    health.ts           # Health check routes
    campaigns.ts        # Campaign CRUD
    leads.ts            # Lead management
    accounts.ts         # Email accounts + warmup
    analytics.ts        # Campaign analytics
    webhooks.ts         # Instantly webhook receiver
tests/
  unit/                 # Unit tests
  integration/          # Integration tests
  helpers/              # Test utilities
drizzle/                # SQL migrations
```
