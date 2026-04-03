import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyAccounts } from "../db/schema";
import { eq } from "drizzle-orm";
import {
  listAccounts as listInstantlyAccounts,
  enableWarmup as enableInstantlyWarmup,
  disableWarmup as disableInstantlyWarmup,
  getWarmupAnalytics,
} from "../lib/instantly-client";
import { resolveInstantlyApiKey } from "../lib/key-client";
import { WarmupRequestSchema } from "../schemas";

const router = Router();

/**
 * GET /accounts
 */
router.get("/", async (req: Request, res: Response) => {
  try {
    const accounts = await db.select().from(instantlyAccounts);
    res.json({ accounts });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /accounts/sync
 * Sync accounts from Instantly API to local DB
 */
router.post("/sync", async (req: Request, res: Response) => {
  try {
    // Resolve Instantly API key via key-service
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/accounts/sync",
    });

    const instantlyAccountsList = await listInstantlyAccounts(apiKey);

    for (const account of instantlyAccountsList) {
      const warmupEnabled = account.warmup_status === 1;
      const status = account.status > 0 ? "active" : "inactive";

      await db
        .insert(instantlyAccounts)
        .values({
          email: account.email,
          warmupEnabled,
          status,
          dailySendLimit: account.daily_limit,
        })
        .onConflictDoUpdate({
          target: instantlyAccounts.email,
          set: {
            warmupEnabled,
            status,
            dailySendLimit: account.daily_limit,
            updatedAt: new Date(),
          },
        });
    }

    res.json({
      success: true,
      synced: instantlyAccountsList.length,
    });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /accounts/:email/warmup
 */
router.post("/:email/warmup", async (req: Request, res: Response) => {
  const { email } = req.params;

  const parsed = WarmupRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "enabled (boolean) required" });
  }
  const { enabled } = parsed.data;

  try {
    // Resolve Instantly API key via key-service
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/accounts/:email/warmup",
    });

    if (enabled) {
      await enableInstantlyWarmup(apiKey, email);
    } else {
      await disableInstantlyWarmup(apiKey, email);
    }

    await db
      .update(instantlyAccounts)
      .set({ warmupEnabled: enabled, updatedAt: new Date() })
      .where(eq(instantlyAccounts.email, email));

    res.json({ success: true, email, warmupEnabled: enabled });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /accounts/warmup-analytics
 */
router.get("/warmup-analytics", async (req: Request, res: Response) => {
  try {
    // Resolve Instantly API key via key-service
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey } = await resolveInstantlyApiKey(orgId, userId, {
      method: "GET",
      path: "/accounts/warmup-analytics",
    });

    const analytics = await getWarmupAnalytics(apiKey);
    res.json({ analytics });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
