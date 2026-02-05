import express from "express";
import cors from "cors";
import * as dotenv from "dotenv";

dotenv.config();

import healthRoutes from "./routes/health";
import campaignsRoutes from "./routes/campaigns";
import leadsRoutes from "./routes/leads";
import accountsRoutes from "./routes/accounts";
import analyticsRoutes from "./routes/analytics";
import webhooksRoutes from "./routes/webhooks";
import sendRoutes from "./routes/send";
import { serviceAuth } from "./middleware/serviceAuth";

const app = express();

app.use(cors());
app.use(express.json());

// Public routes (no auth)
app.use(healthRoutes);
app.use("/webhooks", webhooksRoutes);

// Protected routes (require X-API-Key)
app.use("/send", serviceAuth, sendRoutes);
app.use("/campaigns", serviceAuth, campaignsRoutes);
app.use("/campaigns", serviceAuth, leadsRoutes);
app.use("/accounts", serviceAuth, accountsRoutes);
app.use(serviceAuth, analyticsRoutes);

const PORT = process.env.PORT || 3011;

app.listen(PORT, () => {
  console.log(`instantly-service running on port ${PORT}`);
});

export { app };
