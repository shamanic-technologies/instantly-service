import { Router } from "express";

const router = Router();

router.get("/", (req, res) => {
  res.json({
    service: "instantly-service",
    version: "1.0.0",
  });
});

router.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "instantly-service",
  });
});

export default router;
