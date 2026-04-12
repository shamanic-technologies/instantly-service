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

// TEMPORARY: debug endpoint to inspect received headers
router.get("/debug/headers", (req, res) => {
  res.json({
    "x-org-id": req.headers["x-org-id"],
    "x-org-id-type": typeof req.headers["x-org-id"],
    "x-api-key": req.headers["x-api-key"] ? "present" : "missing",
    "x-user-id": req.headers["x-user-id"],
    allHeaders: Object.fromEntries(
      Object.entries(req.headers).filter(([k]) => k.startsWith("x-")),
    ),
  });
});

export default router;
