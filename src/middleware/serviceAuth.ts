import { Request, Response, NextFunction } from "express";

export function serviceAuth(req: Request, res: Response, next: NextFunction) {
  const apiKey = req.headers["x-api-key"];
  const expected = process.env.INSTANTLY_SERVICE_API_KEY || "";

  if (!apiKey || apiKey !== expected) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  next();
}
