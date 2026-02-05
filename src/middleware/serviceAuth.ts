import { Request, Response, NextFunction } from "express";

const INSTANTLY_SERVICE_API_KEY = process.env.INSTANTLY_SERVICE_API_KEY || "";

export function serviceAuth(req: Request, res: Response, next: NextFunction) {
  const apiKey = req.headers["x-api-key"];

  if (!apiKey || apiKey !== INSTANTLY_SERVICE_API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  next();
}
