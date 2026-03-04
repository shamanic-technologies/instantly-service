/**
 * HTTP client for transactional-email-service
 * Used to send admin notification emails (e.g. campaign errors).
 */

const TRANSACTIONAL_EMAIL_SERVICE_URL =
  process.env.TRANSACTIONAL_EMAIL_SERVICE_URL || "http://localhost:3000";
const TRANSACTIONAL_EMAIL_SERVICE_API_KEY =
  process.env.TRANSACTIONAL_EMAIL_SERVICE_API_KEY || "";

interface TemplateItem {
  name: string;
  subject: string;
  htmlBody: string;
  textBody?: string;
  from?: string;
  messageStream?: string;
}

interface DeployTemplatesParams {
  appId: string;
  templates: TemplateItem[];
}

interface SendEmailParams {
  appId: string;
  eventType: string;
  recipientEmail: string;
  metadata?: Record<string, string>;
}

interface EmailIdentityContext {
  orgId: string;
  userId: string;
  runId?: string;
}

async function emailServiceRequest<T>(
  path: string,
  identity: EmailIdentityContext,
  options: { method?: string; body?: unknown } = {},
): Promise<T> {
  const { method = "GET", body } = options;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "x-api-key": TRANSACTIONAL_EMAIL_SERVICE_API_KEY,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
  };
  if (identity.runId) {
    headers["x-run-id"] = identity.runId;
  }

  const response = await fetch(`${TRANSACTIONAL_EMAIL_SERVICE_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `transactional-email-service ${method} ${path} failed: ${response.status} - ${errorText}`,
    );
  }

  return response.json() as Promise<T>;
}

export async function deployTemplates(
  params: DeployTemplatesParams,
  identity: EmailIdentityContext,
): Promise<void> {
  await emailServiceRequest("/templates", identity, {
    method: "PUT",
    body: params,
  });
}

export async function sendEmail(
  params: SendEmailParams,
  identity: EmailIdentityContext,
): Promise<void> {
  await emailServiceRequest("/send", identity, {
    method: "POST",
    body: params,
  });
}
