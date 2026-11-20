/**
 * The SES kill switch (SES-COLD-1119), in a leaf module so every reader (the
 * outbound door, the send-only sender set) reads the same env at the same time.
 * "true" = every SES domain sends over its mailbox's SMTP again; read at EACH
 * call (env file + container recreate, no deploy).
 */
export const SES_KILL_SWITCH_ENV = "SES_COLD_SENDING_DISABLED";

export function sesColdSendingDisabled(): boolean {
  return (process.env[SES_KILL_SWITCH_ENV] ?? "").trim().toLowerCase() === "true";
}
