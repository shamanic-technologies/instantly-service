/**
 * The three grains every monitoring read answers at (MAILBOX-MONITORING-1019):
 *
 *   account  one sending ADDRESS (`instantly_accounts.email`)
 *   mailbox  the real mailbox the address authenticates as (`mailbox_login`,
 *            the address itself when it is its own mailbox): the unit a daily
 *            quota belongs to
 *   domain   the address's domain
 *
 * A mailbox never spans two domains (prod 2026-10-19: 0 of 316 addresses sit on
 * a login of another domain), so mailbox figures add up to domain figures, and
 * domain figures to the fleet.
 *
 * Filters are AND-ed (`?domain=x&mailbox=y` narrows to y if it is on x, else to
 * nothing). Every value is compared lower-cased.
 */

import { sql, type SQL } from "drizzle-orm";

export const ENTITY_GRAINS = ["account", "mailbox", "domain"] as const;
export type EntityGrain = (typeof ENTITY_GRAINS)[number];

export interface EntityScope {
  account?: string;
  mailbox?: string;
  domain?: string;
}

const norm = (v: string) => v.trim().toLowerCase();

/** Read `account` / `mailbox` / `domain` (and optionally `groupBy`) from a query string. */
export function parseEntityScope(
  q: Record<string, unknown>,
): { scope: EntityScope; groupBy: EntityGrain | null } | { error: string } {
  const scope: EntityScope = {};
  for (const k of ENTITY_GRAINS) {
    const v = q[k];
    if (v === undefined || v === "") continue;
    if (typeof v !== "string") return { error: `${k} must be a single value` };
    scope[k] = norm(v);
  }
  const g = q.groupBy;
  if (g === undefined || g === "") return { scope, groupBy: null };
  if (typeof g !== "string" || !(ENTITY_GRAINS as readonly string[]).includes(g)) {
    return { error: "groupBy must be account|mailbox|domain" };
  }
  return { scope, groupBy: g as EntityGrain };
}

/** The mailbox an address belongs to: its login, or itself. */
export function mailboxOf(address: string, mailboxLogin: string | null | undefined): string {
  return norm(mailboxLogin && mailboxLogin.trim() !== "" ? mailboxLogin : address);
}

export function domainOf(address: string): string {
  return norm(address.split("@")[1] ?? "");
}

/** The key of an address at one grain. Pure. */
export function entityKey(grain: EntityGrain, address: string, mailboxLogin: string | null | undefined): string {
  if (grain === "account") return norm(address);
  if (grain === "mailbox") return mailboxOf(address, mailboxLogin);
  return domainOf(address);
}

/** Does this address fall inside the scope? Pure. */
export function inScope(scope: EntityScope, address: string, mailboxLogin: string | null | undefined): boolean {
  if (scope.account && norm(address) !== scope.account) return false;
  if (scope.mailbox && mailboxOf(address, mailboxLogin) !== scope.mailbox) return false;
  if (scope.domain && domainOf(address) !== scope.domain) return false;
  return true;
}

export function scopeIsFleet(scope: EntityScope): boolean {
  return !scope.account && !scope.mailbox && !scope.domain;
}

/** SQL key expression at one grain over an (address, mailbox login) column pair. */
export function entityKeySql(grain: EntityGrain, addressCol: SQL, loginCol: SQL): SQL {
  if (grain === "account") return sql`lower(${addressCol})`;
  if (grain === "mailbox") return sql`lower(COALESCE(NULLIF(${loginCol}, ''), ${addressCol}))`;
  return sql`lower(split_part(${addressCol}, '@', 2))`;
}

/** SQL predicate of a scope over an (address, mailbox login) column pair. */
export function entityScopeSql(scope: EntityScope, addressCol: SQL, loginCol: SQL): SQL {
  const clauses: SQL[] = [sql`true`];
  if (scope.account) clauses.push(sql`${entityKeySql("account", addressCol, loginCol)} = ${scope.account}`);
  if (scope.mailbox) clauses.push(sql`${entityKeySql("mailbox", addressCol, loginCol)} = ${scope.mailbox}`);
  if (scope.domain) clauses.push(sql`${entityKeySql("domain", addressCol, loginCol)} = ${scope.domain}`);
  return sql.join(clauses, sql` AND `);
}
