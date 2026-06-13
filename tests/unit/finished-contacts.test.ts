import { describe, it, expect, afterEach } from "vitest";
import {
  FINISHED_INSTANTLY_STATUSES,
  LOCAL_TERMINAL_STATUSES,
  isDeleteFinishedEnabled,
  parseInstantlyStatus,
  isFinishedInstantlyStatus,
  localTerminalStatus,
  isLocallyTerminal,
  isLeadAlreadyGone,
} from "../../src/lib/finished-contacts";

describe("finished-contacts decision helpers", () => {
  const prev = process.env.DELETE_FINISHED_CONTACTS_ENABLED;
  afterEach(() => {
    if (prev === undefined) delete process.env.DELETE_FINISHED_CONTACTS_ENABLED;
    else process.env.DELETE_FINISHED_CONTACTS_ENABLED = prev;
  });

  it("FINISHED_INSTANTLY_STATUSES is exactly {2,3}", () => {
    expect([...FINISHED_INSTANTLY_STATUSES].sort()).toEqual([2, 3]);
  });

  it("LOCAL_TERMINAL_STATUSES is exactly {paused, completed}", () => {
    expect([...LOCAL_TERMINAL_STATUSES].sort()).toEqual(["completed", "paused"]);
  });

  describe("isDeleteFinishedEnabled — kill-switch default OFF", () => {
    it("true only for the exact string 'true'", () => {
      process.env.DELETE_FINISHED_CONTACTS_ENABLED = "true";
      expect(isDeleteFinishedEnabled()).toBe(true);
    });
    it("false when unset", () => {
      delete process.env.DELETE_FINISHED_CONTACTS_ENABLED;
      expect(isDeleteFinishedEnabled()).toBe(false);
    });
    it("false for any other value (typo never enables)", () => {
      for (const v of ["TRUE", "1", "yes", "on", "false", ""]) {
        process.env.DELETE_FINISHED_CONTACTS_ENABLED = v;
        expect(isDeleteFinishedEnabled()).toBe(false);
      }
    });
  });

  describe("parseInstantlyStatus", () => {
    it("passes through numbers", () => {
      expect(parseInstantlyStatus(1)).toBe(1);
      expect(parseInstantlyStatus(3)).toBe(3);
    });
    it("coerces numeric strings", () => {
      expect(parseInstantlyStatus("2")).toBe(2);
    });
    it("returns null for null/undefined (NOT 0)", () => {
      expect(parseInstantlyStatus(null)).toBeNull();
      expect(parseInstantlyStatus(undefined)).toBeNull();
    });
    it("returns null for non-numeric", () => {
      expect(parseInstantlyStatus("active")).toBeNull();
      expect(parseInstantlyStatus({})).toBeNull();
    });
  });

  describe("isFinishedInstantlyStatus", () => {
    it("true for paused (2) and completed (3)", () => {
      expect(isFinishedInstantlyStatus(2)).toBe(true);
      expect(isFinishedInstantlyStatus(3)).toBe(true);
    });
    it("false for active (1) and null", () => {
      expect(isFinishedInstantlyStatus(1)).toBe(false);
      expect(isFinishedInstantlyStatus(null)).toBe(false);
    });
  });

  describe("localTerminalStatus", () => {
    it("3 → completed, 2 → paused", () => {
      expect(localTerminalStatus(3)).toBe("completed");
      expect(localTerminalStatus(2)).toBe("paused");
    });
  });

  describe("isLocallyTerminal", () => {
    it("true for paused/completed, false for active", () => {
      expect(isLocallyTerminal("paused")).toBe(true);
      expect(isLocallyTerminal("completed")).toBe(true);
      expect(isLocallyTerminal("active")).toBe(false);
    });
  });

  describe("isLeadAlreadyGone", () => {
    it("true for an instantly 404 error message", () => {
      expect(
        isLeadAlreadyGone("instantly-api DELETE /leads failed: 404 - not found"),
      ).toBe(true);
    });
    it("false for other status codes", () => {
      expect(isLeadAlreadyGone("instantly-api DELETE /leads failed: 500 - boom")).toBe(false);
      expect(isLeadAlreadyGone("instantly-api DELETE /leads failed: 400 - bad")).toBe(false);
    });
  });
});
