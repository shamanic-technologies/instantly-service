import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockGetCurrentGoals = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/brand-client", () => ({
  getCurrentGoals: (...args: unknown[]) => mockGetCurrentGoals(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

import {
  isStopOnClickEnabled,
  anyGoalIsSignup,
  maybeStopOnClickForSignup,
} from "../../src/lib/stop-on-click";

const campaign = {
  instantlyCampaignId: "inst-camp-1",
  orgId: "org-1",
  brandIds: ["brand-1"],
};

describe("stop-on-click pure helpers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("isStopOnClickEnabled: true only for exactly 'true'", () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");
    expect(isStopOnClickEnabled()).toBe(true);
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "false");
    expect(isStopOnClickEnabled()).toBe(false);
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "");
    expect(isStopOnClickEnabled()).toBe(false);
  });

  it("anyGoalIsSignup: ANY-brand semantics", () => {
    expect(anyGoalIsSignup(["signup"])).toBe(true);
    expect(anyGoalIsSignup(["purchase"])).toBe(false);
    expect(anyGoalIsSignup(["purchase", "signup"])).toBe(true);
    expect(anyGoalIsSignup([])).toBe(false);
  });
});

describe("maybeStopOnClickForSignup", () => {
  beforeEach(() => {
    mockGetCurrentGoals.mockReset();
    mockResolveInstantlyApiKey.mockReset();
    mockUpdateCampaignStatus.mockReset();
    mockResolveInstantlyApiKey.mockResolvedValue({ key: "api-key-1", keySource: "platform" });
    mockUpdateCampaignStatus.mockResolvedValue({});
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("flag OFF: no goal fetch, no pause", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "false");
    await maybeStopOnClickForSignup(campaign, "lead@x.com");
    expect(mockGetCurrentGoals).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("flag ON + brand goal signup: pauses the Instantly campaign", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");
    mockGetCurrentGoals.mockResolvedValue(["signup"]);

    await maybeStopOnClickForSignup(campaign, "lead@x.com");

    expect(mockUpdateCampaignStatus).toHaveBeenCalledTimes(1);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("api-key-1", "inst-camp-1", "paused");
  });

  it("flag ON + brand goal purchase: no pause", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");
    mockGetCurrentGoals.mockResolvedValue(["purchase"]);

    await maybeStopOnClickForSignup(campaign, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("flag ON + multi-brand, one signup: pauses (ANY)", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");
    mockGetCurrentGoals.mockResolvedValue(["purchase", "signup"]);

    await maybeStopOnClickForSignup(
      { ...campaign, brandIds: ["brand-1", "brand-2"] },
      "lead@x.com",
    );

    expect(mockUpdateCampaignStatus).toHaveBeenCalledTimes(1);
  });

  it("flag ON + brand-service throws: no pause, no throw (fail-soft)", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");
    mockGetCurrentGoals.mockRejectedValue(new Error("brand-service down"));

    await expect(maybeStopOnClickForSignup(campaign, "lead@x.com")).resolves.toBeUndefined();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("flag ON + no brandIds: no pause", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");

    await maybeStopOnClickForSignup({ ...campaign, brandIds: [] }, "lead@x.com");

    expect(mockGetCurrentGoals).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("flag ON + null orgId: no pause", async () => {
    vi.stubEnv("STOP_ON_CLICK_ON_SIGNUP_ENABLED", "true");

    await maybeStopOnClickForSignup({ ...campaign, orgId: null }, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });
});
