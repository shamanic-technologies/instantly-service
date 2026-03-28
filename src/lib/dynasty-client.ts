interface DynastyEntry {
  dynastySlug: string;
  slugs: string[];
}

/**
 * Resolve a feature dynasty slug into its list of versioned slugs.
 * Returns empty array if the dynasty is unknown.
 */
export async function resolveFeatureDynastySlugs(
  dynastySlug: string,
  headers?: Record<string, string>,
): Promise<string[]> {
  const baseUrl = process.env.FEATURES_SERVICE_URL;
  const apiKey = process.env.FEATURES_SERVICE_API_KEY;
  if (!baseUrl || !apiKey) {
    console.warn("[instantly-service] FEATURES_SERVICE_URL or FEATURES_SERVICE_API_KEY not configured");
    return [];
  }
  const url = `${baseUrl}/features/dynasty/slugs?dynastySlug=${encodeURIComponent(dynastySlug)}`;
  const res = await fetch(url, {
    headers: { "X-API-Key": apiKey, ...headers },
  });
  if (!res.ok) {
    console.warn(`[instantly-service] Failed to resolve feature dynasty slug ${dynastySlug}: ${res.status}`);
    return [];
  }
  const data = (await res.json()) as { slugs: string[] };
  return data.slugs ?? [];
}

/**
 * Resolve a workflow dynasty slug into its list of versioned slugs.
 * Returns empty array if the dynasty is unknown.
 */
export async function resolveWorkflowDynastySlugs(
  dynastySlug: string,
  headers?: Record<string, string>,
): Promise<string[]> {
  const baseUrl = process.env.WORKFLOW_SERVICE_URL;
  const apiKey = process.env.WORKFLOW_SERVICE_API_KEY;
  if (!baseUrl || !apiKey) {
    console.warn("[instantly-service] WORKFLOW_SERVICE_URL or WORKFLOW_SERVICE_API_KEY not configured");
    return [];
  }
  const url = `${baseUrl}/workflows/dynasty/slugs?dynastySlug=${encodeURIComponent(dynastySlug)}`;
  const res = await fetch(url, {
    headers: { "X-API-Key": apiKey, ...headers },
  });
  if (!res.ok) {
    console.warn(`[instantly-service] Failed to resolve workflow dynasty slug ${dynastySlug}: ${res.status}`);
    return [];
  }
  const data = (await res.json()) as { slugs: string[] };
  return data.slugs ?? [];
}

/**
 * Fetch all feature dynasties.
 */
export async function fetchFeatureDynasties(
  headers?: Record<string, string>,
): Promise<DynastyEntry[]> {
  const baseUrl = process.env.FEATURES_SERVICE_URL;
  const apiKey = process.env.FEATURES_SERVICE_API_KEY;
  if (!baseUrl || !apiKey) {
    console.warn("[instantly-service] FEATURES_SERVICE_URL or FEATURES_SERVICE_API_KEY not configured");
    return [];
  }
  const url = `${baseUrl}/features/dynasties`;
  const res = await fetch(url, {
    headers: { "X-API-Key": apiKey, ...headers },
  });
  if (!res.ok) {
    console.warn(`[instantly-service] Failed to fetch feature dynasties: ${res.status}`);
    return [];
  }
  const data = (await res.json()) as { dynasties: DynastyEntry[] };
  return data.dynasties ?? [];
}

/**
 * Fetch all workflow dynasties.
 */
export async function fetchWorkflowDynasties(
  headers?: Record<string, string>,
): Promise<DynastyEntry[]> {
  const baseUrl = process.env.WORKFLOW_SERVICE_URL;
  const apiKey = process.env.WORKFLOW_SERVICE_API_KEY;
  if (!baseUrl || !apiKey) {
    console.warn("[instantly-service] WORKFLOW_SERVICE_URL or WORKFLOW_SERVICE_API_KEY not configured");
    return [];
  }
  const url = `${baseUrl}/workflows/dynasties`;
  const res = await fetch(url, {
    headers: { "X-API-Key": apiKey, ...headers },
  });
  if (!res.ok) {
    console.warn(`[instantly-service] Failed to fetch workflow dynasties: ${res.status}`);
    return [];
  }
  const data = (await res.json()) as { dynasties: DynastyEntry[] };
  return data.dynasties ?? [];
}

/**
 * Build a reverse map: versioned slug → dynasty slug.
 * Slugs not in any dynasty fall back to their raw value at query time.
 */
export function buildSlugToDynastyMap(
  dynasties: DynastyEntry[],
): Map<string, string> {
  const map = new Map<string, string>();
  for (const d of dynasties) {
    for (const slug of d.slugs) {
      map.set(slug, d.dynastySlug);
    }
  }
  return map;
}
