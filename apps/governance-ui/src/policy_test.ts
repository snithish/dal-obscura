export function parseTestClaims(text: string): { claims: Record<string, unknown>; error?: string } {
  if (!text.trim()) return { claims: {} };
  try {
    const value: unknown = JSON.parse(text);
    if (!value || Array.isArray(value) || typeof value !== "object") {
      return { claims: {}, error: "Claims must be a JSON object." };
    }
    return { claims: value as Record<string, unknown> };
  } catch {
    return { claims: {}, error: 'Enter valid JSON, for example {"region":"us"}.' };
  }
}
