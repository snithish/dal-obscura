import type { Mask } from "../../data/api";

/** Illustrative scalar masks only; the server owns typed evaluation and authorization. */
export async function simulateMask(
  value: string | null,
  mask: Mask | undefined,
): Promise<string | number | boolean | null> {
  if (mask?.type === "default") return mask.value ?? null;
  if (value === null || mask?.type === "null") return null;
  if (!mask) return value;
  if (mask.type === "redact")
    return typeof mask.value === "string" ? mask.value : "Invalid replacement";
  if (mask.type === "hash") {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
      "",
    );
  }
  if (mask.type === "email")
    return /^[^@]+@[^@]+$/.test(value) ? value.replace(/(^.)[^@]*(@[^@]+)$/u, "$1***$2") : null;
  const keep = mask.value;
  if (typeof keep !== "number" || !Number.isInteger(keep) || keep < 0)
    return "Invalid keep_last value";
  const characters = Array.from(value);
  return characters.length <= keep
    ? value
    : "*".repeat(Math.max(characters.length - keep, 0)) +
        (keep ? characters.slice(-keep).join("") : "");
}
