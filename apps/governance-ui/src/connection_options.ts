export type SecretReference = { secret: string; scope: string };

/**
 * Keeps a server-held secret reference when an edit form leaves the field blank.
 * The browser may round-trip the reference name and scope, but never a secret
 * value or an unstructured redaction marker.
 */
export function preserveSecretReference(value: unknown): SecretReference | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (Object.keys(record).length !== 2 || typeof record.secret !== "string" || typeof record.scope !== "string") {
    return undefined;
  }
  const secret = record.secret.trim();
  const scope = record.scope.trim();
  if (!secret || !scope) return undefined;
  return { secret, scope };
}
