export type RecoveryFailure = {
  status?: number;
  code?: string;
  message?: string;
  requestId?: string;
  fieldErrors?: Array<{ field: string; message: string; type: string }>;
};

const statusMessages: Record<number, string> = {
  403: "Your account is authenticated but is not allowed to perform this action.",
  404: "The requested resource is no longer available. Refresh the workspace and choose it again.",
  409: "This resource changed on the server. Refresh it before retrying.",
  422: "The server rejected the submitted values. Check the validation details and retry.",
  428: "A current revision is required. Reload this resource before saving.",
  429: "Too many requests were made. Wait a moment and retry.",
  503: "The control plane is temporarily unavailable. Your current local state remains unchanged.",
};

export function recoveryMessage(error: unknown, fallback: string): string {
  const failure = error as RecoveryFailure | null;
  const message = failure?.code === "auth_challenge"
    ? "The browser or edge session expired. Sign in again to continue."
    : failure?.status !== undefined ? statusMessages[failure.status] : undefined;
  const result = message ?? (failure?.code === "validation_error" ? failure.message : undefined) ?? fallback;
  const fields = failure?.fieldErrors?.map((item) => `${displayErrorField(item.field)}: ${item.message}`) ?? [];
  const withFields = fields.length ? `${result} ${fields.join(" ")}` : result;
  return failure?.requestId ? `${withFields} Request ID: ${failure.requestId}` : withFields;
}

export function displayErrorField(field: string): string {
  return field.replace(/^rules\.(\d+)(\.|$)/, (_, index, separator) => `Rule ${Number(index) + 1}${separator ? " · " : ""}`);
}
