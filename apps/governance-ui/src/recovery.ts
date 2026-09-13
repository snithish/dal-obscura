export type RecoveryFailure = {
  status?: number;
  requestId?: string;
};

const statusMessages: Record<number, string> = {
  403: "Your account is authenticated but is not allowed to perform this action.",
  404: "The requested resource is no longer available. Refresh the workspace and choose it again.",
  409: "This resource changed on the server. Refresh it before retrying.",
  422: "The server rejected the submitted values. Correct the highlighted policy or configuration fields.",
  429: "Too many requests were made. Wait a moment and retry.",
  503: "The control plane is temporarily unavailable. Your current local state remains unchanged.",
};

export function recoveryMessage(error: unknown, fallback: string): string {
  const failure = error as RecoveryFailure | null;
  const message = failure?.status !== undefined ? statusMessages[failure.status] : undefined;
  const result = message ?? fallback;
  return failure?.requestId ? `${result} Request ID: ${failure.requestId}` : result;
}
