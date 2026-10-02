import type { DeferredResponse } from "./async";

export type AuthenticatedApiOptions = {
  admin?: boolean;
  readOnly?: boolean;
  conflictSave?: boolean;
  configuredSettings?: boolean;
  configuredConnections?: boolean;
  configuredLifecycle?: boolean;
  configuredAudit?: boolean;
  multipleFormats?: boolean;
  initialPolicyRules?: Array<Record<string, unknown>>;
  ruleEditor?: boolean;
  deferredAudit?: DeferredResponse;
  deferredSettings?: DeferredResponse;
  deferredConnections?: DeferredResponse;
  deferredDiscovery?: DeferredResponse;
  deferredInventory?: DeferredResponse;
  deferredSave?: DeferredResponse;
  deferredEvaluate?: DeferredResponse;
};
