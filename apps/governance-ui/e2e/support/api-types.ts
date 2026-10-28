import type { components } from "../../src/generated/control_plane";
export type ApiSchemas = components["schemas"];
export type FixtureRule = Omit<ApiSchemas["PolicyRuleSchema"], "name" | "description"> &
  Partial<Pick<ApiSchemas["PolicyRuleSchema"], "name" | "description">>;
import type { DeferredResponse } from "./async";

export type AuthenticatedApiOptions = {
  admin?: boolean;
  readOnly?: boolean;
  conflictSave?: boolean;
  configuredSettings?: boolean;
  configuredConnections?: boolean;
  configuredAudit?: boolean;
  multipleFormats?: boolean;
  initialPolicyRules?: FixtureRule[];
  ruleEditor?: boolean;
  deferredAudit?: DeferredResponse;
  deferredSettings?: DeferredResponse;
  deferredConnections?: DeferredResponse;
  deferredDiscovery?: DeferredResponse;
  deferredInventory?: DeferredResponse;
  deferredSave?: DeferredResponse;
  deferredEvaluate?: DeferredResponse;
};
