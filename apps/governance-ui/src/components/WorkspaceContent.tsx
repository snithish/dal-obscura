import type { ReactNode } from "react";
import type { UiPage } from "../navigation";

export type WorkspaceContentProps = {
  signedOut: boolean;
  page: UiPage;
  workspace: "loading" | "ready" | "unavailable";
  accessView: ReactNode;
  managementView: ReactNode;
  noAssetsView: ReactNode;
  assetView?: ReactNode;
};

/** Chooses the current route view without mixing authentication and asset state. */
export function WorkspaceContent({
  signedOut,
  page,
  workspace,
  accessView,
  managementView,
  noAssetsView,
  assetView,
}: WorkspaceContentProps) {
  if (signedOut) return <>{accessView}</>;
  if (page !== "assets") return <>{managementView}</>;
  if (workspace === "loading") return <>{accessView}</>;
  return <>{assetView ?? noAssetsView}</>;
}
