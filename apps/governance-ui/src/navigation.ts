export type UiPage = "assets" | "activity" | "connections" | "settings";
export type AssetTab = "policy" | "tests" | "access" | "consumers";
export type UiLocation = {
  page: UiPage;
  assetId?: string;
  tab?: AssetTab;
};

const pages: readonly UiPage[] = ["assets", "activity", "connections", "settings"];
const assetTabs: readonly AssetTab[] = ["policy", "tests", "access", "consumers"];

export function pageFromHash(hash: string): UiPage {
  const value = hash.replace(/^#/, "").split(/[?&]/, 1)[0] as UiPage;
  return pages.includes(value) ? value : "assets";
}

export function locationFromUrl(hash: string, search: string): UiLocation {
  const params = new URLSearchParams(search);
  const rawTab = params.get("tab");
  const location: UiLocation = { page: pageFromHash(hash) };
  const assetId = params.get("asset");
  if (assetId) location.assetId = assetId;
  if (rawTab && assetTabs.includes(rawTab as AssetTab)) location.tab = rawTab as AssetTab;
  return location;
}
