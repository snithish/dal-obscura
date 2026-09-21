export type UiPage = "assets" | "changes" | "activity" | "connections" | "settings";
export type AssetTab = "policy" | "tests" | "history" | "access" | "consumers";
export type UiLocation = {
  page: UiPage;
  assetId?: string;
  draftId?: string;
  draftRevision?: number;
  tab?: AssetTab;
  version?: number;
};

const pages: readonly UiPage[] = ["assets", "changes", "activity", "connections", "settings"];
const assetTabs: readonly AssetTab[] = ["policy", "tests", "history", "access", "consumers"];

export function pageFromHash(hash: string): UiPage {
  const value = hash.replace(/^#/, "").split(/[?&]/, 1)[0] as UiPage;
  return pages.includes(value) ? value : "assets";
}

export function locationFromUrl(hash: string, search: string): UiLocation {
  const params = new URLSearchParams(search);
  const rawTab = params.get("tab");
  const rawVersion = params.get("version");
  const parsedVersion = rawVersion && /^\d+$/.test(rawVersion) ? Number(rawVersion) : undefined;
  const location: UiLocation = { page: pageFromHash(hash) };
  const assetId = params.get("asset");
  const draftId = params.get("draft");
  const rawDraftRevision = params.get("draft_revision");
  const parsedDraftRevision = rawDraftRevision && /^\d+$/.test(rawDraftRevision) ? Number(rawDraftRevision) : undefined;
  if (assetId) location.assetId = assetId;
  if (draftId) location.draftId = draftId;
  if (parsedDraftRevision !== undefined && parsedDraftRevision > 0) location.draftRevision = parsedDraftRevision;
  if (rawTab && assetTabs.includes(rawTab as AssetTab)) location.tab = rawTab as AssetTab;
  if (parsedVersion && parsedVersion > 0) location.version = parsedVersion;
  return location;
}
