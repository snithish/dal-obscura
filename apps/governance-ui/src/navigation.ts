export type UiPage = "assets" | "changes" | "activity" | "connections" | "settings";

const pages: readonly UiPage[] = ["assets", "changes", "activity", "connections", "settings"];

export function pageFromHash(hash: string): UiPage {
  const value = hash.replace(/^#/, "").split(/[?&]/, 1)[0] as UiPage;
  return pages.includes(value) ? value : "assets";
}
