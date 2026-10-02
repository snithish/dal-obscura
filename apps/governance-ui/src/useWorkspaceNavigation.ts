import { useEffect, useRef, useState } from "react";
import {
  locationFromUrl,
  type AssetTab,
  type UiLocation,
  type UiPage,
} from "./navigation";

type NavigationOptions = {
  authenticated: boolean;
  dirty: boolean;
  confirmLeaving: () => Promise<boolean>;
  onLeave: () => void;
  onAssetSelected: (id: string) => void;
};

function locationUrl(location: UiLocation): string {
  const params = new URLSearchParams();
  if (location.assetId) params.set("asset", location.assetId);
  if (location.tab) params.set("tab", location.tab);
  return `${window.location.pathname}${params.size ? `?${params}` : ""}#${location.page}`;
}

/** One atomic location owns browser history and the unsaved-navigation guard. */
export function useWorkspaceNavigation(options: NavigationOptions) {
  const [location, setLocation] = useState(() =>
    locationFromUrl(window.location.hash, window.location.search),
  );
  const current = useRef(location);
  const callbacks = useRef(options);
  const pending = useRef(false);
  callbacks.current = options;

  function commit(next: UiLocation) {
    current.current = next;
    setLocation(next);
  }

  async function change(next: UiLocation, fromHistory = false) {
    if (pending.current) return;
    const previous = current.current;
    const leaving =
      next.page !== previous.page || next.assetId !== previous.assetId;
    if (!leaving && next.tab === previous.tab) return;
    pending.current = true;
    const approved =
      !callbacks.current.dirty || (await callbacks.current.confirmLeaving());
    pending.current = false;
    if (!approved) {
      if (fromHistory)
        window.history.pushState(null, "", locationUrl(previous));
      return;
    }
    if (leaving) {
      callbacks.current.onLeave();
      window.scrollTo(0, 0);
    }
    commit(next);
    if (!fromHistory) window.history.pushState(null, "", locationUrl(next));
    if (
      leaving &&
      next.page === "assets" &&
      next.assetId &&
      callbacks.current.authenticated
    ) {
      callbacks.current.onAssetSelected(next.assetId);
    }
  }

  useEffect(() => {
    const sync = () =>
      void change(
        locationFromUrl(window.location.hash, window.location.search),
        true,
      );
    window.addEventListener("hashchange", sync);
    window.addEventListener("popstate", sync);
    return () => {
      window.removeEventListener("hashchange", sync);
      window.removeEventListener("popstate", sync);
    };
  }, []);

  return {
    ...location,
    navigate: (page: UiPage) => change({ page }),
    openAsset: (assetId: string) => change({ page: "assets", assetId }),
    updateTab: (tab: AssetTab) => commit({ ...current.current, tab }),
    redirectToAssets: () => {
      commit({ ...current.current, page: "assets" });
      window.history.replaceState(null, "", locationUrl(current.current));
    },
  };
}
