export type Asset = {
  id: string;
  name: string;
  catalog: string;
  backend: string;
  table_identifier: string | null;
  owner_count: number;
  owners: string[];
  policy_status: string;
  draft_status: string;
};

export type AssetOptionRow = {
  key: string;
  value: string;
};

export type OwnerRow = {
  principal: string;
};

export type AssetFilters = {
  catalog: string;
  owner: string;
  search: string;
  status: string;
};

export type AssetOptionsResult =
  | { ok: true; options: Record<string, unknown> }
  | { ok: false; error: string };

export function ownersFromRows(rows: OwnerRow[]): string[] {
  const seen = new Set<string>();
  const owners: string[] = [];
  for (const row of rows) {
    const owner = row.principal.trim();
    if (owner && !seen.has(owner)) {
      owners.push(owner);
      seen.add(owner);
    }
  }
  return owners;
}

export function assetOptionsFromForm(
  snapshot: string,
  rows: AssetOptionRow[],
): AssetOptionsResult {
  const trimmedSnapshot = snapshot.trim();
  if (trimmedSnapshot) {
    const snapshotNumber = Number(trimmedSnapshot);
    if (!Number.isFinite(snapshotNumber)) {
      return { ok: false, error: "Snapshot ID must be a number." };
    }
  }

  return {
    ok: true,
    options: {
      ...(trimmedSnapshot ? { snapshot: Number(trimmedSnapshot) } : {}),
      ...Object.fromEntries(
        rows
          .filter((row) => row.key.trim())
          .map((row) => [row.key.trim(), parseSimpleValue(row.value)]),
      ),
    },
  };
}

export function filterAssets(assets: Asset[], filters: AssetFilters): Asset[] {
  const search = filters.search.trim().toLowerCase();
  return assets.filter((asset) => {
    const matchesSearch =
      !search ||
      [asset.name, asset.catalog, asset.backend, asset.table_identifier ?? ""].some((value) =>
        value.toLowerCase().includes(search),
      );
    const matchesCatalog = !filters.catalog || asset.catalog === filters.catalog;
    const matchesOwner =
      !filters.owner ||
      (filters.owner === "owned" ? asset.owner_count > 0 : asset.owner_count === 0);
    const matchesStatus = !filters.status || asset.policy_status === filters.status;
    return matchesSearch && matchesCatalog && matchesOwner && matchesStatus;
  });
}

function parseSimpleValue(value: string): string | number | boolean {
  const trimmed = value.trim();
  if (trimmed === "true") {
    return true;
  }
  if (trimmed === "false") {
    return false;
  }
  const numeric = Number(trimmed);
  return trimmed !== "" && Number.isFinite(numeric) ? numeric : trimmed;
}
