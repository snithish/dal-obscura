import "./inventory.css";
import { useMemo, useState } from "react";
import { AssetLastChange } from "./InventoryInsights";
import { Button, NativeSelect, Table, Text, TextInput } from "@mantine/core";
import type { Asset } from "../../data/api";
import { Icon } from "../../shared/Icon";

export type AssetInventoryProps = {
  assets: Asset[];
  search: string;
  loading: boolean;
  hasMore: boolean;
  onSearch: (value: string) => void;
  onSelect: (id: string) => void;
  onLoadMore: () => void;
};

export function AssetInventory(props: AssetInventoryProps) {
  const [catalog, setCatalog] = useState("");
  const [policy, setPolicy] = useState("");
  const catalogs = useMemo(
    () =>
      [
        ...new Set([...props.assets.map((asset) => asset.catalog), ...(catalog ? [catalog] : [])]),
      ].sort(),
    [props.assets, catalog],
  );
  const filtered = useMemo(
    () =>
      props.assets.filter(
        (asset) =>
          (!catalog || asset.catalog === catalog) && (!policy || asset.policy_status === policy),
      ),
    [props.assets, catalog, policy],
  );
  const hasFilters = Boolean(catalog || policy);
  return (
    <section className="asset-inventory" aria-label="Governed asset inventory">
      <div className="inventory-toolbar">
        <div className="inventory-toolbar-title">
          <h2>Governed assets</h2>
          <Text size="sm" c="dimmed" role="status">
            {props.loading
              ? "Updating inventory…"
              : hasFilters
                ? `${filtered.length} of ${props.assets.length} loaded assets shown`
                : `${props.assets.length} ${props.assets.length === 1 ? "asset" : "assets"} loaded`}
          </Text>
        </div>
        <TextInput
          label="Find governed asset"
          type="search"
          value={props.search}
          placeholder="Search catalog or asset"
          onChange={(event) => props.onSearch(event.currentTarget.value)}
          leftSection={<Icon name="search" size={15} />}
        />
      </div>
      <div className="inventory-filters">
        <NativeSelect
          label="Catalog for loaded assets"
          value={catalog}
          onChange={(event) => setCatalog(event.currentTarget.value)}
          data={[
            { value: "", label: "All loaded catalogs" },
            ...catalogs.map((value) => ({ value, label: value })),
          ]}
        />
        <NativeSelect
          label="Policy for loaded assets"
          value={policy}
          onChange={(event) => setPolicy(event.currentTarget.value)}
          data={[
            { value: "", label: "All loaded policies" },
            { value: "configured", label: "Configured" },
            { value: "missing", label: "Default NULL masks" },
          ]}
        />
        {hasFilters && (
          <Button
            variant="subtle"
            onClick={() => {
              setCatalog("");
              setPolicy("");
            }}
          >
            Clear loaded-asset filters
          </Button>
        )}
        <p className="help">
          Filters apply only to loaded assets.{" "}
          {props.hasMore
            ? "Load more assets to extend these results."
            : "All matching pages have been loaded."}
        </p>
      </div>
      {filtered.length ? (
        <div
          className="asset-inventory-table"
          role="region"
          aria-label="Governed assets table"
          tabIndex={0}
        >
          <Table highlightOnHover>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Asset</Table.Th>
                <Table.Th>Catalog / format</Table.Th>
                <Table.Th>Owners</Table.Th>
                <Table.Th>Policy</Table.Th>
                <Table.Th className="inventory-revision">Last policy change</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {filtered.map((asset) => (
                <Table.Tr key={asset.id}>
                  <Table.Td>
                    <button
                      type="button"
                      className="inventory-asset-link"
                      onClick={() => props.onSelect(asset.id)}
                    >
                      {asset.name}
                    </button>
                  </Table.Td>
                  <Table.Td>
                    <div className="inventory-catalog">
                      <span>{asset.catalog}</span>
                      <span className="context-tag">{asset.backend}</span>
                    </div>
                  </Table.Td>
                  <Table.Td>
                    {asset.owners.length ? (
                      <ul className="inventory-owner-details">
                        {asset.owners.map((owner) => (
                          <li key={owner}>
                            {owner.includes("|") ? (
                              <details className="inventory-principal">
                                <summary>
                                  <code>{owner.slice(owner.indexOf("|") + 1) || owner}</code>
                                </summary>
                                <code>{owner}</code>
                              </details>
                            ) : (
                              <code>{owner}</code>
                            )}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <span className="muted">Not provided</span>
                    )}
                  </Table.Td>
                  <Table.Td>
                    <span
                      className={`policy-status-tag ${asset.policy_status === "configured" ? "configured" : "default"}`}
                    >
                      <Icon
                        name={asset.policy_status === "configured" ? "shield-check" : "key-round"}
                        size={13}
                      />
                      {asset.policy_status === "configured" ? "Configured" : "Default NULL masks"}
                    </span>
                  </Table.Td>
                  <Table.Td className="inventory-revision">
                    <AssetLastChange assetId={asset.id} revision={asset.policy_revision ?? 0} />
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </div>
      ) : (
        <Text role="status">
          {hasFilters
            ? "No loaded assets match these filters."
            : props.search
              ? "No governed assets match this search. Clear the search to browse again."
              : "No governed assets are available to your account."}
        </Text>
      )}
      {props.hasMore && (
        <Button
          variant="default"
          disabled={props.loading}
          onClick={props.onLoadMore}
          leftSection={<Icon name="chevron-down" size={16} />}
        >
          Load more assets
        </Button>
      )}
    </section>
  );
}
