import { Button, Group, Table, Text, TextInput } from "@mantine/core";
import type { Asset } from "../api";

export type AssetInventoryProps = {
  assets: Asset[];
  selectedId?: string;
  search: string;
  loading: boolean;
  hasMore: boolean;
  locked?: boolean;
  onSearch: (value: string) => void;
  onSelect: (id: string) => void;
  onLoadMore: () => void;
};

export function AssetInventory(props: AssetInventoryProps) {
  return <section className="asset-inventory" aria-label="Governed asset inventory">
    <Group justify="space-between" align="end">
      <TextInput label="Find governed asset" type="search" value={props.search}
        placeholder="Search catalog or asset" disabled={props.locked}
        onChange={(event) => props.onSearch(event.currentTarget.value)} />
      <Text size="sm" c="dimmed" role="status">{props.loading ? "Updating inventory…" : `${props.assets.length} assets loaded`}</Text>
    </Group>
    {props.locked && <Text size="sm" c="dimmed">Review link is pinned to this asset and draft. Open Assets to leave this review.</Text>}
    {props.assets.length ? <div className="asset-inventory-table" role="region" aria-label="Governed assets table" tabIndex={0}>
      <Table highlightOnHover>
        <Table.Thead><Table.Tr><Table.Th>Asset</Table.Th><Table.Th>Catalog / format</Table.Th><Table.Th>Owners</Table.Th><Table.Th>Active policy</Table.Th><Table.Th>Last published</Table.Th></Table.Tr></Table.Thead>
        <Table.Tbody>{props.assets.map((asset) => <Table.Tr key={asset.id} data-selected={asset.id === props.selectedId || undefined}>
          <Table.Td><Button variant="subtle" disabled={props.locked} aria-current={asset.id === props.selectedId ? "true" : undefined}
            onClick={() => props.onSelect(asset.id)}>{asset.name}</Button></Table.Td>
          <Table.Td>{asset.catalog} / {asset.backend}</Table.Td>
          <Table.Td>{asset.owners.length ? asset.owners.join(", ") : "Not provided"}</Table.Td>
          <Table.Td>{asset.active_policy_version != null ? `v${asset.active_policy_version}` : "Not published"}</Table.Td>
          <Table.Td>{asset.last_published_at ? new Date(asset.last_published_at).toLocaleString() : "Never published"}</Table.Td>
        </Table.Tr>)}</Table.Tbody>
      </Table>
    </div> : <Text role="status">{props.search ? "No governed assets match this search. Clear the search to browse again." : "No governed assets are available to your account."}</Text>}
    {props.hasMore && <Button variant="default" disabled={props.locked || props.loading} onClick={props.onLoadMore}>Load more assets</Button>}
  </section>;
}
