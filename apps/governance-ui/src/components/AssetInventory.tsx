import { Button, Group, Table, Text, TextInput } from "@mantine/core";
import type { Asset } from "../api";
import { Icon } from "./Icon";

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
  return <section className="asset-inventory" aria-label="Governed asset inventory">
    <Group justify="space-between" align="end">
      <TextInput label="Find governed asset" type="search" value={props.search}
        placeholder="Search catalog or asset"
        onChange={(event) => props.onSearch(event.currentTarget.value)}
        leftSection={<Icon name="search" size={15} />} />
      <Text size="sm" c="dimmed" role="status">{props.loading ? "Updating inventory…" : `${props.assets.length} ${props.assets.length === 1 ? "asset" : "assets"} loaded`}</Text>
    </Group>
    {props.assets.length ? <div className="asset-inventory-table" role="region" aria-label="Governed assets table" tabIndex={0}>
      <Table highlightOnHover>
        <Table.Thead><Table.Tr><Table.Th>Asset</Table.Th><Table.Th>Catalog / format</Table.Th><Table.Th>Owners</Table.Th><Table.Th>Policy</Table.Th><Table.Th>Revision</Table.Th></Table.Tr></Table.Thead>
        <Table.Tbody>{props.assets.map((asset) => <Table.Tr key={asset.id}>
          <Table.Td><Button variant="subtle"
            onClick={() => props.onSelect(asset.id)}>{asset.name}</Button></Table.Td>
          <Table.Td>{asset.catalog} / {asset.backend}</Table.Td>
          <Table.Td>{asset.owners.length ? asset.owners.join(", ") : "Not provided"}</Table.Td>
          <Table.Td>{asset.policy_status === "configured" ? "Configured" : "Default NULL"}</Table.Td>
          <Table.Td>{asset.policy_revision ?? 0}</Table.Td>
        </Table.Tr>)}</Table.Tbody>
      </Table>
    </div> : <Text role="status">{props.search ? "No governed assets match this search. Clear the search to browse again." : "No governed assets are available to your account."}</Text>}
    {props.hasMore && <Button variant="default" disabled={props.loading} onClick={props.onLoadMore} leftSection={<Icon name="chevron-down" size={16} />}>Load more assets</Button>}
  </section>;
}
