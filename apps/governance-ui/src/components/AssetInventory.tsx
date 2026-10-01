import { Button, Popover, Table, Text, TextInput } from "@mantine/core";
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
    <div className="inventory-toolbar">
      <div className="inventory-toolbar-title"><h2>Governed assets</h2><Text size="sm" c="dimmed" role="status">{props.loading ? "Updating inventory…" : `${props.assets.length} ${props.assets.length === 1 ? "asset" : "assets"} loaded`}</Text></div>
      <TextInput label="Find governed asset" type="search" value={props.search}
        placeholder="Search catalog or asset"
        onChange={(event) => props.onSearch(event.currentTarget.value)}
        leftSection={<Icon name="search" size={15} />} />
    </div>
    {props.assets.length ? <div className="asset-inventory-table" role="region" aria-label="Governed assets table" tabIndex={0}>
      <Table highlightOnHover>
        <Table.Thead><Table.Tr><Table.Th>Asset</Table.Th><Table.Th>Catalog / format</Table.Th><Table.Th>Owners</Table.Th><Table.Th>Policy</Table.Th><Table.Th className="inventory-revision">Revision</Table.Th></Table.Tr></Table.Thead>
        <Table.Tbody>{props.assets.map((asset) => <Table.Tr key={asset.id}>
          <Table.Td><button type="button" className="inventory-asset-link"
            onClick={() => props.onSelect(asset.id)}>{asset.name}</button></Table.Td>
          <Table.Td><div className="inventory-catalog"><span>{asset.catalog}</span><span className="context-tag">{asset.backend}</span></div></Table.Td>
          <Table.Td>{asset.owners.length ? <Popover position="bottom-start" width={360} withArrow><Popover.Target><button type="button" className="inventory-owner-button"><Icon name="key-round" size={14} /><span>{asset.owners[0].split("|").at(-1)}</span>{asset.owners.length > 1 && <span className="context-tag">+{asset.owners.length - 1}</span>}<Icon name="chevron-down" size={13} /></button></Popover.Target><Popover.Dropdown><strong>Asset owners</strong><p className="help">Full principal identities</p><ul className="inventory-owner-details">{asset.owners.map((owner) => <li key={owner}><code>{owner}</code></li>)}</ul></Popover.Dropdown></Popover> : <span className="muted">Not provided</span>}</Table.Td>
          <Table.Td><span className={`policy-status-tag ${asset.policy_status === "configured" ? "configured" : "default"}`}><Icon name={asset.policy_status === "configured" ? "shield-check" : "key-round"} size={13} />{asset.policy_status === "configured" ? "Configured" : "Default NULL"}</span></Table.Td>
          <Table.Td className="inventory-revision"><span className="revision-tag">{asset.policy_revision ?? 0}</span></Table.Td>
        </Table.Tr>)}</Table.Tbody>
      </Table>
    </div> : <Text role="status">{props.search ? "No governed assets match this search. Clear the search to browse again." : "No governed assets are available to your account."}</Text>}
    {props.hasMore && <Button variant="default" disabled={props.loading} onClick={props.onLoadMore} leftSection={<Icon name="chevron-down" size={16} />}>Load more assets</Button>}
  </section>;
}
