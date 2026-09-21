import { Combobox, Modal, Text, TextInput, useCombobox } from "@mantine/core";
import type { Asset } from "../api";

export type PaletteCommand = "assets" | "changes" | "activity" | "connections" | "settings" | "help";
export type CommandPaletteProps = {
  opened: boolean;
  query: string;
  commands: PaletteCommand[];
  assets: Asset[];
  onQueryChange: (query: string) => void;
  onCommand: (command: PaletteCommand) => void;
  onAsset: (assetId: string) => void;
  onClose: () => void;
};

export function CommandPalette(props: CommandPaletteProps) {
  const store = useCombobox({ opened: props.opened });
  const query = props.query.trim().toLowerCase();
  const commands = props.commands.filter((command) => command.includes(query));
  const assets = props.assets.filter((asset) =>
    [asset.id, asset.name, asset.catalog, asset.table_identifier]
      .some((value) => value.toLowerCase().includes(query)),
  );
  const options = [
    ...commands.map((command) => ({ value: `command:${command}`, label: command === "help" ? "Keyboard and workflow help" : `Open ${command[0].toUpperCase()}${command.slice(1)}`, select: () => props.onCommand(command) })),
    ...assets.map((asset) => ({ value: `asset:${asset.id}`, label: `${asset.name} · ${asset.catalog}`, select: () => props.onAsset(asset.id) })),
  ];
  return (
    <Modal opened={props.opened} onClose={props.onClose} title="Command palette" closeButtonProps={{ "aria-label": "Close command palette" }} centered size="lg">
      <Combobox store={store} onOptionSubmit={(value) => options.find((option) => option.value === value)?.select()}>
        <Combobox.EventsTarget withExpandedAttribute>
          <TextInput data-autofocus role="combobox" aria-label="Command search" placeholder="Find an asset or destination"
            value={props.query} onChange={(event) => { props.onQueryChange(event.currentTarget.value); store.resetSelectedOption(); }} />
        </Combobox.EventsTarget>
        <Combobox.Options aria-label="Destinations">
          {options.map((option) => <Combobox.Option key={option.value} value={option.value}>{option.label}</Combobox.Option>)}
          {!options.length && <Combobox.Empty>No authorized destination or asset matches that search.</Combobox.Empty>}
        </Combobox.Options>
      </Combobox>
      <Text size="sm" c="dimmed" mt="md">Escape closes search. Publishing, deletion and revocation are never palette commands.</Text>
    </Modal>
  );
}
