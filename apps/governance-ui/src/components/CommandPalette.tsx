import type { Asset } from "../api";

export type PaletteCommand = "assets" | "changes" | "activity" | "connections" | "settings" | "help";

export type CommandPaletteProps = {
  query: string;
  commands: PaletteCommand[];
  assets: Asset[];
  onQueryChange: (query: string) => void;
  onCommand: (command: PaletteCommand) => void;
  onAsset: (assetId: string) => void;
  onClose: () => void;
};

export function CommandPalette({
  query,
  commands,
  assets,
  onQueryChange,
  onCommand,
  onAsset,
  onClose,
}: CommandPaletteProps) {
  const normalizedQuery = query.toLowerCase();
  const visibleCommands = commands.filter((command) => command.includes(normalizedQuery));
  const hasSearchResult = visibleCommands.length > 0 || assets.length > 0;

  return (
    <div className="palette-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="command-palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <input
          autoFocus
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder="Jump to a destination or search an asset"
          aria-label="Command search"
        />
        <div role="listbox">
          {visibleCommands.map((command) => (
            <button key={command} type="button" role="option" onClick={() => onCommand(command)}>
              {command === "help" ? "Keyboard and workflow help" : `Open ${titleFor(command)}`}
            </button>
          ))}
          {assets.map((asset) => (
            <button key={asset.id} type="button" role="option" onClick={() => onAsset(asset.id)}>
              <strong>{asset.name}</strong>
              <small>{asset.catalog} · {asset.backend}</small>
            </button>
          ))}
          {query && !hasSearchResult && (
            <p className="help">No authorized destination or asset matches that search.</p>
          )}
        </div>
        <p className="help">Press Escape to close. Publishing, deletion, and revocation are never palette commands.</p>
      </section>
    </div>
  );
}

function titleFor(command: Exclude<PaletteCommand, "help">): string {
  return ({ assets: "Assets", changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings" })[command];
}
