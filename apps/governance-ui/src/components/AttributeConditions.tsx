import { useEffect, useRef, useState } from "react";
import { Alert, Button, NativeSelect, Select } from "@mantine/core";
import type { AttributeProvider } from "../api";
import { TokenInput } from "./TokenInput";
import { Icon } from "./Icon";

type Row = { key: string; mode: "equals" | "one_of"; values: string[] };
const rowsFrom = (value: Record<string, string | string[]>): Row[] => Object.entries(value).map(([key, raw]) => ({ key, mode: Array.isArray(raw) ? "one_of" : "equals", values: Array.isArray(raw) ? raw : raw ? [raw] : [] }));

export function AttributeConditions({ value = {}, providers, disabled, onChange, onInvalid }: {
  value?: Record<string, string | string[]>; providers: AttributeProvider[]; disabled: boolean;
  onChange: (value: Record<string, string | string[]>) => void; onInvalid?: (invalid: boolean) => void;
}) {
  const [rows, setRows] = useState(() => rowsFrom(value));
  const lastCommit = useRef(JSON.stringify(value));
  useEffect(() => {
    if (JSON.stringify(value) !== lastCommit.current) setRows(rowsFrom(value));
    lastCommit.current = JSON.stringify(value);
  }, [value]);
  const keys = [...new Set(providers.flatMap((provider) => provider.attributes.map((attribute) => attribute.key)))];
  const invalid = rows.some((row) => !row.key || !row.values.length) || new Set(rows.map((row) => row.key)).size !== rows.length;
  useEffect(() => { onInvalid?.(invalid); }, [invalid, onInvalid]);
  function commit(next: Row[]) {
    setRows(next);
    const result = Object.fromEntries(next.map((row) => [row.key, row.mode === "one_of" ? row.values : row.values[0] ?? ""]));
    lastCommit.current = JSON.stringify(result); onChange(result);
  }
  const update = (index: number, change: Partial<Row>) => commit(rows.map((row, i) => i === index ? { ...row, ...change } : row));
  return <div className="attribute-conditions">
    <p className="help">All conditions must match. Missing attributes do not match. Values are case-sensitive.</p>
    {!keys.length && <Alert color="gray">No attributes configured. An administrator can map identity attributes in Settings. Existing conditions stay intact.</Alert>}
    {rows.map((row, index) => {
      const sources = providers.filter((provider) => provider.attributes.some((attribute) => attribute.key === row.key));
      const attributes = sources.flatMap((provider) => provider.attributes.filter((attribute) => attribute.key === row.key));
      const definition = attributes[0];
      // A shared canonical key may be supplied by several trusted providers.
      // If any source is unrestricted, custom values remain valid for that key.
      const restricted = attributes.length > 0 && attributes.every((attribute) => attribute.allowed_values.length > 0);
      const domain = [...new Set(attributes.flatMap((attribute) => attribute.allowed_values))];
      const stale = restricted ? row.values.filter((value) => !domain.includes(value)) : [];
      const keyOptions = [...new Set([...keys, row.key].filter(Boolean))].filter((key) => key === row.key || !rows.some((other, i) => i !== index && other.key === key)).map((key) => ({ value: key, label: providers.flatMap((provider) => provider.attributes).find((attribute) => attribute.key === key)?.label || key }));
      return <div className="attribute-condition-card" key={index}>
        <div className="attribute-condition-controls">
          <Select label={`Attribute ${index + 1}`} searchable filter={({ options, search }) => options.filter((option) => "value" in option && `${option.label} ${option.value}`.toLowerCase().includes(search.toLowerCase()))} data={keyOptions} value={row.key || null} placeholder="Choose an attribute" disabled={disabled} onChange={(key) => update(index, { key: key ?? "", values: [] })} nothingFoundMessage="No matching attributes" />
          <NativeSelect label={`Operator ${index + 1}`} value={row.mode} disabled={disabled} data={[{ value: "equals", label: "equals" }, { value: "one_of", label: "is one of" }]} onChange={(event) => { const mode = event.currentTarget.value as Row["mode"]; update(index, { mode, values: mode === "equals" ? row.values.slice(0, 1) : row.values }); }} />
          {restricted ? row.mode === "equals" ? <Select label={`Allowed value ${index + 1}`} searchable data={[...new Set([...domain, ...stale])]} value={row.values[0] ?? null} disabled={disabled || !row.key} placeholder="Choose a value" onChange={(value) => update(index, { values: value ? [value] : [] })} /> : <div className="attribute-values-picker"><Select label={`Allowed values ${index + 1}`} searchable data={domain.filter((value) => !row.values.includes(value))} value={null} disabled={disabled || !row.key} placeholder="Add an allowed value" nothingFoundMessage="No more matching values" onChange={(value) => { if (value) update(index, { values: [...row.values, value] }); }} /><div className="token-chips">{row.values.map((value) => <span className="column-chip" key={value}>{value}<button type="button" aria-label={`Remove value ${value}`} disabled={disabled} onClick={() => update(index, { values: row.values.filter((item) => item !== value) })}>×</button></span>)}</div></div> : <TokenInput label={`Values ${index + 1}`} value={row.values} maxTokens={row.mode === "equals" ? 1 : 100} disabled={disabled || !row.key} placeholder="Type a value and press Enter" onChange={(values) => update(index, { values })} splitCommas={false} help="Unrestricted text. Press Enter to add a value." />}
          <Button variant="subtle" color="red" disabled={disabled} aria-label={`Remove condition ${index + 1}`} onClick={() => commit(rows.filter((_, i) => i !== index))}><Icon name="trash" size={16} /></Button>
        </div>
        {definition && <p className="attribute-source"><strong>{row.key}</strong>{definition.description ? ` · ${definition.description}` : ""} · {restricted ? `${domain.length} allowed values` : "Unrestricted text"}<span>{sources.map((source) => `${source.issuer} → ${source.attributes.find((attribute) => attribute.key === row.key)!.claim_path}`).join("; ")}</span></p>}
        {row.key && !definition && <Alert color="yellow">Unmapped attribute “{row.key}”. Existing condition preserved; select a configured attribute to replace it.</Alert>}
        {stale.length > 0 && <Alert color="yellow">Values no longer in the configured domain: {stale.join(", ")}. They remain selected for review.</Alert>}
      </div>;
    })}
    {invalid && <p role="alert" className="field-error">Choose a unique attribute and at least one value for every condition.</p>}
    <Button variant="default" size="sm" disabled={disabled || keys.every((key) => rows.some((row) => row.key === key))} leftSection={<Icon name="plus" size={14} />} onClick={() => commit([...rows, { key: "", mode: "equals", values: [] }])}>Add condition</Button>
  </div>;
}
