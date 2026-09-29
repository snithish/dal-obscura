import { useEffect, useState } from "react";
import { Button, NativeSelect, TextInput } from "@mantine/core";
import type { Mask, PolicyRule } from "../api";
import { groupMasks, type ColumnOption } from "../policy_editor";
import { ColumnMultiSelect } from "./ColumnMultiSelect";
import { TokenInput } from "./TokenInput";
import { Icon } from "./Icon";

export const maskLabels: Record<Mask["type"], string> = { null: "Hide value", redact: "Redact", hash: "Hash", email: "Mask email", keep_last: "Keep last characters", default: "Replace value" };
const descriptions: Record<Mask["type"], string> = {
  null: "Return an empty (NULL) value, keeping the column's type.",
  redact: "Replace every value with the same text.", hash: "Replace values with a consistent SHA-256 hash.",
  email: "Hide part of each email address.", keep_last: "Hide all but the last few characters.", default: "Replace every value with a text, number, boolean, or NULL literal.",
};

export function BulkMaskEditor({ rule, options, supportedMasks, readOnly, onApply, onDirtyChange, initiallyEditing = false }: {
  initiallyEditing?: boolean; rule: PolicyRule; options: ColumnOption[]; supportedMasks: Mask["type"][]; readOnly: boolean;
  onApply: (targets: string[], mask: Mask | undefined, excluded: string[]) => void; onDirtyChange?: (dirty: boolean) => void;
}) {
  const [editing, setEditing] = useState(initiallyEditing);
  useEffect(() => { if (initiallyEditing) onDirtyChange?.(true); }, []);
  const [originalTargets, setOriginalTargets] = useState<string[]>([]);
  const [targets, setTargets] = useState<string[]>(initiallyEditing ? rule.columns : []);
  const [excluded, setExcluded] = useState<string[]>([]);
  const [type, setType] = useState<Mask["type"] | "none" | "">("");
  const [rawValue, setRawValue] = useState("");
  const [tokens, setTokens] = useState<string[]>([]);
  const [pendingToken, setPendingToken] = useState("");
  const [showColumns, setShowColumns] = useState(false);
  const [showPeople, setShowPeople] = useState(false);
  const groups = groupMasks(rule, [...new Set([...rule.columns, ...Object.keys(rule.masks)])]);
  const masked = groups.filter((group) => group.mask);
  const unmasked = groups.find((group) => !group.mask)?.columns ?? [];
  const available = options.filter((option) => rule.columns.includes(option.value) || Object.hasOwn(rule.masks, option.value));
  const validTargets = targets.length > 0 && targets.every((column) => available.some((option) => option.value === column && option.valid));

  function begin(columns: string[], mask?: Mask) {
    setTargets(columns); setOriginalTargets(mask ? columns : []); setExcluded([]); setType(mask?.type ?? "");
    setRawValue(mask?.type === "default" ? JSON.stringify(mask.value ?? null) : mask?.value == null ? "" : String(mask.value));
    setTokens(mask?.exempt_principals ?? []); setPendingToken("");
    setShowColumns(false); setShowPeople(Boolean(mask?.exempt_principals?.length));
    setEditing(true); onDirtyChange?.(true);
  }
  function close() { setEditing(false); onDirtyChange?.(false); }
  let mask: Mask | undefined = type && type !== "none" ? { type } : undefined;
  let error = "";
  if (type === "keep_last") {
    if (!/^\d+$/.test(rawValue) || !Number.isSafeInteger(Number(rawValue))) error = "Enter a whole number of characters, zero or greater.";
    else mask = { type, value: Number(rawValue) };
  } else if (type === "redact") mask = { type, value: rawValue };
  else if (type === "default") {
    try {
      const value: unknown = JSON.parse(rawValue);
      if (value !== null && !["string", "number", "boolean"].includes(typeof value)) throw new Error();
      if (typeof value === "number" && !Number.isFinite(value)) throw new Error();
      mask = { type, value: value as Mask["value"] };
    } catch { error = 'Enter a JSON scalar, such as "private", 0, true, or null.'; }
  }
  const exemptionTokens = [...new Set([...tokens, pendingToken.trim()].filter(Boolean))];
  if (exemptionTokens.some((token) => token === "*" || token.toLowerCase() === "everyone" || (token.startsWith("group:") && (!token.slice(6) || /\s/.test(token) || ["*", "everyone"].includes(token.slice(6).toLowerCase()))))) error = "Use a principal ID or group:name. Wildcard exemptions are not allowed.";
  if (type && type !== "none" && type !== "null" && targets.some((target) => !excluded.includes(target) && options.find((option) => option.value === target)?.path.segments.at(-1)?.kind === "map_key")) error = "Map keys support No mask or Hide value only. Exclude map keys to mask their values.";
  const selectedExcluded = excluded.filter((column) => targets.includes(column));
  const count = targets.length - selectedExcluded.length;

  return <section className="policy-step bulk-mask-editor" aria-label="Column masks">
    <div className="policy-step-heading"><span className="step-number">3</span><div><h3>Protect sensitive values</h3><p>Apply a mask to several columns. Add exceptions only where needed.</p></div>{!editing && <Button variant="light" size="sm" onClick={() => begin(unmasked.length ? unmasked : rule.columns)} disabled={readOnly || !available.some((option) => option.valid)} leftSection={<Icon name="plus" size={15} />}>Add mask</Button>}</div>
    {!masked.length && !editing && <div className="policy-empty"><Icon name="shield-check" size={22} /><div><strong>Values are shown as stored</strong><p>Add a mask to protect sensitive columns.</p></div></div>}
    {masked.length > 0 && <div className="mask-cards">{masked.map((group, index) => <article className="mask-card" key={index}>
      <div className="mask-card-heading"><Icon name="shield-check" size={17} /><strong>{maskLabels[group.mask!.type]}</strong><span className="policy-count">{group.columns.length} {group.columns.length === 1 ? "column" : "columns"}</span><Button variant="subtle" size="compact-sm" aria-label={`Edit ${maskLabels[group.mask!.type]} mask`} onClick={() => begin(group.columns, group.mask)} disabled={readOnly || editing}>Edit</Button><Button variant="subtle" color="red" size="compact-sm" aria-label={`Remove ${maskLabels[group.mask!.type]} mask`} onClick={() => onApply(group.columns, undefined, [])} disabled={readOnly || editing}>Remove</Button></div>
      <div className="policy-paths">{group.columns.map((column) => <code key={column}>{column}</code>)}</div>
      {group.mask!.value !== undefined && <p className="mask-detail">{group.mask!.type === "keep_last" ? "Characters retained" : "Replacement"}: <code>{JSON.stringify(group.mask!.value)}</code></p>}
      {Boolean(group.mask!.exempt_principals?.length) && <div className="mask-exception-summary"><span>Can see original values:</span>{group.mask!.exempt_principals!.map((token) => <span className="column-chip" key={token}>{token}</span>)}</div>}
    </article>)}</div>}
    {editing && <div className="policy-composer" role="group" aria-label="Mask configuration">
      <div className="composer-heading"><h4>Configure mask</h4><span className="local-edit-label">Not yet applied</span></div>
      <NativeSelect label="Mask" value={type} data={[{ value: "", label: "Choose how to protect values" }, { value: "none", label: "No mask — show original values" }, ...supportedMasks.map((item) => ({ value: item, label: maskLabels[item] }))]} disabled={readOnly} onChange={(event) => {
        const next = event.currentTarget.value as Mask["type"] | "none" | ""; setType(next); setRawValue(next === "redact" ? "[REDACTED]" : next === "keep_last" ? "4" : next === "default" ? '"private"' : "");
      }} />
      {type && type !== "none" && <p className="help">{descriptions[type]}</p>}
      {type === "redact" && <TextInput label="Replacement text" value={rawValue} onChange={(event) => setRawValue(event.currentTarget.value)} disabled={readOnly} />}
      {type === "keep_last" && <TextInput label="Characters to retain" inputMode="numeric" value={rawValue} onChange={(event) => setRawValue(event.currentTarget.value)} disabled={readOnly} />}
      {type === "default" && <TextInput label="Replacement value" value={rawValue} onChange={(event) => setRawValue(event.currentTarget.value)} disabled={readOnly} />}
      <ColumnMultiSelect label="Columns to mask" options={available} value={targets} onChange={(next) => { setTargets(next); setExcluded((current) => current.filter((column) => next.includes(column))); }} disabled={readOnly} />
      {type && type !== "none" && <div className="mask-exceptions"><div className="exception-actions">{!showColumns && <Button variant="subtle" size="compact-sm" onClick={() => setShowColumns(true)} disabled={readOnly}>Exclude columns</Button>}{!showPeople && <Button variant="subtle" size="compact-sm" onClick={() => setShowPeople(true)} disabled={readOnly}>Exempt people or groups</Button>}</div>
        {showColumns && <div className="exception-section"><ColumnMultiSelect label="Columns without this mask" options={available.filter((option) => targets.includes(option.value))} value={selectedExcluded} onChange={setExcluded} disabled={readOnly} /><p className="help">Keep these columns visible with their original values in this rule. Applying removes their current mask.</p></div>}
        {showPeople && <div className="exception-section"><TokenInput label="People or groups that skip this mask" value={tokens} onChange={setTokens} onDraftChange={setPendingToken} placeholder="group:privacy-reviewers" disabled={readOnly} /><p className="help">These readers still need access. Row filters and masks from other rules still apply.</p></div>}
      </div>}
      {error && <p className="field-error" role="alert">{error}</p>}
      <div className="composer-footer"><p>{type ? type === "none" ? `${count} columns show original values` : `${count} masked · ${selectedExcluded.length} excluded` : "Choose a mask to continue"}</p><Button variant="default" onClick={close}>Cancel mask changes</Button><Button disabled={readOnly || !type || !validTargets || Boolean(error)} onClick={() => { onApply([...new Set([...originalTargets, ...targets])], mask ? { ...mask, ...(exemptionTokens.length ? { exempt_principals: exemptionTokens } : {}) } : undefined, [...selectedExcluded, ...originalTargets.filter((column) => !targets.includes(column))]); close(); }}>Apply mask</Button></div>
    </div>}
    {unmasked.length > 0 && <details className="unmasked-columns"><summary>{unmasked.length} {unmasked.length === 1 ? "column shows" : "columns show"} original values</summary><div className="policy-paths">{unmasked.map((column) => <code key={column}>{column}</code>)}</div></details>}
  </section>;
}
