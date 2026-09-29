import { useRef, useState } from "react";
import { Button } from "@mantine/core";
import type { Mask, PolicyRule, SchemaNode } from "../api";
import { applyMaskToTargets, authoritativeColumnOptions, expandColumnSelections, removeColumnSelections } from "../policy_editor";
import { BulkMaskEditor } from "./BulkMaskEditor";
import { ColumnMultiSelect } from "./ColumnMultiSelect";
import { RowFilterEditor } from "./RowFilterEditor";

export function PolicyRuleEditor({ rule, fields, supportedMasks, readOnly, onUpdateRule, onDirtyChange }: {
  assetId: string; ruleIndex: number; rule: PolicyRule; fields: SchemaNode[] | undefined; supportedMasks: Mask["type"][]; readOnly: boolean;
  onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onDirtyChange?: (dirty: boolean) => void;
}) {
  const pending = useRef({ mask: false, rows: false });
  const [showMasks, setShowMasks] = useState(Object.keys(rule.masks).length > 0);
  const [showRows, setShowRows] = useState(Boolean(rule.row_filter));
  const report = (section: "mask" | "rows", dirty: boolean) => { pending.current[section] = dirty; onDirtyChange?.(pending.current.mask || pending.current.rows); };
  const options = authoritativeColumnOptions(fields, [...rule.columns, ...Object.keys(rule.masks)]);
  return <div className="policy-rule-editor">
    <section className="policy-step"><div className="policy-step-heading"><div><h3>Select allowed columns</h3><p>These columns override the default NULL mask. They show original values unless you add a mask.</p></div></div>
      {!fields && <p role="status">Schema unavailable. Refresh before selecting columns.</p>}
      <ColumnMultiSelect shortcuts label="Allowed columns" options={options} value={rule.columns} disabled={readOnly || !fields || pending.current.mask} onChange={(selected) => { const columns = expandColumnSelections(options, selected); onUpdateRule((current) => ({ ...removeColumnSelections(current, current.columns.filter((column) => !columns.includes(column)), options), columns })); }} />
    </section>
    <div className="rule-add-options">{!showMasks && <Button variant="default" disabled={readOnly || !rule.columns.length} onClick={() => setShowMasks(true)}>Add Column Masks</Button>}{!showRows && <Button variant="default" disabled={readOnly} onClick={() => setShowRows(true)}>Add Row Filter</Button>}</div>
    {showMasks && <BulkMaskEditor initiallyEditing={!Object.keys(rule.masks).length} rule={rule} options={options} supportedMasks={supportedMasks} readOnly={readOnly} onApply={(targets, mask, excluded) => onUpdateRule((current) => applyMaskToTargets(current, targets, mask, excluded, options))} onDirtyChange={(dirty) => report("mask", dirty)} />}
    {showRows && <RowFilterEditor initiallyFiltering={!rule.row_filter} value={rule.row_filter} fields={fields ?? []} disabled={readOnly} onChange={(value) => onUpdateRule((current) => ({ ...current, row_filter: value }))} onDirtyChange={(dirty) => report("rows", dirty)} />}
  </div>;
}
