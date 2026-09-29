import { useEffect, useMemo, useRef, useState } from "react";
import { Button, NativeSelect, Textarea, TextInput } from "@mantine/core";
import type { SchemaNode } from "../api";
import { compileRowFilter, rowFilterType } from "../row_filter_editor";
import type { RowFilterCondition, RowFilterOperator } from "../row_filter_editor";
import { Icon } from "./Icon";

type Mode = "builder" | "sql";
const labels: Record<RowFilterOperator, string> = { equals: "equals", not_equals: "does not equal", greater: "is greater than", less: "is less than", greater_equal: "is at least", less_equal: "is at most", in: "is one of", is_null: "is empty (NULL)", is_not_null: "is not empty (NULL)" };
const operators = (kind: string | undefined): RowFilterOperator[] => kind === "number" ? ["equals", "not_equals", "greater", "less", "greater_equal", "less_equal", "in", "is_null", "is_not_null"] : kind === "boolean" ? ["equals", "not_equals", "is_null", "is_not_null"] : ["equals", "not_equals", "in", "is_null", "is_not_null"];

export function RowFilterEditor({ value, fields, disabled, onChange, onDirtyChange, initiallyFiltering = false }: {
  initiallyFiltering?: boolean; value: string | null; fields: SchemaNode[]; disabled: boolean; onChange: (value: string | null) => void; onDirtyChange?: (dirty: boolean) => void;
}) {
  const [filtering, setFiltering] = useState(Boolean(value));
  const [mode, setMode] = useState<Mode>(value ? "sql" : "builder");
  const [sql, setSql] = useState(value ?? "");
  const [dirty, setDirty] = useState(false);
  const [join, setJoin] = useState<"AND" | "OR">("AND");
  const [conditions, setConditions] = useState<RowFilterCondition[]>([]);
  const [resetRequested, setResetRequested] = useState(false);
  const ownCommit = useRef<string | null | undefined>(undefined);
  const lastValue = useRef(value);
  const options = useMemo(() => {
    const result: SchemaNode[] = [];
    const visit = (node: SchemaNode) => { if (rowFilterType(node.type) && node.path.segments.every((segment) => segment.kind === "field")) result.push(node); node.children?.forEach(visit); };
    fields.forEach(visit); return result;
  }, [fields]);
  useEffect(() => { if (initiallyFiltering && !value && !disabled) { setFiltering(true); setConditions(options[0] ? [{ field: options[0], operator: "equals", value: "" }] : []); mark(true); } }, []);
  const generated = compileRowFilter(conditions.map((condition) => condition.operator === "in" && Array.isArray(condition.value) ? { ...condition, value: condition.value.map((item) => item.trim()) } : condition), join);
  const invalidField = conditions.some((condition) => !options.some((option) => option.human_path === condition.field.human_path));
  const error = mode === "builder" ? invalidField ? "A selected column is no longer available. Choose another column." : generated.error : sql.trim() ? undefined : "Enter a filter expression, or choose All rows.";
  function mark(next: boolean) { setDirty(next); onDirtyChange?.(next); }
  function restore() {
    setFiltering(Boolean(value)); setSql(value ?? ""); setMode(value ? "sql" : "builder"); setConditions([]); setJoin("AND"); setResetRequested(false); mark(false);
  }
  useEffect(() => {
    if (lastValue.current === value) return;
    lastValue.current = value;
    if (ownCommit.current === value) { ownCommit.current = undefined; return; }
    restore();
  }, [value]);
  function update(next: RowFilterCondition[]) { setConditions(next); mark(true); }
  function addCondition() { if (options[0]) update([...conditions, { field: options[0], operator: "equals", value: "" }]); }
  function apply() {
    if (disabled || error) return;
    const next = mode === "builder" ? generated.sql : sql.trim();
    ownCommit.current = next === value ? undefined : next; onChange(next); mark(false);
  }
  return <section className="policy-step row-filter-editor" aria-label="Row filters">
    <div className="policy-step-heading"><span className="step-number">4</span><div><h3>Choose which rows readers see</h3><p>Limit records using their original values, before masking.</p></div></div>
    <div className="policy-segmented" role="group" aria-label="Row access"><button type="button" aria-pressed={!filtering} disabled={disabled} onClick={() => { setFiltering(false); ownCommit.current = value === null ? undefined : null; onChange(null); mark(false); }}>All rows</button><button type="button" aria-pressed={filtering} disabled={disabled} onClick={() => { if (filtering) return; setFiltering(true); setMode("builder"); setConditions(options[0] ? [{ field: options[0], operator: "equals", value: "" }] : []); mark(true); }}>Filter rows</button></div>
    {!filtering ? <p className="help">All rows are allowed by this rule.</p> : <div className="row-filter-content">
      <div className="filter-toolbar"><div className="policy-segmented" role="group" aria-label="Filter editing mode"><button type="button" aria-pressed={mode === "builder"} disabled={disabled} onClick={() => { if (mode === "builder") return; if (sql.trim()) setResetRequested(true); else { setMode("builder"); if (!conditions.length) addCondition(); } }}>Builder</button><button type="button" aria-pressed={mode === "sql"} disabled={disabled || (mode === "builder" && dirty && conditions.length > 0 && Boolean(error))} onClick={() => { if (mode === "sql") return; setSql(generated.sql ?? ""); setMode("sql"); }}>DuckDB SQL</button></div>{dirty && <span className="local-edit-label">Not yet applied</span>}</div>
      {resetRequested && <div className="inline-confirm"><p>Replace this SQL with a new filter? The builder cannot import arbitrary SQL.</p><Button variant="default" size="sm" onClick={() => setResetRequested(false)}>Keep SQL</Button><Button size="sm" onClick={() => { setMode("builder"); setConditions(options[0] ? [{ field: options[0], operator: "equals", value: "" }] : []); setJoin("AND"); setResetRequested(false); mark(true); }}>Replace with builder</Button></div>}
      {mode === "builder" ? <>
        <label className="filter-match">Include rows matching <select aria-label="Condition group" value={join} disabled={disabled} onChange={(event) => { setJoin(event.currentTarget.value as "AND" | "OR"); mark(true); }}><option value="AND">all conditions (AND)</option><option value="OR">any condition (OR)</option></select></label>
        <div className="filter-conditions">{conditions.map((condition, index) => {
          const kind = rowFilterType(condition.field.type);
          const noValue = ["is_null", "is_not_null"].includes(condition.operator);
          return <div className="filter-condition-wrap" key={index}>{index > 0 && <span className="filter-join">{join}</span>}<div className="row-filter-condition">
            <NativeSelect aria-label={`Filter column ${index + 1}`} value={condition.field.human_path} disabled={disabled} data={options.map((field) => ({ value: field.human_path, label: `${field.human_path} · ${field.type}` }))} onChange={(event) => { const field = options.find((option) => option.human_path === event.currentTarget.value); if (field) update(conditions.map((row, i) => i === index ? { field, operator: "equals", value: "" } : row)); }} />
            <NativeSelect aria-label={`Filter operator ${index + 1}`} value={condition.operator} disabled={disabled} data={operators(kind).map((operator) => ({ value: operator, label: labels[operator] }))} onChange={(event) => update(conditions.map((row, i) => i === index ? { ...row, operator: event.currentTarget.value as RowFilterOperator, value: "" } : row))} />
            {!noValue && (kind === "boolean" ? <NativeSelect aria-label={`Filter value ${index + 1}`} value={condition.value as string ?? ""} disabled={disabled} data={[{ value: "", label: "Choose a value" }, "true", "false"]} onChange={(event) => update(conditions.map((row, i) => i === index ? { ...row, value: event.currentTarget.value } : row))} /> : <TextInput aria-label={`Filter value ${index + 1}`} inputMode={kind === "number" && condition.operator !== "in" ? "decimal" : "text"} value={Array.isArray(condition.value) ? condition.value.join(",") : condition.value ?? ""} placeholder={condition.operator === "in" ? "Values separated by commas" : "Value"} disabled={disabled} onChange={(event) => update(conditions.map((row, i) => i === index ? { ...row, value: condition.operator === "in" ? event.currentTarget.value.split(",") : event.currentTarget.value } : row))} />)}
            {noValue && <span className="filter-no-value">No value needed</span>}
            <Button variant="subtle" color="red" size="compact-sm" aria-label={`Remove condition ${index + 1}`} disabled={disabled} onClick={() => update(conditions.filter((_, i) => i !== index))}><Icon name="x" size={16} /></Button>
          </div></div>;
        })}</div>
        <Button className="add-condition" variant="subtle" size="sm" leftSection={<Icon name="plus" size={15} />} disabled={disabled || !options.length} onClick={addCondition}>Add condition</Button>
        {!options.length && <p className="help">No columns supported by the builder. Use DuckDB SQL for other types.</p>}
        {generated.sql && <details className="generated-filter"><summary>View SQL expression</summary><code>{generated.sql}</code></details>}
      </> : <Textarea label="DuckDB SQL row filter" autosize minRows={3} value={sql} disabled={disabled} onChange={(event) => { setSql(event.currentTarget.value); mark(true); }} placeholder="country = 'US'" description="A filter expression, not a SELECT statement." />}
      {dirty && error && <p className="field-error" role="alert">{error}</p>}
      {dirty && <div className="composer-footer"><Button variant="default" onClick={restore}>Discard row changes</Button><Button disabled={disabled || Boolean(error)} onClick={apply}>Apply row filter</Button></div>}
    </div>}
    <div className="policy-note"><Icon name="filter" size={15} /><p>Other matching rules can further restrict rows. Mask exemptions never bypass row filters.</p></div>
  </section>;
}
