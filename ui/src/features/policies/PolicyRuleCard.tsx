import {
  describeRuleForm,
  type ColumnSelection,
  type ConditionRow,
  type MaskRow,
  type PolicyRuleForm,
} from "./policyLogic";

export function RuleCard({
  canRemove,
  editable,
  onChange,
  onRemove,
  rule,
  ruleIndex,
  schemaColumns,
}: {
  canRemove: boolean;
  editable: boolean;
  onChange: (patch: Partial<PolicyRuleForm>) => void;
  onRemove: () => void;
  rule: PolicyRuleForm;
  ruleIndex: number;
  schemaColumns: string[];
}) {
  const description = describeRuleForm(rule, schemaColumns);
  return (
    <article className="rounded-card border border-border bg-white p-4">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Rule {ruleIndex + 1}
          </span>
          <h3 className="mt-1 text-base font-black">{description.title}</h3>
          <div className="mt-2 flex flex-wrap gap-2">
            {description.details.map((detail) => (
              <span className="badge" key={detail}>
                {detail}
              </span>
            ))}
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <select
            className="field min-w-[120px]"
            disabled={!editable}
            value={rule.effect}
            onChange={(event) => {
              const effect = event.target.value as "allow" | "deny";
              onChange(
                effect === "deny"
                  ? { effect, masks: [], rowFilter: "" }
                  : { effect },
              );
            }}
          >
            <option value="allow">Allow</option>
            <option value="deny">Deny</option>
          </select>
          {canRemove ? (
            <button
              className="btn-secondary"
              disabled={!editable}
              type="button"
              onClick={onRemove}
            >
              Remove
            </button>
          ) : null}
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
        <TextListField
          help="Comma-separated users, groups, or service identities."
          label="Principals"
          value={rule.principalsText}
          onChange={(value) => onChange({ principalsText: value })}
          disabled={!editable}
        />
        {schemaColumns.length === 0 ? (
          <TextListField
            help="Use * for all columns, or comma-separate explicit columns."
            label="Visible columns"
            value={rule.columnsText}
            onChange={(value) => onChange({ columnsText: value })}
            disabled={!editable}
          />
        ) : (
          <ColumnSelector
            selections={rule.columnSelections}
            onChange={(columnSelections) => onChange({ columnSelections })}
            disabled={!editable}
          />
        )}
      </div>

      {rule.effect === "allow" ? (
        <label className="mt-4 block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Row filter SQL
          </span>
          <input
            className="field mt-2"
            disabled={!editable}
            placeholder="region = 'us'"
            value={rule.rowFilter}
            onChange={(event) => onChange({ rowFilter: event.target.value })}
          />
          <span className="mt-1 block text-xs text-muted">
            DuckDB SQL expression applied before masking.
          </span>
        </label>
      ) : null}

      <EditablePairs
        addLabel="Add condition"
        emptyLabel="No match conditions"
        itemLabel="Condition"
        keyLabel="Claim or attribute"
        valueLabel="Expected value"
        rows={rule.conditions}
        onChange={(conditions) => onChange({ conditions })}
        disabled={!editable}
      />

      {rule.effect === "allow" ? (
        <MaskEditor
          schemaColumns={schemaColumns}
          masks={rule.masks}
          onChange={(masks) => onChange({ masks })}
          disabled={!editable}
        />
      ) : null}
    </article>
  );
}

function TextListField({
  disabled,
  help,
  label,
  onChange,
  value,
}: {
  disabled: boolean;
  help: string;
  label: string;
  onChange: (value: string) => void;
  value: string;
}) {
  return (
    <label className="block">
      <span className="text-xs font-black uppercase tracking-wide text-muted">{label}</span>
      <input
        className="field mt-2"
        disabled={disabled}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
      <span className="mt-1 block text-xs text-muted">{help}</span>
    </label>
  );
}

function EditablePairs({
  addLabel,
  disabled,
  emptyLabel,
  itemLabel,
  keyLabel,
  onChange,
  rows,
  valueLabel,
}: {
  addLabel: string;
  disabled: boolean;
  emptyLabel: string;
  itemLabel: string;
  keyLabel: string;
  onChange: (rows: ConditionRow[]) => void;
  rows: ConditionRow[];
  valueLabel: string;
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-3">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <h4 className="text-sm font-black">{itemLabel}s</h4>
          <p className="mt-1 text-xs text-muted">{emptyLabel} unless one is added.</p>
        </div>
        <button
          className="btn-secondary"
          disabled={disabled}
          type="button"
          onClick={() => onChange([...rows, { key: "", value: "", valueKind: "text" }])}
        >
          {addLabel}
        </button>
      </div>
      <div className="mt-3 grid gap-3">
        {rows.length === 0 ? (
          <div className="rounded-card border border-dashed border-border bg-white p-4 text-sm text-muted">
            {emptyLabel}
          </div>
        ) : (
          rows.map((row, index) => (
            <div
              className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_1fr_140px_auto]"
              key={`${row.key}-${index}`}
            >
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  {keyLabel}
                </span>
                <input
                  className="field mt-2"
                  disabled={disabled}
                  value={row.key}
                  onChange={(event) =>
                    onChange(replaceAt(rows, index, { ...row, key: event.target.value }))
                  }
                />
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  {valueLabel}
                </span>
                <input
                  className="field mt-2"
                  disabled={disabled}
                  value={row.value}
                  onChange={(event) =>
                    onChange(replaceAt(rows, index, { ...row, value: event.target.value }))
                  }
                />
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Type
                </span>
                <select
                  className="field mt-2"
                  disabled={disabled}
                  value={row.valueKind}
                  onChange={(event) =>
                    onChange(
                      replaceAt(rows, index, {
                        ...row,
                        valueKind: event.target.value as "text" | "list",
                      }),
                    )
                  }
                >
                  <option value="text">Text</option>
                  <option value="list">List</option>
                </select>
              </label>
              <button
                className="btn-secondary self-end"
                disabled={disabled}
                type="button"
                onClick={() => onChange(rows.filter((_, currentIndex) => currentIndex !== index))}
              >
                Remove
              </button>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function MaskEditor({
  disabled,
  masks,
  onChange,
  schemaColumns,
}: {
  disabled: boolean;
  masks: MaskRow[];
  onChange: (masks: MaskRow[]) => void;
  schemaColumns: string[];
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-3">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <h4 className="text-sm font-black">Column masks</h4>
          <p className="mt-1 text-xs text-muted">Mask sensitive columns after row filters run.</p>
        </div>
        <button
          className="btn-secondary"
          disabled={disabled}
          type="button"
          onClick={() => onChange([...masks, { column: "", type: "email" }])}
        >
          Add mask
        </button>
      </div>
      <div className="mt-3 grid gap-3">
        {masks.length === 0 ? (
          <div className="rounded-card border border-dashed border-border bg-white p-4 text-sm text-muted">
            No column masks
          </div>
        ) : (
          masks.map((mask, index) => (
            <div
              className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_180px_auto]"
              key={`${mask.column}-${index}`}
            >
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Column
                </span>
                {schemaColumns.length === 0 ? (
                  <input
                    className="field mt-2"
                    disabled={disabled}
                    value={mask.column}
                    onChange={(event) =>
                      onChange(replaceAt(masks, index, { ...mask, column: event.target.value }))
                    }
                  />
                ) : (
                  <select
                    className="field mt-2"
                    disabled={disabled}
                    value={mask.column}
                    onChange={(event) =>
                      onChange(replaceAt(masks, index, { ...mask, column: event.target.value }))
                    }
                  >
                    <option value="">Choose column</option>
                    {schemaColumns.map((column) => (
                      <option key={column} value={column}>
                        {column}
                      </option>
                    ))}
                  </select>
                )}
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Mask type
                </span>
                <select
                  className="field mt-2"
                  disabled={disabled}
                  value={mask.type}
                  onChange={(event) =>
                    onChange(replaceAt(masks, index, { ...mask, type: event.target.value }))
                  }
                >
                  <option value="email">Email</option>
                  <option value="hash">Hash</option>
                  <option value="null">Null</option>
                  <option value="redact">Redact</option>
                </select>
              </label>
              <button
                className="btn-secondary self-end"
                disabled={disabled}
                type="button"
                onClick={() => onChange(masks.filter((_, currentIndex) => currentIndex !== index))}
              >
                Remove
              </button>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function ColumnSelector({
  disabled,
  onChange,
  selections,
}: {
  disabled: boolean;
  onChange: (selections: ColumnSelection[]) => void;
  selections: ColumnSelection[];
}) {
  return (
    <section>
      <span className="text-xs font-black uppercase tracking-wide text-muted">
        Visible columns
      </span>
      <div className="mt-2 grid max-h-48 gap-2 overflow-auto rounded-card border border-border bg-soft p-3">
        {selections.map((selection, index) => (
          <label
            className="flex items-center gap-2 rounded-card bg-white px-3 py-2 text-sm font-bold"
            key={selection.column}
          >
            <input
              checked={selection.selected}
              disabled={disabled}
              type="checkbox"
              onChange={(event) =>
                onChange(
                  replaceAt(selections, index, {
                    ...selection,
                    selected: event.target.checked,
                  }),
                )
              }
            />
            <span>{selection.column}</span>
          </label>
        ))}
      </div>
    </section>
  );
}

function replaceAt<T>(items: T[], index: number, next: T): T[] {
  return items.map((item, currentIndex) => (currentIndex === index ? next : item));
}
