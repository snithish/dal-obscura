import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Checkbox, Modal, TextInput } from "@mantine/core";
import type { PolicyRule, SchemaNode } from "../../data/api";
import { authoritativeColumnOptions, createRuleCoverage, groupMasks } from "./policy_editor";
import { simulateMask } from "./value_lens";
import { RuleColumnSummary } from "./RuleColumnSummary";
import { COLUMN_ROW_HEIGHT, COLUMN_VIEW_HEIGHT } from "./columns/virtualization";

export function RuleInsights({
  rule,
  fields,
}: {
  rule: PolicyRule;
  fields: SchemaNode[] | undefined;
}) {
  const [open, setOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const options = useMemo(() => authoritativeColumnOptions(fields), [fields]);
  const coverage = useMemo(() => createRuleCoverage(rule, options), [rule, options]);
  if (rule.effect === "allow_all" || !fields) return null;
  return (
    <section className="rule-insights" aria-label="Rule mask coverage">
      <h3>Mask coverage</h3>
      <p className="help">
        Selected schema leaves in this rule. No mask returns stored values when requested. Tallies
        do not evaluate audiences or exemptions.
      </p>
      <RuleColumnSummary rule={coverage.rule} aggregate />
      {coverage.conflicts.size > 0 && (
        <p className="field-error" role="alert">
          {coverage.conflicts.size} conflicting leaf masks. Use the saved-policy test for
          validation.
        </p>
      )}
      <Button ref={trigger} variant="default" onClick={() => setOpen(true)}>
        Open value lens
      </Button>
      <Modal
        attributes={{ header: { role: "presentation" } }}
        opened={open}
        onClose={() => {
          setOpen(false);
        }}
        onExitTransitionEnd={() => trigger.current?.focus()}
        closeButtonProps={{ "aria-label": "Close value lens" }}
        title="Value lens"
        size={900}
      >
        {open && <LensContent options={options} coverage={coverage} />}
      </Modal>
    </section>
  );
}

function LensContent({
  options,
  coverage,
}: {
  options: ReturnType<typeof authoritativeColumnOptions>;
  coverage: ReturnType<typeof createRuleCoverage>;
}) {
  const [scope, setScope] = useState("");
  const [query, setQuery] = useState("");
  const [sample, setSample] = useState("sample@example.invalid");
  const [nullSample, setNullSample] = useState(false);
  const [scrollTop, setScrollTop] = useState(0);
  const viewport = useRef<HTMLDivElement>(null);
  const [values, setValues] = useState<Record<string, unknown>>({});
  const granted = useMemo(
    () => new Set([...coverage.rule.columns, ...coverage.conflicts]),
    [coverage],
  );
  const rows = useMemo(() => {
    const allowed = scope ? new Set(coverage.descendants.get(scope) ?? []) : undefined;
    const term = query.trim().toLowerCase();
    return options.filter(
      (option) =>
        option.value.toLowerCase().includes(term) &&
        (!allowed ||
          option.value === scope ||
          (coverage.descendants.get(option.value) ?? []).some((leaf) => allowed.has(leaf))),
    );
  }, [options, coverage, scope, query]);
  const start = Math.max(0, Math.floor(scrollTop / COLUMN_ROW_HEIGHT) - 2);
  const end = Math.min(
    rows.length,
    Math.ceil((scrollTop + COLUMN_VIEW_HEIGHT) / COLUMN_ROW_HEIGHT) + 2,
  );
  const visible = useMemo(() => rows.slice(start, end), [rows, start, end]);
  useEffect(() => {
    setScrollTop(0);
    if (viewport.current) viewport.current.scrollTop = 0;
  }, [scope, query]);
  useEffect(() => {
    let current = true;
    setValues({});
    void Promise.all(
      visible.map(async (option) => {
        const children = coverage.descendants.get(option.value) ?? [];
        if (children.length !== 1 || children[0] !== option.value) return [option.value, undefined];
        return [
          option.value,
          coverage.conflicts.has(option.value)
            ? "Conflicting masks"
            : granted.has(option.value)
              ? await simulateMask(nullSample ? null : sample, coverage.rule.masks[option.value])
              : null,
        ];
      }),
    ).then((entries) => {
      if (current) setValues(Object.fromEntries(entries));
    });
    return () => {
      current = false;
    };
  }, [visible, coverage, granted, sample, nullSample]);
  return (
    <>
      <p className="help">
        Client-side simulation of this draft rule using a synthetic text sample, not server
        evaluation. Audiences, row filters, mask exemptions, other rules, and Arrow type conversions
        are not evaluated. No source data is loaded.
      </p>
      <TextInput
        label="Synthetic sample value"
        value={sample}
        onChange={(event) => setSample(event.currentTarget.value)}
      />
      <Checkbox
        label="Use a NULL sample"
        checked={nullSample}
        onChange={(event) => setNullSample(event.currentTarget.checked)}
      />
      <TextInput
        type="search"
        label="Find schema path"
        value={query}
        onChange={(event) => setQuery(event.currentTarget.value)}
      />
      <Button variant="subtle" onClick={() => setScope("")}>
        All schema paths
      </Button>
      {scope && <p role="status">Subtree: {scope}</p>}
      <div className="value-lens-head">
        <span>Field path / subtree mask state</span>
        <span>Simulated value · Returned as stored — no mask in this rule</span>
      </div>
      <div
        ref={viewport}
        className="value-lens-list"
        role="region"
        aria-label="Synthetic field values"
        tabIndex={0}
        onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
        style={{ height: COLUMN_VIEW_HEIGHT }}
      >
        <div style={{ height: rows.length * COLUMN_ROW_HEIGHT, position: "relative" }}>
          {visible.map((option, index) => {
            const children = coverage.descendants.get(option.value) ?? [];
            const parent = children.length !== 1 || children[0] !== option.value;
            const groups = parent
              ? groupMasks(
                  coverage.rule,
                  children.filter((leaf) => granted.has(leaf) && !coverage.conflicts.has(leaf)),
                )
              : [];
            return (
              <div
                key={option.value}
                className="value-lens-row"
                style={{
                  position: "absolute",
                  top: (start + index) * COLUMN_ROW_HEIGHT,
                  height: COLUMN_ROW_HEIGHT,
                }}
              >
                {parent ? (
                  <Button
                    variant="subtle"
                    onClick={() => {
                      setScope(option.value);
                      setQuery("");
                    }}
                  >
                    {option.value}
                  </Button>
                ) : (
                  <code title={option.value}>{option.value}</code>
                )}
                <span>
                  {parent ? (
                    <span className="lens-swatch" aria-label="Subtree mask state">
                      {children.length} leaves ·{" "}
                      {groups
                        .map((group) => `${group.columns.length} ${group.mask?.type ?? "no mask"}`)
                        .join(", ") || "not selected"}
                    </span>
                  ) : coverage.conflicts.has(option.value) ? (
                    "Conflicting masks"
                  ) : !granted.has(option.value) ? (
                    "Not selected in this rule (NULL default)"
                  ) : option.value in values ? (
                    JSON.stringify(values[option.value])
                  ) : (
                    "Calculating…"
                  )}
                </span>
              </div>
            );
          })}
        </div>
      </div>
      <p className="help">
        {rows.length.toLocaleString()} schema paths · at most 12 rows mounted. Parent rows show
        state summaries, never raw values.
      </p>
    </>
  );
}
