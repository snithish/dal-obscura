import { useMemo } from "react";
import type { PolicyRule } from "../../data/api";
import { groupMasks } from "./policy_editor";
import { ColumnPathSummary } from "./masks/ColumnPathSummary";

const exposure = { keep_last: 1, email: 2, hash: 3, redact: 4, default: 5, null: 6 };

export function RuleColumnSummary({
  rule,
  aggregate = false,
}: {
  rule: PolicyRule;
  aggregate?: boolean;
}) {
  const columns = useMemo(() => [...new Set(rule.columns)], [rule.columns]);
  const groups = useMemo(
    () =>
      groupMasks(rule, columns).sort(
        (a, b) => (a.mask ? exposure[a.mask.type] : 0) - (b.mask ? exposure[b.mask.type] : 0),
      ),
    [rule, columns],
  );
  if (rule.effect === "allow_all") return null;
  return (
    <div className="rule-column-summary" aria-label="Rule column summary">
      {aggregate || columns.length > 20 ? (
        <div className="rule-column-tallies" role="list" aria-label="Mask-state tallies">
          {groups.map((group, index) => (
            <span
              role="listitem"
              className={`sem-chip ${!group.mask ? "tally-stored" : group.mask.type === "keep_last" || group.mask.type === "email" ? "tally-partial" : ""}`}
              key={index}
              title={
                group.mask ? JSON.stringify(group.mask) : "Returns stored values when requested"
              }
            >
              {group.columns.length.toLocaleString()} {group.mask?.type ?? "no mask"}
            </span>
          ))}
        </div>
      ) : (
        <div className={`rule-column-chips ${columns.length > 6 ? "clamped" : ""}`}>
          {columns.map((column) => (
            <span className="sem-chip" key={column}>
              <code>{column}</code>
              <span>{rule.masks[column]?.type ?? "no mask"}</span>
            </span>
          ))}
        </div>
      )}
      {columns.length > 6 && (
        <ColumnPathSummary columns={columns} label="columns" preview={false} />
      )}
    </div>
  );
}
