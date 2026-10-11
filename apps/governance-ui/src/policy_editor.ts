import type { Mask, PolicyRule, SchemaNode, SchemaPath } from "./api";

export type ColumnOption = {
  value: string;
  label: string;
  type: string;
  path?: SchemaPath;
  valid: boolean;
};

export type MaskGroup = { mask?: Mask; columns: string[] };

/** Missing or removed fields remain visible without fabricating schema paths. */
export function authoritativeColumnOptions(fields: SchemaNode[] | undefined, selected: string[] = []): ColumnOption[] {
  if (!fields) return selected.map((value) => ({ value, label: value, type: "Unavailable", valid: false }));
  const options: ColumnOption[] = [];
  const pending = [...fields].reverse();
  while (pending.length) {
    const node = pending.pop()!;
    options.push({ value: node.human_path, label: node.human_path, type: node.type, path: node.path, valid: true });
    for (let index = (node.children?.length ?? 0) - 1; index >= 0; index -= 1) pending.push(node.children![index]);
  }
  const known = new Set(options.map((option) => option.value));
  for (const value of selected) if (!known.has(value)) options.push({ value, label: value, type: "Removed from schema", valid: false });
  return options;
}

function pathSegments(path: SchemaPath | undefined): string[] | undefined {
  if (!path || !path.segments.length) return undefined;
  return path.segments.map((segment) => JSON.stringify([segment.kind, segment.name ?? null, segment.field_id ?? null]));
}

/** Typed schema ancestry; display strings are never split on dots. */
export function schemaPathCovers(parent: string, child: string, options: ColumnOption[]): boolean {
  if (parent === child) return true;
  const byName = new Map(options.map((option) => [option.value, option]));
  return columnOptionCovers(byName.get(parent), byName.get(child));
}

/** Typed ancestry without rebuilding lookup maps for every rendered row. */
export function columnOptionCovers(parent: ColumnOption | undefined, child: ColumnOption | undefined): boolean {
  if (!parent || !child) return false;
  if (parent.value === child.value) return true;
  const parentSegments = pathSegments(parent.path);
  const childSegments = pathSegments(child.path);
  return Boolean(parentSegments && childSegments && parentSegments.length < childSegments.length && parentSegments.every((part, index) => part === childSegments[index]));
}

function stable(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => `${JSON.stringify(key)}:${stable(item)}`).join(",")}}`;
  return JSON.stringify(value);
}

function maskKey(mask: Mask): string {
  return stable({ ...mask, exempt_principals: [...new Set(mask.exempt_principals ?? [])].sort() });
}

/** Index grant coverage once per edit, retaining exact typed ancestry. */
function selectionCoverage(values: string[], options: ColumnOption[]) {
  const exact = new Set(values);
  const paths = new Map(options.map((option) => [option.value, pathSegments(option.path)]));
  const selectedPaths = new Set<string>();
  const selectedAncestors = new Set<string>();
  for (const value of exact) {
    const segments = paths.get(value);
    if (!segments) continue;
    selectedPaths.add(JSON.stringify(segments));
    for (let depth = 1; depth < segments.length; depth += 1) selectedAncestors.add(JSON.stringify(segments.slice(0, depth)));
  }
  return (value: string, includeDescendants = false): boolean => {
    if (exact.has(value)) return true;
    const segments = paths.get(value);
    if (!segments) return false;
    if (includeDescendants && selectedAncestors.has(JSON.stringify(segments))) return true;
    for (let depth = 1; depth < segments.length; depth += 1) {
      if (selectedPaths.has(JSON.stringify(segments.slice(0, depth)))) return true;
    }
    return false;
  };
}

export function groupMasks(rule: PolicyRule, columns: string[]): MaskGroup[] {
  const groups = new Map<string, MaskGroup>();
  for (const column of columns) {
    const mask = rule.masks[column];
    const key = mask === undefined ? "<unmasked>" : maskKey(mask);
    const group = groups.get(key) ?? { ...(mask === undefined ? {} : { mask }), columns: [] };
    group.columns.push(column);
    groups.set(key, group);
  }
  return [...groups.values()];
}

/** Applies only to explicit targets; access and unrelated rule metadata stay intact. */
export function applyMaskToTargets(
  rule: PolicyRule,
  targets: string[],
  mask: Mask | undefined,
  excluded: string[] = [],
  options: ColumnOption[] = [],
): PolicyRule {
  const covered = selectionCoverage(rule.columns, options);
  const targetSet = new Set(targets.filter((column) => covered(column)));
  const excludedSet = new Set(excluded.filter((column) => targetSet.has(column)));
  const masks = { ...rule.masks };
  for (const column of targetSet) {
    if (!mask || excludedSet.has(column)) delete masks[column];
    else masks[column] = { ...mask };
  }
  return { ...rule, masks };
}

/** Removes explicit grants and masks no longer covered by any remaining grant. */
export function removeColumnSelections(rule: PolicyRule, removed: string[], options: ColumnOption[]): PolicyRule {
  const removedSet = new Set(removed);
  const remaining = rule.columns.filter((column) => !removedSet.has(column));
  const masks = { ...rule.masks };
  const coveredBefore = selectionCoverage(rule.columns, options);
  const coveredAfter = selectionCoverage(remaining, options);
  for (const maskPath of Object.keys(masks)) {
    if (coveredBefore(maskPath, true) && !coveredAfter(maskPath, true)) delete masks[maskPath];
  }
  return { ...rule, columns: remaining, masks };
}

/** Expand selections to schema leaves; never infer hierarchy by splitting display paths. */
export function leafColumnOptions(options: ColumnOption[]): ColumnOption[] {
  const parents = new Set<string>();
  for (const option of options) {
    if (!option.valid) continue;
    const segments = pathSegments(option.path) ?? [];
    for (let depth = 1; depth < segments.length; depth += 1) parents.add(JSON.stringify(segments.slice(0, depth)));
  }
  return options.filter((option) => option.valid && !parents.has(JSON.stringify(pathSegments(option.path) ?? [])));
}

/** Index descendants once so wide pickers do not compare every pair of fields. */
export function columnLeafPaths(options: ColumnOption[]): Map<string, string[]> {
  const byTypedPath = new Map<string, ColumnOption[]>();
  const result = new Map<string, string[]>();
  for (const option of options) {
    if (!option.valid) continue;
    const key = JSON.stringify(pathSegments(option.path) ?? []);
    byTypedPath.set(key, [...(byTypedPath.get(key) ?? []), option]);
    result.set(option.value, []);
  }
  for (const leaf of leafColumnOptions(options)) {
    result.get(leaf.value)!.push(leaf.value);
    const segments = pathSegments(leaf.path) ?? [];
    for (let depth = 1; depth < segments.length; depth += 1) {
      for (const parent of byTypedPath.get(JSON.stringify(segments.slice(0, depth))) ?? []) result.get(parent.value)!.push(leaf.value);
    }
  }
  return result;
}

export function selectColumnsShortcut(options: ColumnOption[], mode: "all" | "except" | "prefix", value = ""): string[] {
  const excluded = options.find((option) => option.value === value);
  return leafColumnOptions(options).filter((option) => mode === "all" || (mode === "except" ? !columnOptionCovers(excluded, option) : option.value.startsWith(value))).map((option) => option.value);
}

export function expandColumnSelections(options: ColumnOption[], values: string[]): string[] {
  const leaves = columnLeafPaths(options);
  return [...new Set(values.flatMap((value) => {
    const matches = leaves.get(value) ?? [];
    return matches.length ? matches : [value];
  }))];
}
