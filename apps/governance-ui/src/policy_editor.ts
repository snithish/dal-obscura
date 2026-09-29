import type { Mask, PolicyRule, SchemaNode, SchemaPath } from "./api";

export type ColumnOption = {
  value: string;
  label: string;
  type: string;
  path: SchemaPath;
  valid: boolean;
};

export type MaskGroup = { mask?: Mask; columns: string[] };

/** Schema is authoritative: legacy schema summaries are deliberately not used. */
export function authoritativeColumnOptions(fields: SchemaNode[] | undefined, selected: string[] = []): ColumnOption[] {
  if (!fields) return selected.map((value) => ({ value, label: value, type: "Unavailable", path: { version: 0, segments: [] }, valid: false }));
  const options: ColumnOption[] = [];
  const visit = (node: SchemaNode) => {
    options.push({ value: node.human_path, label: node.human_path, type: node.type, path: node.path, valid: true });
    node.children?.forEach(visit);
  };
  fields.forEach(visit);
  const known = new Set(options.map((option) => option.value));
  for (const value of selected) if (!known.has(value)) options.push({ value, label: value, type: "Removed from schema", path: { version: 0, segments: [] }, valid: false });
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
  const parentSegments = pathSegments(byName.get(parent)?.path);
  const childSegments = pathSegments(byName.get(child)?.path);
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

export function mixedMaskState(rule: PolicyRule, columns: string[]): boolean {
  if (columns.length < 2) return false;
  return new Set(columns.map((column) => rule.masks[column] === undefined ? "<unmasked>" : maskKey(rule.masks[column]))).size > 1;
}

/** Applies only to explicit targets; access and unrelated rule metadata stay intact. */
export function applyMaskToTargets(
  rule: PolicyRule,
  targets: string[],
  mask: Mask | undefined,
  excluded: string[] = [],
  options: ColumnOption[] = [],
): PolicyRule {
  const allowed = new Set(rule.columns);
  const targetSet = new Set(targets.filter((column) => allowed.has(column) || rule.columns.some((parent) => schemaPathCovers(parent, column, options))));
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
  const remaining = rule.columns.filter((column) => !removed.includes(column));
  const masks = { ...rule.masks };
  const overlaps = (selection: string, maskPath: string) => schemaPathCovers(selection, maskPath, options) || schemaPathCovers(maskPath, selection, options);
  for (const maskPath of Object.keys(masks)) {
    const coveredBefore = rule.columns.some((column) => overlaps(column, maskPath));
    const coveredAfter = remaining.some((column) => overlaps(column, maskPath));
    if (coveredBefore && !coveredAfter) delete masks[maskPath];
  }
  return { ...rule, columns: remaining, masks };
}

/** Expand selections to schema leaves; never infer hierarchy by splitting display paths. */
export function leafColumnOptions(options: ColumnOption[]): ColumnOption[] {
  return options.filter((option) => option.valid && !options.some((other) => other.valid && other.value !== option.value && schemaPathCovers(option.value, other.value, options)));
}

export function selectColumnsShortcut(options: ColumnOption[], mode: "all" | "except" | "prefix", value = ""): string[] {
  return leafColumnOptions(options).filter((option) => mode === "all" || (mode === "except" ? !schemaPathCovers(value, option.value, options) : option.value.startsWith(value))).map((option) => option.value);
}

export function expandColumnSelections(options: ColumnOption[], values: string[]): string[] {
  const leaves = leafColumnOptions(options);
  return [...new Set(values.flatMap((value) => {
    const matches = leaves.filter((leaf) => schemaPathCovers(value, leaf.value, options)).map((leaf) => leaf.value);
    return matches.length ? matches : [value];
  }))];
}
