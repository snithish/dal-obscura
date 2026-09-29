import type { SchemaNode } from "./api";

export type RowFilterOperator = "equals" | "not_equals" | "greater" | "less" | "greater_equal" | "less_equal" | "in" | "is_null" | "is_not_null";
export type RowFilterCondition = { field: SchemaNode; operator: RowFilterOperator; value?: string | string[] };
export type RowFilterCompileResult = { sql: string | null; error?: string };

export function rowFilterType(type: string): "string" | "number" | "boolean" | undefined {
  const normalized = type.toLowerCase().replace(/\s+/g, "");
  if (["string", "varchar", "text", "utf8", "large_string"].includes(normalized)) return "string";
  if (["int", "integer", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float", "float32", "float64", "double", "decimal"].includes(normalized) || /^(u?int|float|decimal)\d/.test(normalized)) return "number";
  if (["bool", "boolean"].includes(normalized)) return "boolean";
  return undefined;
}

function identifier(field: SchemaNode): string | undefined {
  const segments = field.path.segments;
  if (!segments.length || segments.some((segment) => segment.kind !== "field" || !segment.name)) return undefined;
  return segments.map((segment) => `"${segment.name!.replaceAll('"', '""')}"`).join(".");
}

function literal(value: string, type: "string" | "number" | "boolean"): string | undefined {
  if (type === "string") return `'${value.replaceAll("'", "''")}'`;
  if (type === "number") {
    const trimmed = value.trim();
    if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(trimmed) || !Number.isFinite(Number(trimmed))) return undefined;
    return trimmed;
  }
  if (value === "true" || value === "false") return value.toUpperCase();
  return undefined;
}

export function compileRowFilter(conditions: RowFilterCondition[], join: "AND" | "OR" = "AND"): RowFilterCompileResult {
  if (!conditions.length) return { sql: null, error: "Add at least one complete condition." };
  const rendered: string[] = [];
  for (const condition of conditions) {
    const column = identifier(condition.field);
    const type = rowFilterType(condition.field.type);
    if (!column || !type) return { sql: null, error: `Builder does not support ${condition.field.human_path} yet.` };
    if (condition.operator === "is_null" || condition.operator === "is_not_null") {
      rendered.push(`(${column} IS ${condition.operator === "is_not_null" ? "NOT " : ""}NULL)`);
      continue;
    }
    if (condition.operator === "in") {
      if (!Array.isArray(condition.value) || condition.value.length === 0) return { sql: null, error: "Provide at least one value for is one of." };
      if (condition.value.some((value) => !value.trim())) return { sql: null, error: "Complete every value in the list before applying." };
      const values = condition.value.map((value) => literal(value, type));
      if (values.some((value) => value === undefined)) return { sql: null, error: "Enter a valid condition value." };
      rendered.push(`(${column} IN (${values.join(", ")}))`);
      continue;
    }
    if (Array.isArray(condition.value) || condition.value === undefined || condition.value === "") return { sql: null, error: "Complete every condition before saving." };
    const value = literal(condition.value, type);
    if (value === undefined) return { sql: null, error: type === "number" ? "Enter a finite number." : "Choose true or false." };
    const operators: Record<Exclude<RowFilterOperator, "in" | "is_null" | "is_not_null">, string> = {
      equals: "=", not_equals: "<>", greater: ">", less: "<", greater_equal: ">=", less_equal: "<=",
    };
    if (type === "boolean" && !["equals", "not_equals"].includes(condition.operator)) return { sql: null, error: "Boolean fields support equals and does not equal only." };
    rendered.push(`(${column} ${operators[condition.operator as keyof typeof operators]} ${value})`);
  }
  return { sql: rendered.join(` ${join} `) };
}
