import type { SchemaNode } from "./api";

export type FlatSchemaNode = { node: SchemaNode; depth: number; index: number };

/** Flattens only visible schema nodes for the virtual tree viewport. */
export function flattenSchemaTree(
  nodes: SchemaNode[],
  expanded: ReadonlySet<string>,
  forceExpanded = false,
): FlatSchemaNode[] {
  const result: FlatSchemaNode[] = [];
  const visit = (node: SchemaNode, depth: number) => {
    result.push({ node, depth, index: result.length });
    if (node.children?.length && (forceExpanded || expanded.has(node.human_path))) {
      node.children.forEach((child) => visit(child, depth + 1));
    }
  };
  nodes.forEach((node) => visit(node, 0));
  return result;
}
