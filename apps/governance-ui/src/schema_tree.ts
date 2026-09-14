import type { SchemaNode } from "./api";

export type FlatSchemaNode = {
  node: SchemaNode;
  depth: number;
  index: number;
  posinset: number;
  setsize: number;
};

/** Flattens only visible schema nodes for the virtual tree viewport. */
export function flattenSchemaTree(
  nodes: SchemaNode[],
  expanded: ReadonlySet<string>,
  forceExpanded = false,
): FlatSchemaNode[] {
  const result: FlatSchemaNode[] = [];
  const visit = (node: SchemaNode, depth: number, posinset: number, setsize: number) => {
    result.push({ node, depth, index: result.length, posinset, setsize });
    if (node.children?.length && (forceExpanded || expanded.has(node.human_path))) {
      node.children.forEach((child, index) => visit(child, depth + 1, index + 1, node.children!.length));
    }
  };
  nodes.forEach((node, index) => visit(node, 0, index + 1, nodes.length));
  return result;
}
