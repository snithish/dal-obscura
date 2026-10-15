import type { SchemaNode } from "../../data/api";

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
  const stack = nodes
    .map((node, index) => ({ node, depth: 0, posinset: index + 1, setsize: nodes.length }))
    .reverse();
  while (stack.length) {
    const { node, depth, posinset, setsize } = stack.pop()!;
    result.push({ node, depth, index: result.length, posinset, setsize });
    if (node.children?.length && (forceExpanded || expanded.has(node.human_path))) {
      for (let index = node.children.length - 1; index >= 0; index--) {
        stack.push({
          node: node.children[index],
          depth: depth + 1,
          posinset: index + 1,
          setsize: node.children.length,
        });
      }
    }
  }
  return result;
}
