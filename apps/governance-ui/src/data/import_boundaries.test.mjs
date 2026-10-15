import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import ts from "typescript";

const root = fileURLToPath(new URL("../", import.meta.url));

function imports(source, filename) {
  const tree = ts.createSourceFile(
    filename,
    source,
    ts.ScriptTarget.Latest,
    true,
    filename.endsWith(".tsx") ? ts.ScriptKind.TSX : ts.ScriptKind.TS,
  );
  const references = [];
  function visit(node) {
    if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier) {
      if (ts.isStringLiteralLike(node.moduleSpecifier)) references.push(node.moduleSpecifier.text);
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference)
    ) {
      const expression = node.moduleReference.expression;
      if (expression && ts.isStringLiteralLike(expression)) references.push(expression.text);
    } else if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      assert.ok(
        node.arguments.length === 1 && ts.isStringLiteralLike(node.arguments[0]),
        `${filename}: dynamic imports must identify a module for boundary checking`,
      );
      references.push(node.arguments[0].text);
    }
    ts.forEachChild(node, visit);
  }
  visit(tree);
  return references;
}

function violations(filename, source) {
  return imports(source, filename).flatMap((specifier) => {
    if (!specifier.startsWith(".")) return [];
    const target = path.posix.normalize(path.posix.join(path.posix.dirname(filename), specifier));
    const forbidden =
      (filename.startsWith("data/") && /^(app|features)\//.test(target)) ||
      (filename.startsWith("features/") && /^app\/(main|Workspace)(\.|$)/.test(target)) ||
      (/^(features\/(connections|settings))\//.test(filename) &&
        target.startsWith("features/activity/"));
    return forbidden ? [`${filename} -> ${target}`] : [];
  });
}

async function sources(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map((entry) => {
      const filename = path.join(directory, entry.name);
      if (entry.isDirectory()) return sources(filename);
      return /\.tsx?$/.test(entry.name) && !entry.name.endsWith(".stories.tsx") ? [filename] : [];
    }),
  );
  return nested.flat();
}

test("parser checks import, re-export and dynamic import boundaries", () => {
  for (const source of [
    'import type { View } from "../features/activity/ActivityView";',
    'export { View } from "../features/activity/ActivityView";',
    'const view = import("../features/activity/ActivityView");',
    'import view = require("../features/activity/ActivityView");',
  ])
    assert.deepEqual(violations("data/client.ts", source), [
      "data/client.ts -> features/activity/ActivityView",
    ]);
  assert.deepEqual(violations("features/settings/view.tsx", 'import("../activity/ActivityView")'), [
    "features/settings/view.tsx -> features/activity/ActivityView",
  ]);
  assert.deepEqual(violations("features/assets/view.tsx", 'export * from "../../app/Workspace"'), [
    "features/assets/view.tsx -> app/Workspace",
  ]);
  assert.deepEqual(violations("app/Workspace.tsx", 'import("../features/assets/AssetRoute")'), []);
  assert.deepEqual(violations("data/client.ts", 'import { api } from "./api";'), []);
  assert.deepEqual(
    imports('// import("../features/pretend")\nconst text = "import(hello)";', "data/client.ts"),
    [],
  );
});

test("production UI modules follow feature ownership", async () => {
  const findings = [];
  for (const filename of await sources(root)) {
    findings.push(
      ...violations(
        path.relative(root, filename).split(path.sep).join("/"),
        await readFile(filename, "utf8"),
      ),
    );
  }
  assert.deepEqual(findings, []);
});
