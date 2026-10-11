import { performance } from 'node:perf_hooks';
import assert from 'node:assert/strict';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
const moduleUrl = process.argv[2] ? pathToFileURL(resolve(process.argv[2])) : new URL('../src/policy_editor.ts', import.meta.url);
const { applyMaskToTargets, removeColumnSelections } = await import(moduleUrl);
const count = 300;
const options = Array.from({ length: count }, (_, index) => {
  const segment = { kind: 'field', name: `parent_${index}`, field_id: index + 1 };
  return [
    { value: `parent_${index}`, path: { version: 1, segments: [segment] } },
    { value: `parent_${index}.value`, path: { version: 1, segments: [segment, { kind: 'field', name: 'value', field_id: count + index + 1 }] } },
  ];
}).flat();
const columns = options.filter((_, index) => index % 2 === 0).map(o => o.value);
const targets = options.filter((_, index) => index % 2 === 1).map(o => o.value);
const input = { columns, masks: Object.fromEntries(targets.map(value => [value, { type: 'hash' }])) };
for (const [name, action] of [
  ['apply', () => applyMaskToTargets(input, targets, { type: 'redact' }, [], options)],
  ['remove', () => removeColumnSelections(input, columns, options)],
]) {
  const samples = [];
  for (let repeat = 0; repeat < 3; repeat++) {
    const start = performance.now();
    const output = action();
    samples.push(performance.now() - start);
    assert.deepEqual(output.masks, name === 'apply' ? Object.fromEntries(targets.map(value => [value, { type: 'redact' }])) : {});
    assert.deepEqual(output.columns, name === 'apply' ? columns : []);
  }
  console.log(JSON.stringify({ name, count, node: process.version, medianMs: samples.sort((a,b)=>a-b)[1], samplesMs: samples }));
}
