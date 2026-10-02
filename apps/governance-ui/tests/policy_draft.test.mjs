import assert from "node:assert/strict";
import test from "node:test";
import { emptyPolicyDraft, reducePolicyDraft } from "../src/policy_draft.ts";

const rule = {
  ordinal: 10, effect: "allow", principals: ["group:analysts"], columns: ["email"],
  masks: { email: { type: "hash", exempt_principals: ["privacy"] } },
  row_filter: null, when: { region: ["us"] },
};
const load = () => reducePolicyDraft(emptyPolicyDraft(), { type: "load", rules: [rule], revision: 3 });

test("save completion advances saved baseline while preserving edits made in flight", () => {
  let draft = load();
  draft = reducePolicyDraft(draft, { type: "add" });
  draft = reducePolicyDraft(draft, { type: "beginSave" });
  const submission = draft.pending;
  draft = reducePolicyDraft(draft, { type: "remove", index: 0 });
  draft = reducePolicyDraft(draft, { type: "saved", submission, revision: 4 });
  assert.equal(draft.revision, 4);
  assert.equal(draft.status, "unsaved");
  assert.equal(draft.rules.length, 1);
  assert.equal(draft.saved.length, 2);
  draft = reducePolicyDraft(draft, { type: "discard" });
  assert.deepEqual(draft.rules, submission.rules);
  assert.equal(draft.status, "saved");
});

test("old asset save cannot overwrite newly loaded policy or its revision", () => {
  let draft = reducePolicyDraft(load(), { type: "beginSave" });
  const submission = draft.pending;
  draft = reducePolicyDraft(draft, { type: "load", rules: [], revision: 0 });
  draft = reducePolicyDraft(draft, { type: "saved", submission, revision: 4 });
  assert.deepEqual(draft.rules, []);
  assert.equal(draft.revision, 0);
  assert.equal(draft.status, "saved");
});

test("duplicate and discard isolate nested masks, exemptions, and condition values", () => {
  let draft = reducePolicyDraft(load(), { type: "duplicate", index: 0 });
  const duplicate = structuredClone(draft.rules[1]);
  duplicate.masks.email.exempt_principals.push("new-reviewer");
  duplicate.when.region.push("eu");
  draft = reducePolicyDraft(draft, { type: "update", index: 1, rule: duplicate });
  assert.deepEqual(draft.rules[0], rule);
  draft = reducePolicyDraft(draft, { type: "discard" });
  assert.deepEqual(draft.rules, [rule]);
});

test("failed save preserves draft and exposes validation only for its submitted version", () => {
  let draft = reducePolicyDraft(load(), { type: "beginSave" });
  const submission = draft.pending;
  const errors = [{ field: "rules.0.principals", type: "value_error", message: "Select a principal" }];
  draft = reducePolicyDraft(draft, { type: "failed", submission, errors });
  assert.equal(draft.status, "failed");
  assert.deepEqual(draft.errors, errors);
  draft = reducePolicyDraft(draft, { type: "beginSave" });
  const next = draft.pending;
  draft = reducePolicyDraft(draft, { type: "add" });
  draft = reducePolicyDraft(draft, { type: "failed", submission: next, errors });
  assert.equal(draft.status, "unsaved");
  assert.deepEqual(draft.errors, []);
});
