import type { ApiFieldError, PolicyRule } from "./api";

export type SaveState = "saved" | "saving" | "unsaved" | "failed";
export type PolicySubmission = {
  rules: PolicyRule[];
  revision: number;
  editVersion: number;
};
export type PolicyDraft = {
  rules: PolicyRule[];
  saved: PolicyRule[];
  revision: number;
  editVersion: number;
  status: SaveState;
  errors: ApiFieldError[];
  pending: PolicySubmission | null;
};
export type PolicyDraftAction =
  | { type: "load"; rules: PolicyRule[]; revision: number }
  | { type: "clear" | "discard" | "add" | "allowAll" | "beginSave" }
  | { type: "update"; index: number; rule: PolicyRule }
  | { type: "remove" | "duplicate"; index: number }
  | { type: "saved"; submission: PolicySubmission; revision: number }
  | { type: "failed"; submission: PolicySubmission; errors: ApiFieldError[] }
  | { type: "cancelSave"; submission: PolicySubmission };

export function emptyPolicyDraft(editVersion = 0): PolicyDraft {
  return {
    rules: [],
    saved: [],
    revision: 0,
    editVersion,
    status: "saved",
    errors: [],
    pending: null,
  };
}

const nextOrdinal = (rules: PolicyRule[]) =>
  Math.max(0, ...rules.map((rule) => rule.ordinal)) + 10;
const newRule = (ordinal: number): PolicyRule => ({
  ordinal,
  effect: "allow",
  name: "",
  description: "",
  principals: [],
  columns: [],
  masks: {},
  row_filter: null,
  when: {},
});

/** One transition owns the draft, saved baseline, revision, and in-flight save. */
export function reducePolicyDraft(
  state: PolicyDraft,
  action: PolicyDraftAction,
): PolicyDraft {
  switch (action.type) {
    case "clear":
      return emptyPolicyDraft(state.editVersion + 1);
    case "load":
      return {
        ...emptyPolicyDraft(state.editVersion + 1),
        rules: structuredClone(action.rules),
        saved: structuredClone(action.rules),
        revision: action.revision,
      };
    case "discard":
      return {
        ...state,
        rules: structuredClone(state.saved),
        editVersion: state.editVersion + 1,
        status: "saved",
        errors: [],
      };
    case "beginSave":
      if (state.pending) return state;
      return {
        ...state,
        status: "saving",
        errors: [],
        pending: {
          rules: structuredClone(state.rules),
          revision: state.revision,
          editVersion: state.editVersion,
        },
      };
    case "saved":
      if (state.pending !== action.submission) return state;
      return {
        ...state,
        saved: action.submission.rules,
        revision: action.revision,
        pending: null,
        status:
          state.editVersion === action.submission.editVersion
            ? "saved"
            : "unsaved",
        errors: [],
      };
    case "failed":
      if (state.pending !== action.submission) return state;
      return {
        ...state,
        pending: null,
        status:
          state.editVersion === action.submission.editVersion
            ? "failed"
            : "unsaved",
        errors:
          state.editVersion === action.submission.editVersion
            ? action.errors
            : [],
      };
    case "cancelSave":
      return state.pending === action.submission
        ? { ...state, pending: null, status: "unsaved" }
        : state;
  }

  let rules: PolicyRule[];
  switch (action.type) {
    case "add":
      rules = [...state.rules, newRule(nextOrdinal(state.rules))];
      break;
    case "allowAll":
      rules = [
        ...state.rules.filter((rule) => rule.effect !== "allow_all"),
        {
          ...newRule(nextOrdinal(state.rules)),
          effect: "allow_all",
          name: "Allow all",
          description:
            "All authenticated readers receive all columns and rows without masks.",
          principals: ["*"],
          columns: ["*"],
        },
      ];
      break;
    case "duplicate": {
      if (!state.rules[action.index]) return state;
      const duplicate = structuredClone(state.rules[action.index]);
      rules = [
        ...state.rules,
        { ...duplicate, ordinal: nextOrdinal(state.rules) },
      ];
      break;
    }
    case "remove":
      if (!state.rules[action.index]) return state;
      rules = state.rules.filter((_, index) => index !== action.index);
      break;
    case "update":
      if (!state.rules[action.index]) return state;
      rules = state.rules.map((rule, index) =>
        index === action.index ? action.rule : rule,
      );
      break;
  }
  return {
    ...state,
    rules,
    editVersion: state.editVersion + 1,
    status: "unsaved",
    errors: [],
  };
}
