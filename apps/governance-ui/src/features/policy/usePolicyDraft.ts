import { useRef, useState } from "react";
import type { ApiFieldError, PolicyRule } from "../../data/api";
import {
  emptyPolicyDraft,
  reducePolicyDraft,
  type PolicyDraftAction,
  type PolicyDraft,
  type PolicySubmission,
} from "./policy_draft";

export function usePolicyDraft(initial: Pick<PolicyDraft, "rules" | "revision">) {
  const [state, setState] = useState(() => reducePolicyDraft(emptyPolicyDraft(), { type: "load", ...initial }));
  const current = useRef(state);
  function dispatch(action: PolicyDraftAction) {
    const next = reducePolicyDraft(current.current, action);
    // Async continuations must observe edits immediately, before React commits.
    current.current = next;
    setState(next);
  }
  return {
    ...state,
    dispatch,
    current: () => current.current,
    update(index: number, change: (rule: PolicyRule) => PolicyRule) {
      const rule = current.current.rules[index];
      if (rule)
        dispatch({
          type: "update",
          index,
          rule: change(structuredClone(rule)),
        });
    },
    beginSave(): PolicySubmission | null {
      if (current.current.pending) return null;
      dispatch({ type: "beginSave" });
      return current.current.pending;
    },
    saved(submission: PolicySubmission, revision: number): boolean {
      const applies =
        current.current.pending === submission &&
        current.current.editVersion === submission.editVersion;
      dispatch({ type: "saved", submission, revision });
      return applies;
    },
    failed(submission: PolicySubmission, errors: ApiFieldError[]): boolean {
      const applies =
        current.current.pending === submission &&
        current.current.editVersion === submission.editVersion;
      dispatch({ type: "failed", submission, errors });
      return applies;
    },
  };
}
