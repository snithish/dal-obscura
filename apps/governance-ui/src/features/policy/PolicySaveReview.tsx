import { useEffect, useMemo, useState } from "react";
import { Button, Checkbox, Modal } from "@mantine/core";
import type { PolicyRule } from "../../data/api";
import { ColumnPathSummary } from "./masks/ColumnPathSummary";
import { policyChanges } from "./policy_review";

export function PolicySaveReview({
  opened,
  assetName,
  revision,
  saved,
  rules,
  canRevoke,
  disabled,
  onClose,
  onSave,
}: {
  opened: boolean;
  assetName: string;
  revision: number;
  saved: PolicyRule[];
  rules: PolicyRule[];
  canRevoke: boolean;
  disabled: boolean;
  onClose: () => void;
  onSave: (revoke: boolean) => void;
}) {
  const [revoke, setRevoke] = useState(false);
  const [page, setPage] = useState(0);
  useEffect(() => {
    if (!opened) {
      setRevoke(false);
      setPage(0);
    }
  }, [opened]);
  const changes = useMemo(() => policyChanges(saved, rules), [saved, rules]);
  const currentPage = Math.min(page, Math.max(0, Math.ceil(changes.length / 10) - 1));
  return (
    <Modal
      opened={opened}
      onClose={onClose}
      title="Review policy changes"
      size="xl"
      closeButtonProps={{ "aria-label": "Close policy review" }}
    >
      <p>
        <strong>{assetName}</strong>
      </p>
      <p>Based on saved revision {revision}</p>
      <p className="help">
        This compares authored settings, not effective access. The saved-policy test excludes
        unsaved changes.
      </p>
      {!changes.length && (
        <p>No authored settings changed. Saving still creates a policy revision.</p>
      )}
      {changes.slice(currentPage * 10, currentPage * 10 + 10).map((change) => (
        <section className="policy-review-change" key={change.ordinal}>
          <h3>
            {change.after?.name?.trim() || change.before?.name?.trim() || `Rule ${change.ordinal}`}
          </h3>
          <p className="context-tag">{change.kind}</p>
          {!!change.fields.length && (
            <ul>
              {change.fields.map((field) => (
                <li key={field}>{field}</li>
              ))}
            </ul>
          )}
          <div className="policy-review-versions">
            {(
              [
                ["Saved", change.before],
                ["Draft", change.after],
              ] as const
            ).map(([label, rule]) => (
              <details key={label} open={change.kind !== "updated" || undefined}>
                <summary>{label} settings</summary>
                {rule ? (
                  <>
                    <p>
                      <strong>Name:</strong> {rule.name || "Unnamed rule"}
                    </p>
                    <p>
                      <strong>Description:</strong> {rule.description || "None"}
                    </p>
                    <p>
                      <strong>Effect:</strong>{" "}
                      {rule.effect === "allow_all"
                        ? "Allow all authenticated readers; bypass all masks and row filters"
                        : "Allow selected columns"}
                    </p>
                    <p>
                      <strong>Audience:</strong>{" "}
                      <code>{rule.principals.join(", ") || "No audience"}</code>
                    </p>
                    <p>
                      <strong>Identity conditions:</strong>{" "}
                      <code>{JSON.stringify(rule.when ?? {})}</code>
                    </p>
                    <p>
                      <strong>Allowed columns:</strong> {rule.columns.length}
                    </p>
                    <ColumnPathSummary columns={rule.columns} label="selected columns" />
                    <p>
                      <strong>Column masks:</strong> {Object.keys(rule.masks).length}
                    </p>
                    <ColumnPathSummary
                      columns={Object.entries(rule.masks).map(
                        ([path, mask]) => `${path}: ${JSON.stringify(mask)}`,
                      )}
                      label="mask settings"
                    />
                    <p>
                      <strong>Row filter:</strong>{" "}
                      <code>{rule.row_filter || "All rows in this rule"}</code>
                    </p>
                  </>
                ) : (
                  <p>Rule absent</p>
                )}
              </details>
            ))}
          </div>
        </section>
      ))}
      {changes.length > 10 && (
        <div className="editor-actions">
          <Button
            variant="default"
            disabled={currentPage === 0}
            onClick={() => setPage(currentPage - 1)}
          >
            Previous rules
          </Button>
          <span>
            {currentPage * 10 + 1}–{Math.min(currentPage * 10 + 10, changes.length)} of{" "}
            {changes.length} changed rules
          </span>
          <Button
            variant="default"
            disabled={(currentPage + 1) * 10 >= changes.length}
            onClick={() => setPage(currentPage + 1)}
          >
            Next rules
          </Button>
        </div>
      )}
      <p className="help">
        New tickets use the saved policy. Existing tokens retain their captured policy until expiry
        or revocation.
      </p>
      {canRevoke && (
        <Checkbox
          label="Revoke existing tokens after saving"
          checked={revoke}
          onChange={(event) => setRevoke(event.currentTarget.checked)}
        />
      )}
      {canRevoke && revoke && (
        <p className="help">Existing readers must obtain new tickets after revocation.</p>
      )}
      <div className="editor-actions">
        <Button variant="default" data-autofocus onClick={onClose}>
          Keep editing
        </Button>
        <Button disabled={disabled} onClick={() => onSave(canRevoke && revoke)}>
          Save policy
        </Button>
      </div>
    </Modal>
  );
}
