import { type PolicyPreview } from "./policyLogic";

export type PreviewClaimRow = {
  key: string;
  value: string;
};

export function PreviewPanel({
  claims,
  groupsText,
  onClaimsChange,
  onGroupsTextChange,
  onPrincipalChange,
  preview,
  principal,
}: {
  claims: PreviewClaimRow[];
  groupsText: string;
  onClaimsChange: (rows: PreviewClaimRow[]) => void;
  onGroupsTextChange: (value: string) => void;
  onPrincipalChange: (value: string) => void;
  preview: PolicyPreview;
  principal: string;
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-4">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
        <div>
          <h3 className="text-base font-black">Policy preview</h3>
          <p className="mt-1 text-sm leading-6 text-muted">
            Test a principal and claims against the draft rules before publishing.
          </p>
        </div>
        <span className="badge">{preview.decision}</span>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Principal
          </span>
          <input
            className="field mt-2"
            value={principal}
            onChange={(event) => onPrincipalChange(event.target.value)}
          />
        </label>
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Groups
          </span>
          <input
            className="field mt-2"
            value={groupsText}
            onChange={(event) => onGroupsTextChange(event.target.value)}
          />
          <span className="mt-1 block text-xs text-muted">
            Comma-separated group names without the group: prefix.
          </span>
        </label>
      </div>

      <section className="mt-4 rounded-card border border-border bg-white p-3">
        <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
          <div>
            <h4 className="text-sm font-black">Preview claims</h4>
            <p className="mt-1 text-xs text-muted">
              Add identity claims used by rule conditions.
            </p>
          </div>
          <button
            className="btn-secondary"
            type="button"
            onClick={() => onClaimsChange([...claims, { key: "", value: "" }])}
          >
            Add claim
          </button>
        </div>
        <div className="mt-3 grid gap-3">
          {claims.length === 0 ? (
            <div className="rounded-card border border-dashed border-border bg-soft p-4 text-sm text-muted">
              No preview claims
            </div>
          ) : (
            claims.map((claim, index) => (
              <div
                className="grid grid-cols-1 gap-3 rounded-card border border-border bg-soft p-3 md:grid-cols-[1fr_1fr_auto]"
                key={`${claim.key}-${index}`}
              >
                <label className="block">
                  <span className="text-xs font-black uppercase tracking-wide text-muted">
                    Claim
                  </span>
                  <input
                    className="field mt-2"
                    value={claim.key}
                    onChange={(event) =>
                      onClaimsChange(
                        replaceAt(claims, index, { ...claim, key: event.target.value }),
                      )
                    }
                  />
                </label>
                <label className="block">
                  <span className="text-xs font-black uppercase tracking-wide text-muted">
                    Value
                  </span>
                  <input
                    className="field mt-2"
                    value={claim.value}
                    onChange={(event) =>
                      onClaimsChange(
                        replaceAt(claims, index, { ...claim, value: event.target.value }),
                      )
                    }
                  />
                </label>
                <button
                  className="btn-secondary self-end"
                  type="button"
                  onClick={() =>
                    onClaimsChange(claims.filter((_, currentIndex) => currentIndex !== index))
                  }
                >
                  Remove
                </button>
              </div>
            ))
          )}
        </div>
      </section>

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
        <PreviewCard label="Matched" value={preview.reason} />
        <PreviewCard label="Row filter" value={preview.rowFilter ?? "None"} />
        <PreviewCard
          label="Masks"
          value={
            preview.masks.length > 0
              ? preview.masks.map((mask) => `${mask.column}: ${mask.type}`).join(", ")
              : "None"
          }
        />
      </div>
      <div className="mt-3 rounded-card border border-border bg-white p-3">
        <span className="text-xs font-black uppercase tracking-wide text-muted">
          Visible columns
        </span>
        <div className="mt-2 flex flex-wrap gap-2">
          {preview.visibleColumns.length === 0 ? (
            <span className="text-sm text-muted">No columns visible</span>
          ) : (
            preview.visibleColumns.map((column) => (
              <span className="badge" key={column}>
                {column}
              </span>
            ))
          )}
        </div>
      </div>
    </section>
  );
}

function PreviewCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-card border border-border bg-white p-3">
      <span className="text-xs font-black text-muted">{label}</span>
      <strong className="mt-1 block break-words text-sm">{value}</strong>
    </div>
  );
}

export function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-card border border-border bg-soft p-3">
      <span className="text-xs font-black text-muted">{label}</span>
      <strong className="mt-1 block truncate text-sm">{value}</strong>
    </div>
  );
}

function replaceAt<T>(items: T[], index: number, next: T): T[] {
  return items.map((item, currentIndex) => (currentIndex === index ? next : item));
}
