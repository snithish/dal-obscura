import { type ReactNode, useEffect, useState } from "react";

import { apiGet, apiPost } from "../../api/client";
import {
  activationConfirmationLabel,
  canPublishDraft,
  publishBlockers,
} from "./publishLogic";

type WorkspaceSummary = {
  active_publication: {
    manifest_hash: string;
    publication_id: string;
    status: string;
  } | null;
  asset_count: number;
  catalog_count: number;
  draft_change_count: number;
  missing_policy_count: number;
  unowned_asset_count: number;
};

type Draft = {
  asset_count: number;
  assets: Array<{
    catalog: string;
    name: string;
    policy_status: string;
  }>;
  catalog_count: number;
  catalogs: Array<{
    name: string;
    status: string;
  }>;
};

type Publication = {
  active: boolean;
  id: string;
  manifest_hash: string;
  schema_version: number;
  status: string;
};

type CreatedPublication = {
  asset_count: number;
  catalog_count: number;
  manifest_hash: string;
  publication_id: string;
};

export function PublishPage() {
  const [summary, setSummary] = useState<WorkspaceSummary | null>(null);
  const [draft, setDraft] = useState<Draft | null>(null);
  const [publications, setPublications] = useState<Publication[]>([]);
  const [status, setStatus] = useState<string | null>(null);
  const [pendingAction, setPendingAction] = useState<"publish" | "activate" | null>(null);
  const [isPublishing, setIsPublishing] = useState(false);
  const [isActivating, setIsActivating] = useState(false);

  async function refresh() {
    const [loadedSummary, loadedDraft, loadedPublications] = await Promise.all([
      apiGet<WorkspaceSummary>("/v1/workspace/summary"),
      apiGet<Draft>("/v1/publications/draft"),
      apiGet<Publication[]>("/v1/publications"),
    ]);
    setSummary(loadedSummary);
    setDraft(loadedDraft);
    setPublications(loadedPublications);
  }

  useEffect(() => {
    void refresh().catch(() => setStatus("No publishable workspace has been configured yet."));
  }, []);

  async function publishDraft() {
    setStatus(null);
    setIsPublishing(true);
    try {
      const publication = await apiPost<CreatedPublication>("/v1/publications");
      await refresh();
      setStatus(`Publication ${publication.publication_id} created.`);
      setPendingAction(null);
    } catch {
      setStatus("Publish failed. Check runtime settings, catalogs, assets, and policies.");
    } finally {
      setIsPublishing(false);
    }
  }

  async function activatePublication(publicationId: string) {
    setStatus(null);
    setIsActivating(true);
    try {
      await apiPost(`/v1/publications/${publicationId}/activate`);
      await refresh();
      setStatus("Publication activated.");
      setPendingAction(null);
    } catch {
      setStatus("Activation failed.");
    } finally {
      setIsActivating(false);
    }
  }

  const latestPublication = publications[publications.length - 1];
  const readiness = {
    assetCount: draft?.asset_count ?? 0,
    catalogCount: draft?.catalog_count ?? 0,
    missingPolicyCount: summary?.missing_policy_count ?? 0,
    unownedAssetCount: summary?.unowned_asset_count ?? 0,
  };
  const blockers = publishBlockers(readiness);
  const canPublish = Boolean(draft && canPublishDraft(readiness));

  return (
    <div className="grid gap-6">
      <header className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">Release review</p>
          <h1 className="mt-1 text-3xl font-black">Publish</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Review draft configuration, create a publication, and activate the
            exact manifest the data plane should serve.
          </p>
        </div>
        <button
          className="btn-primary"
          disabled={!canPublish || isPublishing}
          onClick={() => setPendingAction("publish")}
          type="button"
        >
          {isPublishing ? "Publishing..." : "Review publish"}
        </button>
      </header>

      {status ? <div className="alert">{status}</div> : null}

      <section className="grid grid-cols-1 gap-3 md:grid-cols-4">
        <Metric label="Catalogs" value={draft?.catalog_count ?? 0} />
        <Metric label="Assets" value={draft?.asset_count ?? 0} />
        <Metric label="Missing policy" value={summary?.missing_policy_count ?? 0} />
        <Metric label="Draft changes" value={summary?.draft_change_count ?? 0} />
      </section>

      {blockers.length > 0 ? (
        <section className="surface border-l-4 border-l-[#c6802b] p-5">
          <h2 className="text-lg font-black">Publish blockers</h2>
          <div className="mt-3 grid gap-2">
            {blockers.map((blocker) => (
              <div
                className="rounded-card border border-border bg-soft px-3 py-2 text-sm font-bold text-muted"
                key={blocker}
              >
                {blocker}
              </div>
            ))}
          </div>
        </section>
      ) : null}

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="surface p-5">
          <h2 className="text-lg font-black">Draft review</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            These resources will be compiled into the next publication.
          </p>
          <div className="mt-5 grid gap-4 md:grid-cols-2">
            <ReviewList
              emptyLabel="No catalogs configured"
              items={(draft?.catalogs ?? []).map((catalog) => ({
                detail: catalog.status,
                label: catalog.name,
              }))}
              title="Catalogs"
            />
            <ReviewList
              emptyLabel="No assets promoted"
              items={(draft?.assets ?? []).map((asset) => ({
                detail: `${asset.catalog} / ${asset.policy_status}`,
                label: asset.name,
              }))}
              title="Assets"
            />
          </div>
        </div>

        <div className="surface p-5">
          <h2 className="text-lg font-black">Activation</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Activation switches the data plane to a compiled, immutable manifest.
          </p>
          <div className="mt-5 rounded-card border border-border bg-soft p-4">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Active publication
            </span>
            <strong className="mt-2 block break-all text-sm">
              {summary?.active_publication?.publication_id ?? "None"}
            </strong>
            <p className="mt-2 break-all text-xs leading-5 text-muted">
              {summary?.active_publication?.manifest_hash ?? "No active manifest yet."}
            </p>
          </div>
          {latestPublication ? (
            <button
              className="btn-primary mt-4 w-full"
              disabled={latestPublication.active || isActivating}
              type="button"
              onClick={() => setPendingAction("activate")}
            >
              {latestPublication.active
                ? "Latest publication active"
                : isActivating
                  ? "Activating..."
                  : "Review activation"}
            </button>
          ) : null}
        </div>
      </section>

      {pendingAction === "publish" ? (
        <ConfirmationPanel
          confirmLabel={isPublishing ? "Publishing..." : "Create publication"}
          disabled={!canPublish || isPublishing}
          eyebrow="Publish confirmation"
          title="Create immutable publication"
          onCancel={() => setPendingAction(null)}
          onConfirm={publishDraft}
        >
          <p className="text-sm leading-6 text-muted">
            This compiles the current draft into an immutable manifest. It does not
            change the active data plane publication until activation.
          </p>
          <div className="mt-4 grid grid-cols-2 gap-3">
            <Metric label="Catalogs" value={draft?.catalog_count ?? 0} />
            <Metric label="Assets" value={draft?.asset_count ?? 0} />
          </div>
        </ConfirmationPanel>
      ) : null}

      {pendingAction === "activate" && latestPublication ? (
        <ConfirmationPanel
          confirmLabel={
            isActivating
              ? "Activating..."
              : activationConfirmationLabel(latestPublication.id)
          }
          disabled={latestPublication.active || isActivating}
          eyebrow="Activation confirmation"
          title="Switch active data plane manifest"
          onCancel={() => setPendingAction(null)}
          onConfirm={() => activatePublication(latestPublication.id)}
        >
          <p className="text-sm leading-6 text-muted">
            Activation immediately points clients at this compiled manifest. Review
            the publication ID and manifest hash before confirming.
          </p>
          <div className="mt-4 rounded-card border border-border bg-soft p-4">
            <span className="text-xs font-black uppercase tracking-wide text-muted">
              Publication
            </span>
            <strong className="mt-2 block break-all text-sm">{latestPublication.id}</strong>
            <span className="mt-2 block break-all text-xs text-muted">
              {latestPublication.manifest_hash}
            </span>
          </div>
        </ConfirmationPanel>
      ) : null}

      <section className="surface p-5">
        <h2 className="text-lg font-black">Publication history</h2>
        <div className="mt-4 overflow-hidden rounded-card border border-border">
          {publications.length === 0 ? (
            <div className="px-4 py-10 text-center text-sm text-muted">
              No publications created yet.
            </div>
          ) : (
            publications.map((publication) => (
              <div
                className="grid grid-cols-1 gap-2 border-b border-border px-4 py-3 text-sm last:border-b-0 md:grid-cols-[1fr_140px_100px]"
                key={publication.id}
              >
                <div className="min-w-0">
                  <strong className="block break-all">{publication.id}</strong>
                  <span className="block break-all text-xs text-muted">
                    {publication.manifest_hash}
                  </span>
                </div>
                <span className="badge">{publication.status}</span>
                <span className="badge">{publication.active ? "active" : "inactive"}</span>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function ConfirmationPanel({
  children,
  confirmLabel,
  disabled,
  eyebrow,
  onCancel,
  onConfirm,
  title,
}: {
  children: ReactNode;
  confirmLabel: string;
  disabled: boolean;
  eyebrow: string;
  onCancel: () => void;
  onConfirm: () => void;
  title: string;
}) {
  return (
    <section className="surface border-l-4 border-l-accent p-5">
      <div className="flex flex-col justify-between gap-4 md:flex-row md:items-start">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">{eyebrow}</p>
          <h2 className="mt-1 text-lg font-black">{title}</h2>
        </div>
        <div className="flex flex-wrap gap-2">
          <button className="btn-secondary" type="button" onClick={onCancel}>
            Cancel
          </button>
          <button className="btn-primary" disabled={disabled} type="button" onClick={onConfirm}>
            {confirmLabel}
          </button>
        </div>
      </div>
      <div className="mt-4">{children}</div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="surface p-4">
      <span className="text-xs font-bold text-muted">{label}</span>
      <strong className="mt-2 block text-3xl font-black">{value}</strong>
    </div>
  );
}

function ReviewList({
  emptyLabel,
  items,
  title,
}: {
  emptyLabel: string;
  items: Array<{ detail: string; label: string }>;
  title: string;
}) {
  return (
    <div className="rounded-card border border-border">
      <div className="border-b border-border bg-soft px-4 py-3 text-xs font-black uppercase tracking-wide text-muted">
        {title}
      </div>
      {items.length === 0 ? (
        <div className="px-4 py-8 text-sm text-muted">{emptyLabel}</div>
      ) : (
        items.map((item) => (
          <div className="border-b border-border px-4 py-3 last:border-b-0" key={item.label}>
            <strong className="block text-sm">{item.label}</strong>
            <span className="mt-1 block text-xs text-muted">{item.detail}</span>
          </div>
        ))
      )}
    </div>
  );
}
