import { useEffect } from "react";
import { Button } from "@mantine/core";
import { useQuery } from "@tanstack/react-query";
import { assetQueries } from "./assetQueries";
import { recoveryMessage } from "../../data/recovery";
import { sessionQueryScope } from "../../data/query_scope";
import type { AssetTab } from "../../app/navigation/routes";
import { LoginPanel } from "../identity/LoginPanel";
import { useSessionScope } from "../identity/SessionBoundary";
import { PolicyEditorController } from "../policy/PolicyEditorController";

export function AssetRoute({
  assetId,
  initialTab,
  onTabChange,
  onBack,
  onContextChange,
  onDirtyChange,
}: {
  assetId: string;
  initialTab?: AssetTab;
  onTabChange: (tab: AssetTab) => void;
  onBack: () => void;
  onContextChange: (context: { name: string; catalog: string } | null) => void;
  onDirtyChange: (dirty: boolean) => void;
}) {
  const { session, generation, queryClient, isCurrent } = useSessionScope();
  const queries = assetQueries(sessionQueryScope(session, generation), assetId);
  const detail = useQuery(queries.detail, queryClient);
  const schema = useQuery(queries.schema, queryClient);
  const grants = useQuery(queries.grants, queryClient);
  const access = useQuery(queries.access, queryClient);
  useEffect(() => {
    if (detail.data && schema.data && grants.data && access.data && isCurrent(generation))
      onContextChange({ name: detail.data.name, catalog: detail.data.catalog });
    return () => onContextChange(null);
  }, [detail.data, schema.data, grants.data, access.data, generation, isCurrent, onContextChange]);

  const error = detail.error ?? schema.error ?? grants.error ?? access.error;
  const reload = () => {
    void detail.refetch();
    void schema.refetch();
    void grants.refetch();
    void access.refetch();
  };
  if (!detail.data || !schema.data || !grants.data || !access.data) {
    return (
      <section aria-label="Asset workspace">
        <Button variant="subtle" onClick={onBack}>
          Back to assets
        </Button>
        {error ? (
          <LoginPanel
            title="Asset unavailable"
            message={recoveryMessage(
              error,
              "Could not load this asset and its access metadata. Retry or return to assets.",
            )}
            retry={reload}
          />
        ) : (
          <p role="status">Loading asset workspace…</p>
        )}
      </section>
    );
  }
  return (
    <PolicyEditorController
      key={assetId}
      asset={{ ...detail.data, schema: schema.data }}
      access={access.data}
      grants={grants.data}
      initialTab={initialTab}
      onTabChange={onTabChange}
      onBack={onBack}
      onReloadAccess={reload}
      onDirtyChange={onDirtyChange}
    />
  );
}
