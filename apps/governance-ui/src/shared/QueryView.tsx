import type { ReactNode } from "react";
import { Button } from "@mantine/core";
import { Icon } from "./Icon";

export function QueryView({ page, loaded, loading, error, onReload, children }: {
  page: string; loaded: boolean; loading: boolean; error: string;
  onReload: () => void; children: ReactNode;
}) {
  if (loading && !loaded) return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>Loading {page}</h2><p>Checking the current workspace state and your capabilities.</p></section>;
  if (error && !loaded) return <section className="coming-soon" role="alert"><span className="eyebrow">{page.toUpperCase()}</span><h2>Management view unavailable</h2><p>{error}</p><Button type="button" variant="default" className="secondary" onClick={onReload} leftSection={<Icon name="refresh-cw" size={16} />}>Retry</Button></section>;
  return <>{error && <div className="validation-summary management-refresh-error" role="alert"><strong>Workspace refresh failed</strong><p>{error}</p><Button type="button" variant="default" size="sm" className="secondary compact" onClick={onReload} leftSection={<Icon name="refresh-cw" size={15} />}>Retry refresh</Button></div>}{children}</>;
}
