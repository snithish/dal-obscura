import { useEffect, useMemo, useRef, useState } from "react";
import { Button, TextInput } from "@mantine/core";
import type { AssetSchema } from "../api";
import { flattenSchemaTree } from "../schema_tree";
import { Icon } from "./Icon";

const ROW_HEIGHT = 56;
const VIEW_HEIGHT = 392;

export function SchemaSidebar({ schema, onClose }: { schema?: AssetSchema; onClose: () => void }) {
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(schema?.fields.map((field) => field.human_path)));
  const [scrollTop, setScrollTop] = useState(0);
  const viewport = useRef<HTMLDivElement>(null);
  const filtering = Boolean(search.trim());
  const all = useMemo(() => flattenSchemaTree(schema?.fields ?? [], new Set(), true), [schema]);
  const rows = useMemo(() => search.trim() ? all.filter(({ node }) => `${node.human_path} ${node.type}`.toLowerCase().includes(search.trim().toLowerCase())) : flattenSchemaTree(schema?.fields ?? [], expanded), [all, schema, expanded, search]);
  useEffect(() => { setScrollTop(0); if (viewport.current) viewport.current.scrollTop = 0; }, [search]);
  const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - 2);
  const end = Math.min(rows.length, Math.ceil((scrollTop + VIEW_HEIGHT) / ROW_HEIGHT) + 2);
  return <aside id="asset-schema-sidebar" className="schema-sidebar" aria-label="Asset schema">
    <div className="schema-sidebar-heading"><div><span className="eyebrow">REFERENCE</span><h2>Schema <span className="context-tag">{all.length}</span></h2></div><Button variant="subtle" aria-label="Hide schema" onClick={onClose}><Icon name="x" size={17} /></Button></div>
    <p className="help">Explore fields while writing rules.</p>
    <TextInput type="search" aria-label="Search schema" placeholder="Find a field or type" leftSection={<Icon name="search" size={16} />} value={search} onChange={(event) => setSearch(event.currentTarget.value)} />
    {!schema ? <p role="status" className="schema-empty">Schema unavailable. Refresh the asset to retry.</p> : <>
      <div className="schema-results-count" role="status">{filtering ? `${rows.length} matching fields` : `${all.length} fields · schema v${schema.schema_version}`}</div>
      <div ref={viewport} className="schema-reference-list" onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)} style={{ height: VIEW_HEIGHT }}>
        <div style={{ height: rows.length * ROW_HEIGHT, position: "relative" }}>{rows.slice(start, end).map(({ node, depth }, offset) => {
          const content = <><span className="schema-field-name" title={node.human_path}>{node.human_path}</span><span className="schema-field-meta"><span className="column-type-tag" title={node.type}>{node.type}</span><span>{node.nullable ? "Nullable" : "Required"}</span></span></>;
          const expandable = Boolean(node.children?.length) && !filtering;
          const indent = filtering ? 0 : depth * 12;
          const style = { position: "absolute" as const, top: (start + offset) * ROW_HEIGHT, height: ROW_HEIGHT, paddingLeft: 12 + indent + (expandable ? 16 : 0) };
          return expandable ? <button type="button" className="schema-reference-row expandable" key={node.human_path} style={style} aria-expanded={expanded.has(node.human_path)} aria-label={`${expanded.has(node.human_path) ? "Collapse" : "Expand"} ${node.human_path}`} onClick={() => setExpanded((current) => { const next = new Set(current); if (next.has(node.human_path)) next.delete(node.human_path); else next.add(node.human_path); return next; })}><Icon name="chevron-down" size={14} style={{ left: 8 + indent }} />{content}</button> : <div className="schema-reference-row" key={node.human_path} style={style}>{content}</div>;
        })}</div>
        {!rows.length && <p className="schema-empty">{search ? "No fields match. Try another name or type." : "No fields available."}</p>}
      </div>
      <p className="schema-reference-note"><Icon name="shield-check" size={16} />Reference only. Select allowed fields inside each rule.</p>
    </>}
  </aside>;
}
