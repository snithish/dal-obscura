import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { Button, Modal, TextInput } from "@mantine/core";
import { columnLeafPaths, columnOptionCovers, leafColumnOptions } from "../policy_editor";
import type { ColumnOption } from "../policy_editor";
import { Icon } from "./Icon";

const ROW_HEIGHT = 48;
const VIEW_HEIGHT = 288;

export function ColumnMultiSelect({ label, options, value, onChange, disabled = false, shortcuts = false }: {
  label: string; options: ColumnOption[]; value: string[]; onChange: (value: string[]) => void; disabled?: boolean; shortcuts?: boolean;
}) {
  const id = useId();
  const viewport = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [scrollTop, setScrollTop] = useState(0);
  const [active, setActive] = useState(0);
  const [conflict, setConflict] = useState("");
  const [mode, setMode] = useState<"except" | "prefix" | null>(null);
  const [prefix, setPrefix] = useState("");
  const [excluded, setExcluded] = useState<string[]>([]);
  const [selectedOnly, setSelectedOnly] = useState(false);
  const leaves = useMemo(() => leafColumnOptions(options), [options]);
  const byPath = useMemo(() => new Map(options.map((option) => [option.value, option])), [options]);
  const leafPaths = useMemo(() => columnLeafPaths(options), [options]);
  const covers = (parent: string, child: string) => parent === child || columnOptionCovers(byPath.get(parent), byPath.get(child));
  const expand = (paths: string[]) => [...new Set(paths.flatMap((path) => {
    const children = leafPaths.get(path) ?? [];
    return children.length ? children : [path];
  }))];
  const selected = mode === "except" ? excluded : value;
  const selectedSet = new Set(selected);
  const selectedLeaves = new Set(expand(selected));
  const isSelected = (option: ColumnOption) => {
    if (selectedSet.has(option.value) || selectedLeaves.has(option.value)) return true;
    if (!shortcuts || !option.valid) return false;
    const children = leafPaths.get(option.value) ?? [];
    return children.length > 0 && children.every((leaf) => selectedLeaves.has(leaf));
  };
  const filtered = open ? options.filter((option) => `${option.label} ${option.type}`.toLowerCase().includes(search.trim().toLowerCase()) && (!selectedOnly || isSelected(option))) : [];
  const prefixMatches = leaves.filter((leaf) => leaf.value.startsWith(prefix.trim())).map((leaf) => leaf.value);
  const excludedLeaves = new Set(expand(excluded));
  const exceptResult = leaves.filter((leaf) => !excludedLeaves.has(leaf.value)).map((leaf) => leaf.value);
  const results = shortcuts ? expand(filtered.filter((option) => option.valid).map((option) => option.value)) : filtered.filter((option) => option.valid).map((option) => option.value);
  const resultLeaves = new Set(expand(results));
  const valueLeaves = new Set(expand(value));
  const eligibleResults: string[] = [];
  const occupied = new Set(valueLeaves);
  for (const path of results) {
    const children = leafPaths.get(path) ?? [path];
    if (!children.some((child) => occupied.has(child))) {
      eligibleResults.push(path);
      children.forEach((child) => occupied.add(child));
    }
  }
  useEffect(() => {
    if (viewport.current) viewport.current.scrollTop = 0;
    setScrollTop(0); setActive(0);
  }, [search, mode, selectedOnly]);
  const moveActive = (index: number) => {
    setActive(index);
    if (viewport.current) {
      const top = index * ROW_HEIGHT;
      if (top < viewport.current.scrollTop) viewport.current.scrollTop = top;
      else if (top + ROW_HEIGHT > viewport.current.scrollTop + VIEW_HEIGHT) viewport.current.scrollTop = top + ROW_HEIGHT - VIEW_HEIGHT;
    }
  };
  const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - 2);
  const end = Math.min(filtered.length, Math.ceil((scrollTop + VIEW_HEIGHT) / ROW_HEIGHT) + 2);
  const toggle = (option: ColumnOption) => {
    if (disabled) return;
    setConflict("");
    if (mode === "except") {
      if (!option.valid) return;
      if (isSelected(option)) setExcluded(expand(excluded).filter((path) => !covers(option.value, path)));
      else setExcluded([...excluded.filter((path) => !covers(option.value, path)), option.value]);
      return;
    }
    if (shortcuts && option.valid) {
      const paths = expand([option.value]);
      if (isSelected(option)) onChange(expand(value).filter((path) => !covers(option.value, path)));
      else onChange([...new Set([...expand(value), ...paths])]);
      return;
    }
    if (value.includes(option.value)) return onChange(value.filter((item) => item !== option.value));
    if (!option.valid) return;
    const overlap = value.find((path) => covers(path, option.value) || covers(option.value, path));
    if (overlap) return setConflict(`Remove ${overlap} before selecting an overlapping parent or child path.`);
    onChange([...value, option.value]);
  };
  const close = () => { setOpen(false); setSearch(""); setMode(null); setSelectedOnly(false); setConflict(""); };
  const chooseMode = (next: "except" | "prefix") => { setMode(mode === next ? null : next); setSearch(""); setSelectedOnly(false); setExcluded([]); setConflict(""); };
  const onSearchKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "ArrowDown") { event.preventDefault(); moveActive(Math.max(0, Math.min(filtered.length - 1, active + 1))); }
    else if (event.key === "ArrowUp") { event.preventDefault(); moveActive(Math.max(0, active - 1)); }
    else if (event.key === "Enter" && filtered[active]) { event.preventDefault(); toggle(filtered[active]); }
  };
  const chips = (columns: string[], remove: (column: string) => void) => columns.map((column) => <span className="column-chip" key={column}><span>{column}{!options.find((option) => option.value === column)?.valid && " · unavailable"}</span><button type="button" aria-label={`Remove ${column}`} disabled={disabled} onClick={() => remove(column)}><Icon name="x" size={12} /></button></span>);
  return <div className="column-multi-select">
    <span id={`${id}-label`} className="field-label">{label}</span>
    <button ref={trigger} type="button" className="column-select-trigger" aria-label={`${label}: ${value.length} selected`} aria-expanded={open} aria-haspopup="dialog" disabled={disabled} onClick={() => setOpen(true)}>
      <span className="column-trigger-label"><Icon name="database" size={17} /><span>{value.length ? `${value.length} selected` : "Choose columns"}</span></span><span className="column-trigger-action">Browse columns <Icon name="plus" size={15} /></span>
    </button>
    <div className="column-chips" role="group" aria-label="Selected columns">{chips(value, (column) => onChange(value.filter((item) => item !== column)))}</div>
    <Modal opened={open} onClose={close} title={`Choose ${label.toLowerCase()}`} size={880} centered classNames={{ content: "column-picker-modal", body: "column-picker-body" }} closeButtonProps={{ "aria-label": "Close column picker" }} onExitTransitionEnd={() => trigger.current?.focus()}>
      <p className="picker-description">{shortcuts ? "Choose fields to reveal. Unselected fields keep the default NULL mask." : "Choose specific columns for this operation."}</p>
      {shortcuts && <div className="picker-shortcuts" aria-label="Column selection tools"><Button variant="default" disabled={disabled} onClick={() => { onChange(leaves.map((leaf) => leaf.value)); setMode(null); }}>Select all</Button><Button variant={mode === "except" ? "light" : "default"} aria-pressed={mode === "except"} disabled={disabled} onClick={() => chooseMode("except")}>Select all except…</Button><Button variant={mode === "prefix" ? "light" : "default"} aria-pressed={mode === "prefix"} disabled={disabled} onClick={() => chooseMode("prefix")}>Add by prefix</Button></div>}
      {mode === "prefix" && <div className="picker-mode-panel"><TextInput label="Column path prefix" value={prefix} disabled={disabled} onChange={(event) => setPrefix(event.currentTarget.value)} placeholder="customer." description="Matches the start of a column path. Existing selections are kept." /><div className="prefix-preview"><span role="status">{prefix.trim() ? prefixMatches.length : 0} matching columns</span><Button disabled={disabled || !prefix.trim() || !prefixMatches.length} onClick={() => { onChange([...value, ...prefixMatches.filter((path) => !valueLeaves.has(path))]); setMode(null); }}>Add matching columns</Button></div></div>}
      {mode === "except" && <div className="picker-mode-panel"><strong>Choose columns to exclude</strong><p>Check any fields or subtrees to leave NULL. Review the count, then apply.</p></div>}
      <div className="picker-workspace">
        <div className="picker-available">
          <TextInput data-autofocus type="search" role="combobox" aria-expanded={open} aria-controls={`${id}-options`} aria-activedescendant={filtered[active] ? `${id}-option-${active}` : undefined} aria-label={`Search ${label.toLowerCase()}`} placeholder="Search by column name or type" leftSection={<Icon name="search" size={17} />} value={search} onChange={(event) => setSearch(event.currentTarget.value)} onKeyDown={onSearchKeyDown} />
          <div className="picker-results-bar"><span role="status">{filtered.length} {filtered.length === 1 ? "result" : "results"}</span>{mode !== "except" && <button className="text-action" type="button" aria-pressed={selectedOnly} onClick={() => setSelectedOnly(!selectedOnly)}>{selectedOnly ? "Show all columns" : "Show selected"}</button>}</div>
          <div className="picker-list-container"><div ref={viewport} id={`${id}-options`} hidden={!filtered.length} className="column-select-list" role="listbox" aria-label={mode === "except" ? "Columns to exclude" : label} aria-multiselectable="true" onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)} style={{ height: VIEW_HEIGHT }}>
            <div style={{ height: filtered.length * ROW_HEIGHT, position: "relative" }}>{filtered.slice(start, end).map((option, offset) => {
              const index = start + offset;
              const checked = isSelected(option);
              const nestedCount = (leafPaths.get(option.value) ?? []).filter((leaf) => leaf !== option.value).length;
              return <button id={`${id}-option-${index}`} key={option.value} type="button" role="option" tabIndex={0} aria-disabled={disabled || (!option.valid && !value.includes(option.value))} aria-selected={checked} aria-posinset={index + 1} aria-setsize={filtered.length} className={`column-select-option ${index === active ? "active" : ""}`} style={{ position: "absolute", top: index * ROW_HEIGHT, height: ROW_HEIGHT }} onMouseEnter={() => setActive(index)} onClick={() => toggle(option)}>
                <span className="column-check" aria-hidden="true">{checked && <Icon name="check" size={13} />}</span><span className="column-option-name" title={option.label}>{option.label}<small>{nestedCount ? `${nestedCount} nested fields` : option.valid ? "" : "Unavailable in current schema"}</small></span><span className="column-type-tag" title={option.type}>{option.type}</span>
              </button>;
            })}</div>
            </div>
          {!filtered.length && <div className="column-empty" role="status" style={{ height: VIEW_HEIGHT }}><Icon name="search" size={24} /><strong>No matching columns</strong><p>Try a different name or type.</p><Button variant="subtle" onClick={() => { setSearch(""); setSelectedOnly(false); }}>Reset search</Button></div>}</div>
          {mode !== "except" && <div className="picker-result-actions"><Button variant="subtle" disabled={disabled || !eligibleResults.length} onClick={() => onChange([...value, ...eligibleResults])}>Select search results ({eligibleResults.length})</Button><Button variant="subtle" disabled={disabled || ![...valueLeaves].some((path) => resultLeaves.has(path))} onClick={() => onChange((shortcuts ? expand(value) : value).filter((path) => !(leafPaths.get(path) ?? [path]).some((leaf) => resultLeaves.has(leaf))))}>Deselect results</Button></div>}
        </div>
        <aside className="picker-selection" aria-label="Selection review"><div className="picker-selection-heading"><strong>{mode === "except" ? "Excluded fields" : "Your selection"}</strong><span className="context-tag">{selected.length}</span></div><p>{mode === "except" ? "These fields will keep the NULL mask." : "Review selected paths before returning to your rule."}</p><div className="picker-selected-chips">{chips(selected, (column) => mode === "except" ? setExcluded(excluded.filter((item) => item !== column)) : onChange(value.filter((item) => item !== column)))}</div>{!selected.length && <div className="picker-selection-empty"><Icon name="database" size={24} /><span>{mode === "except" ? "No exclusions yet" : "No columns selected"}</span></div>}{mode !== "except" && <Button variant="subtle" disabled={disabled || !value.length} onClick={() => onChange([])}>Clear selection</Button>}</aside>
      </div>
      {conflict && <p className="field-error" role="alert">{conflict}</p>}
      <div className="picker-footer"><span role="status">{mode === "except" ? `${excluded.length} excluded · ${exceptResult.length} columns will be allowed` : `${value.length} selected`}</span>{mode === "except" ? <Button disabled={disabled} onClick={() => { onChange(exceptResult); setMode(null); }}>Apply exclusions ({exceptResult.length} allowed)</Button> : <Button onClick={close}>Done</Button>}</div>
    </Modal>
  </div>;
}
