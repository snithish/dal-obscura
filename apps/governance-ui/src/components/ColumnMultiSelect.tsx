import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";
import { selectColumnsShortcut } from "../policy_editor";
import { Button, NativeSelect, TextInput } from "@mantine/core";
import type { ColumnOption } from "../policy_editor";

const ROW_HEIGHT = 42;
const VIEW_HEIGHT = 252;
function covers(parent: ColumnOption, child: ColumnOption): boolean {
  return parent.path.segments.length < child.path.segments.length && parent.path.segments.every((segment, index) => {
    const other = child.path.segments[index];
    return segment.kind === other?.kind && segment.name === other.name && segment.field_id === other.field_id;
  });
}

export function ColumnMultiSelect({ label, options, value, onChange, disabled = false, shortcuts = false }: {
  label: string; options: ColumnOption[]; value: string[]; onChange: (value: string[]) => void; disabled?: boolean; shortcuts?: boolean;
}) {
  const [shortcut, setShortcut] = useState<"except" | "prefix" | null>(null);
  const [prefix, setPrefix] = useState("");
  const id = useId();
  const root = useRef<HTMLDivElement>(null);
  const viewport = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [scrollTop, setScrollTop] = useState(0);
  const [active, setActive] = useState(0);
  const [conflict, setConflict] = useState("");
  const trigger = useRef<HTMLButtonElement>(null);
  const searchInput = useRef<HTMLInputElement>(null);
  const filtered = useMemo(() => options.filter((option) => `${option.label} ${option.type}`.toLowerCase().includes(search.trim().toLowerCase())), [options, search]);
  const eligibleResults = useMemo(() => {
    const eligible: ColumnOption[] = [];
    for (const option of filtered) {
      if (!option.valid || value.includes(option.value)) continue;
      const overlapsExisting = [...value, ...eligible.map((item) => item.value)].some((item) => options.some((candidate) => candidate.value === item && candidate.valid && (covers(candidate, option) || covers(option, candidate))));
      if (!overlapsExisting) eligible.push(option);
    }
    return eligible;
  }, [filtered, options, value]);
  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => { if (!root.current?.contains(event.target as Node)) { setOpen(false); setSearch(""); } };
    document.addEventListener("pointerdown", outside);
    return () => document.removeEventListener("pointerdown", outside);
  }, [open]);
  useEffect(() => {
    if (viewport.current) viewport.current.scrollTop = 0;
    setScrollTop(0); setActive(0);
  }, [search]);
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
    setConflict("");
    if (value.includes(option.value)) return onChange(value.filter((item) => item !== option.value));
    if (!option.valid) return;
    if (option.valid) {
      const parent = value.find((item) => options.some((candidate) => candidate.value === item && candidate.valid && covers(candidate, option)));
      const child = value.find((item) => options.some((candidate) => candidate.value === item && candidate.valid && covers(option, candidate)));
      if (parent || child) return setConflict(`Remove ${parent ?? child} before selecting an overlapping parent or child path.`);
    }
    onChange([...value, option.value]);
  };
  const close = () => { setOpen(false); setSearch(""); trigger.current?.focus(); };
  const onSearchKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Escape") { event.preventDefault(); close(); }
    else if (event.key === "ArrowDown") { event.preventDefault(); moveActive(Math.max(0, Math.min(filtered.length - 1, active + 1))); }
    else if (event.key === "ArrowUp") { event.preventDefault(); moveActive(Math.max(0, active - 1)); }
    else if (event.key === "Enter" && filtered[active]) { event.preventDefault(); toggle(filtered[active]); }
  };
  return <div ref={root} className="column-multi-select" onKeyDown={(event) => { if (event.key === "Escape" && open) { event.stopPropagation(); close(); } }}>
    <span id={`${id}-label`} className="field-label">{label}</span>
    <button ref={trigger} type="button" className="column-select-trigger" aria-label={`${label}: ${value.length} selected`} aria-controls={`${id}-options`} aria-expanded={open} aria-haspopup="listbox" disabled={disabled} onClick={() => { setOpen(!open); if (!open) requestAnimationFrame(() => searchInput.current?.focus()); }}>
      {value.length ? `${value.length} selected` : "Choose columns"} <span aria-hidden="true">⌄</span>
    </button>
    {shortcuts && <div className="column-shortcuts"><Button variant="subtle" size="compact-sm" disabled={disabled} onClick={() => onChange(selectColumnsShortcut(options, "all"))}>Select all</Button><Button variant="subtle" size="compact-sm" disabled={disabled} onClick={() => setShortcut(shortcut === "except" ? null : "except")}>Select all except…</Button><Button variant="subtle" size="compact-sm" disabled={disabled} onClick={() => setShortcut(shortcut === "prefix" ? null : "prefix")}>Add by prefix</Button></div>}
    {shortcut === "except" && <NativeSelect label="Exclude a column or subtree" disabled={disabled} value="" data={[{ value: "", label: "Choose what to leave NULL" }, ...options.filter((option) => option.valid).map((option) => ({ value: option.value, label: option.label }))]} onChange={(event) => { if (event.currentTarget.value) onChange(selectColumnsShortcut(options, "except", event.currentTarget.value)); }} />}
    {shortcut === "prefix" && <div className="prefix-shortcut"><TextInput label="Column path prefix" value={prefix} disabled={disabled} onChange={(event) => setPrefix(event.currentTarget.value)} placeholder="customer.address." /><Button disabled={disabled || !prefix || !selectColumnsShortcut(options, "prefix", prefix).length} onClick={() => onChange([...new Set([...value, ...selectColumnsShortcut(options, "prefix", prefix).filter((path) => !value.some((existing) => { const parent = options.find((item) => item.value === existing); const child = options.find((item) => item.value === path); return parent && child && covers(parent, child); }))])])}>Add matching columns</Button></div>}
    {open && <div className="column-select-popup">
      <input ref={searchInput} type="search" role="combobox" aria-expanded={open} aria-controls={`${id}-options`} aria-activedescendant={filtered[active] ? `${id}-option-${active}` : undefined} aria-label={`Search ${label.toLowerCase()}`} placeholder="Search columns" value={search} onChange={(event) => { setSearch(event.currentTarget.value); setActive(0); }} onKeyDown={onSearchKeyDown} />
      <button type="button" className="select-results" disabled={!eligibleResults.length} onClick={() => {
        onChange([...value, ...eligibleResults.map((option) => option.value)]);
      }}>Select search results ({eligibleResults.length})</button>
      <div ref={viewport} id={`${id}-options`} className="column-select-list" role="listbox" aria-labelledby={`${id}-label`} aria-multiselectable="true" onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)} style={{ height: VIEW_HEIGHT }}>
        <div style={{ height: filtered.length * ROW_HEIGHT, position: "relative" }}>{filtered.slice(start, end).map((option, offset) => {
          const index = start + offset;
          return <button id={`${id}-option-${index}`} key={option.value} type="button" role="option" tabIndex={-1} aria-disabled={!option.valid && !value.includes(option.value)} aria-selected={value.includes(option.value)} aria-posinset={index + 1} aria-setsize={filtered.length} className={`column-select-option ${index === active ? "active" : ""}`} style={{ position: "absolute", top: index * ROW_HEIGHT, height: ROW_HEIGHT }} onMouseEnter={() => setActive(index)} onClick={() => toggle(option)}>
            <span className="column-check" aria-hidden="true">{value.includes(option.value) ? "✓" : ""}</span><span>{option.label}<small>{option.type}{option.valid ? "" : " · unavailable"}</small></span>
          </button>;
        })}</div>
        {!filtered.length && <p className="column-empty">No matching columns.</p>}
      </div>
      {conflict && <p className="field-error" role="alert">{conflict}</p>}
      <button type="button" className="column-select-done" onClick={close}>Done</button>
    </div>}
    <div className="column-chips" role="group" aria-label="Selected columns">{value.map((column) => <span className="column-chip" key={column}><span>{column}{!options.find((option) => option.value === column)?.valid && " · unavailable"}</span><button type="button" aria-label={`Remove ${column}`} disabled={disabled} onClick={() => onChange(value.filter((item) => item !== column))}>×</button></span>)}</div>
  </div>;
}
