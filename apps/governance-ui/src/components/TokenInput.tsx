import { useId, useState } from "react";
import type { KeyboardEvent } from "react";

export function TokenInput({ label, value, onChange, disabled, placeholder, onDraftChange, help, splitCommas = true, maxTokens }: {
  label: string; value: string[]; onChange: (value: string[]) => void; disabled?: boolean; placeholder?: string; onDraftChange?: (value: string) => void;
  help?: string; splitCommas?: boolean; maxTokens?: number;
}) {
  const id = useId();
  const [draft, setDraft] = useState("");
  const updateDraft = (next: string) => { setDraft(next); onDraftChange?.(next); };
  const add = () => {
    if (disabled) return;
    const tokens = (splitCommas ? draft.split(/[,\n]/) : [draft]).map((token) => token.trim()).filter(Boolean);
    const next = [...new Set([...value, ...tokens])].slice(0, maxTokens);
    if (next.length !== value.length) onChange(next);
    updateDraft("");
  };
  const keyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter" || (splitCommas && event.key === ",")) { event.preventDefault(); add(); }
    else if (event.key === "Backspace" && !draft && value.length) onChange(value.slice(0, -1));
  };
  return <div className="token-input"><label className="field-label" htmlFor={id}>{label}</label><div className="token-input-control"><div className="token-chips">{value.map((token) => <span className="column-chip" key={token}>{token}<button type="button" aria-label={`Remove ${token}`} disabled={disabled} onClick={() => onChange(value.filter((item) => item !== token))}>×</button></span>)}</div><input id={id} value={draft} placeholder={placeholder} disabled={disabled || (maxTokens !== undefined && value.length >= maxTokens)} onChange={(event) => updateDraft(event.currentTarget.value)} onKeyDown={keyDown} onBlur={add} /></div><small>{help ?? "Enter a principal ID or group:name. Press Enter to add."}</small></div>;
}
