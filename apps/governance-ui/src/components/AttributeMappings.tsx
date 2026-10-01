import { useEffect, useRef, useState } from "react";
import { Alert, Button, Textarea, TextInput } from "@mantine/core";
import type { AuthProvider } from "../api";
import { controlPlane } from "../api";
import { parseTestClaims } from "../policy_test";
import { recoveryMessage } from "../recovery";
import { isAbortError } from "../async";
import { TokenInput } from "./TokenInput";
import { Icon } from "./Icon";

type Mapping = { key: string; path: string; label: string; description: string; values: string[] };
function initialRows(provider: AuthProvider): Mapping[] {
  const mappings = (provider.args.attribute_claims ?? {}) as Record<string, string>;
  const definitions = (provider.args.attribute_definitions ?? {}) as Record<string, { label?: string; description?: string; allowed_values?: string[] }>;
  return Object.entries(mappings).map(([key, path]) => ({ key, path, label: definitions[key]?.label ?? "", description: definitions[key]?.description ?? "", values: definitions[key]?.allowed_values ?? [] }));
}

export function AttributeMappings({ provider, onChange }: {
  provider: AuthProvider; onChange: (claims: Record<string, string>, definitions: Record<string, unknown>, error?: string) => void;
}) {
  const [rows, setRows] = useState(() => initialRows(provider));
  const [sample, setSample] = useState('{"sub":"sample-user","employee":{"department":"Engineering"}}');
  const [preview, setPreview] = useState<Record<string, string> | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const request = useRef<AbortController | null>(null);
  const parsed = parseTestClaims(sample);
  const invalid = rows.some((row) => !row.key.trim() || !row.path.trim() || row.path.split(".").some((part) => !part.trim())) || new Set(rows.map((row) => row.key.trim())).size !== rows.length;
  useEffect(() => { setRows(initialRows(provider)); setPreview(null); setError(""); request.current?.abort(); }, [provider.id, provider.revision]);
  useEffect(() => { request.current?.abort(); setBusy(false); setPreview(null); }, [provider.args]);
  useEffect(() => () => request.current?.abort(), []);
  function change(next: Mapping[]) {
    request.current?.abort(); setBusy(false); setPreview(null); setError(""); setRows(next);
    const claims = Object.fromEntries(next.map((row) => [row.key.trim(), row.path.trim()]));
    const definitions = Object.fromEntries(next.map((row) => [row.key.trim(), { label: row.label, description: row.description, allowed_values: row.values }]));
    const invalid = next.some((row) => !row.key.trim() || !row.path.trim() || row.path.split(".").some((part) => !part.trim())) || new Set(next.map((row) => row.key.trim())).size !== next.length;
    onChange(claims, definitions, invalid ? "Every mapping needs a unique internal key and a valid source claim path." : undefined);
  }
  const update = (index: number, changeRow: Partial<Mapping>) => change(rows.map((row, i) => i === index ? { ...row, ...changeRow } : row));
  async function testMapping() {
    if (parsed.error || invalid) return;
    request.current?.abort();
    const controller = new AbortController(); request.current = controller;
    setBusy(true); setError(""); setPreview(null);
    try {
      const result = await controlPlane.previewAttributeMapping(provider.ordinal, parsed.claims, provider.args, controller.signal);
      if (!controller.signal.aborted) setPreview(result.attributes);
    } catch (error) {
      if (!isAbortError(error)) setError(recoveryMessage(error, "Mapping preview could not run."));
    } finally { if (!controller.signal.aborted) setBusy(false); }
  }
  return <section className="attribute-mappings">
    <div className="form-card-head"><div><h3>Identity attributes</h3><p className="help">Map trusted provider claims to stable policy keys. Scalar text values only; missing claims stay missing.</p></div><Button variant="default" size="sm" disabled={rows.length >= 32} leftSection={<Icon name="plus" size={14} />} onClick={() => change([...rows, { key: "", path: "", label: "", description: "", values: [] }])}>Add attribute</Button></div>
    {!rows.length && <p className="empty-result">No policy attributes mapped yet.</p>}
    {rows.map((row, index) => <div className="attribute-mapping-card" key={index}>
      <div className="attribute-mapping-head"><span className="context-tag">Mapping {index + 1}</span><Button variant="subtle" color="red" size="xs" aria-label={`Remove attribute mapping ${index + 1}`} onClick={() => change(rows.filter((_, i) => i !== index))}>Remove mapping</Button></div>
      <div className="attribute-mapping-grid">
        <TextInput label={`Source claim path ${index + 1}`} value={row.path} maxLength={256} placeholder="employee.department" onChange={(event) => update(index, { path: event.currentTarget.value })} />
        <TextInput label={`Internal attribute key ${index + 1}`} value={row.key} maxLength={256} placeholder="department" onChange={(event) => update(index, { key: event.currentTarget.value })} />
        <TextInput label={`Display name ${index + 1}`} value={row.label} maxLength={120} placeholder="Department" onChange={(event) => update(index, { label: event.currentTarget.value })} />
        <TextInput label={`Description ${index + 1}`} value={row.description} maxLength={500} placeholder="Employee business unit" onChange={(event) => update(index, { description: event.currentTarget.value })} />
      </div>
      <TokenInput label={`Allowed values ${index + 1}`} help="Optional enforced domain. Leave empty for unrestricted text. Values are case-sensitive. Press Enter to add." value={row.values} maxTokens={100} splitCommas={false} onChange={(values) => update(index, { values })} placeholder="Type a value and press Enter" />
      <p className="attribute-source">{provider.args.issuer ? String(provider.args.issuer) : "Provider issuer"} → {row.path || "source claim"} → <strong>{row.key || "internal key"}</strong></p>
    </div>)}
    {invalid && <Alert color="red">Every mapping needs a unique internal key and a valid source claim path.</Alert>}
    <details className="attribute-mapping-preview"><summary>Preview attribute mapping</summary><p className="help">Uses these unsaved mappings with the runtime mapper. Paste a synthetic decoded payload, never a bearer token. No identity-provider request or configuration save occurs.</p><Textarea label="Sample provider claims (JSON)" autosize minRows={4} value={sample} error={parsed.error} onChange={(event) => { request.current?.abort(); setBusy(false); setSample(event.currentTarget.value); setPreview(null); setError(""); }} /><Button variant="default" size="sm" loading={busy} disabled={invalid || Boolean(parsed.error)} onClick={() => void testMapping()}>Preview mapping</Button>{error && <Alert color="red">{error}</Alert>}{preview && <div role="status"><h4>Internal attributes</h4>{Object.entries(preview).map(([key, value]) => <span className="attribute-preview-chip" key={key}><strong>{key}</strong>{value}</span>)}{rows.filter((row) => !(row.key in preview)).map((row) => <p key={row.key}>Missing: {row.key} (claim {row.path})</p>)}{!rows.length && <p>No mapped attributes.</p>}</div>}</details>
  </section>;
}
