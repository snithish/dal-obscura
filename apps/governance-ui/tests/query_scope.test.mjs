import assert from "node:assert/strict";
import test from "node:test";
import { QueryClient } from "@tanstack/react-query";
import { assetInventoryQueryKey, sessionQueryScope } from "../src/query_scope.ts";

test("session query scope separates anonymous and exact authenticated identities", () => {
  assert.equal(sessionQueryScope(null, 0), "anonymous");
  assert.notEqual(
    sessionQueryScope({ issuer: "https://issuer.example/", principal: "alice" }, 1),
    sessionQueryScope({ issuer: "https://issuer.example/", principal: "bob" }, 1),
  );
});

test("delimiter-containing identities cannot read each other's cached inventory", () => {
  const client = new QueryClient();
  const first = sessionQueryScope({ issuer: "https://issuer.example/|team", principal: "alice" }, 1);
  const second = sessionQueryScope({ issuer: "https://issuer.example/", principal: "team|alice" }, 1);
  const firstKey = assetInventoryQueryKey(first, "", null);
  const secondKey = assetInventoryQueryKey(second, "", null);
  client.setQueryData(firstKey, { items: ["private-asset"] });
  assert.equal(client.getQueryData(secondKey), undefined);
  assert.deepEqual(client.getQueryData(firstKey), { items: ["private-asset"] });
  client.clear();
});

test("reauthentication does not reuse cached inventory for the same identity", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity } } });
  const actor = { issuer: "https://issuer.example/", principal: "alice" };
  const before = assetInventoryQueryKey(sessionQueryScope(actor, 1), "", null);
  const after = assetInventoryQueryKey(sessionQueryScope(actor, 2), "", null);
  client.setQueryData(before, { items: ["revoked-asset"] });
  const result = await client.fetchQuery({ queryKey: after, queryFn: async () => ({ items: [] }) });
  assert.deepEqual(result, { items: [] });
  client.clear();
});

test("inventory query keys include session, search and cursor", () => {
  assert.deepEqual(assetInventoryQueryKey("issuer|alice", "orders", "cursor-2"), [
    "asset-inventory",
    "issuer|alice",
    "orders",
    "cursor-2",
  ]);
});

test("query cache deduplicates one session page without sharing another session", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  let calls = 0;
  const load = () => {
    calls += 1;
    return Promise.resolve({ items: [calls] });
  };
  const aliceKey = assetInventoryQueryKey("issuer|alice", "", null);
  const [first, second] = await Promise.all([
    client.fetchQuery({ queryKey: aliceKey, queryFn: load }),
    client.fetchQuery({ queryKey: aliceKey, queryFn: load }),
  ]);
  assert.deepEqual(first, { items: [1] });
  assert.deepEqual(second, { items: [1] });
  await client.fetchQuery({
    queryKey: assetInventoryQueryKey("issuer|bob", "", null),
    queryFn: load,
  });
  assert.equal(calls, 2);
  client.clear();
});
