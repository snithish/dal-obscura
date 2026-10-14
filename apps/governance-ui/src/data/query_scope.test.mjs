import assert from "node:assert/strict";
import test from "node:test";
import { QueryClient } from "@tanstack/react-query";
import { assetInventoryQueryKey, sessionQueryScope } from "./query_scope.ts";

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
  const firstKey = assetInventoryQueryKey(first, "");
  const secondKey = assetInventoryQueryKey(second, "");
  client.setQueryData(firstKey, { items: ["private-asset"] });
  assert.equal(client.getQueryData(secondKey), undefined);
  assert.deepEqual(client.getQueryData(firstKey), { items: ["private-asset"] });
  client.clear();
});

test("reauthentication does not reuse cached inventory for the same identity", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity } } });
  const actor = { issuer: "https://issuer.example/", principal: "alice" };
  const before = assetInventoryQueryKey(sessionQueryScope(actor, 1), "");
  const after = assetInventoryQueryKey(sessionQueryScope(actor, 2), "");
  client.setQueryData(before, { items: ["revoked-asset"] });
  const result = await client.fetchQuery({ queryKey: after, queryFn: async () => ({ items: [] }) });
  assert.deepEqual(result, { items: [] });
  client.clear();
});

test("inventory search results stay separate within one session", () => {
  const client = new QueryClient();
  try {
    const orders = assetInventoryQueryKey("issuer|alice", "orders");
    const invoices = assetInventoryQueryKey("issuer|alice", "invoices");
    client.setQueryData(orders, { items: ["orders"] });
    assert.equal(client.getQueryData(invoices), undefined);
    client.setQueryData(invoices, { items: ["invoices"] });
    assert.deepEqual(client.getQueryData(orders), { items: ["orders"] });
    assert.deepEqual(client.getQueryData(invoices), { items: ["invoices"] });
  } finally {
    client.clear();
  }
});

test("query cache deduplicates one session page without sharing another session", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try {
    let calls = 0;
    const load = () => {
      calls += 1;
      return Promise.resolve({ items: [calls] });
    };
    const aliceKey = assetInventoryQueryKey("issuer|alice", "");
    const [first, second] = await Promise.all([
      client.fetchQuery({ queryKey: aliceKey, queryFn: load }),
      client.fetchQuery({ queryKey: aliceKey, queryFn: load }),
    ]);
    assert.deepEqual(first, { items: [1] });
    assert.deepEqual(second, { items: [1] });
    const bob = await client.fetchQuery({
      queryKey: assetInventoryQueryKey("issuer|bob", ""),
      queryFn: load,
    });
    assert.deepEqual(bob, { items: [2] });
    assert.deepEqual(client.getQueryData(aliceKey), { items: [1] });
    assert.equal(calls, 2);
  } finally {
    client.clear();
  }
});
