import assert from "node:assert/strict";
import test from "node:test";
import { QueryClient } from "@tanstack/react-query";
import { assetInventoryQueryKey, sessionQueryScope } from "../src/query_scope.ts";

test("session query scope separates anonymous and exact authenticated identities", () => {
  assert.equal(sessionQueryScope(null), "anonymous");
  assert.equal(sessionQueryScope({ issuer: "https://issuer.example/", principal: "alice" }), "https://issuer.example/|alice");
  assert.notEqual(
    sessionQueryScope({ issuer: "https://issuer.example/", principal: "alice" }),
    sessionQueryScope({ issuer: "https://issuer.example/", principal: "bob" }),
  );
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
