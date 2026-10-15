import { expect, test } from "@playwright/test";
import { wideSelectionApi } from "../support/wide-selection";

test("wide mask edits retain exact targets and keep replacement typing responsive", async ({
  page,
}) => {
  const writes: string[] = [];
  page.on("request", (request) => {
    if (request.method() === "PUT") writes.push(request.url());
  });
  await wideSelectionApi(page, 5000, true);
  await page.getByRole("button", { name: "Edit Hash mask", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("redact");
  const replacement = page.getByLabel("Replacement text", { exact: true });
  await replacement.fill("");
  const timings: number[] = [];
  for (const character of "private-data") {
    const started = performance.now();
    await replacement.pressSequentially(character);
    timings.push(performance.now() - started);
  }
  const p95 = [...timings].sort((a, b) => a - b)[Math.ceil(timings.length * 0.95) - 1];
  await test.info().attach("mask-typing", {
    body: JSON.stringify({ fieldCount: 5000, samplesMs: timings, p95Ms: p95 }),
    contentType: "application/json",
  });
  expect(p95).toBeLessThan(200);
  await expect(page.getByText("5000 masked · 0 excluded", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await expect(page.getByRole("button", { name: "Edit Redact mask", exact: true })).toBeVisible();
  await expect(page.getByText('"private-data"', { exact: true })).toBeVisible();
  expect(writes).toEqual([]);
  const saved = page.waitForRequest(
    (request) => request.method() === "PUT" && request.url().endsWith("/policy"),
  );
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  const submitted = (await saved).postDataJSON().rules[0];
  const columns = Array.from({ length: 5000 }, (_, index) => `field_${index}`);
  expect(submitted.columns).toEqual(columns);
  expect(submitted.masks).toEqual(
    Object.fromEntries(columns.map((name) => [name, { type: "redact", value: "private-data" }])),
  );
});
