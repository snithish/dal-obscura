import { chooseTheme } from "../support/theme";
import { expect, test } from "@playwright/test";
import { authenticatedApi } from "../support/fixtures";

test("activity presets fill exact action filters while custom actions stay editable", async ({
  page,
}) => {
  await authenticatedApi(page, { admin: true, configuredAudit: true });
  await page.goto("/#activity");
  await page.getByLabel("Action preset", { exact: true }).selectOption("asset.policy.replace");
  const action = page.getByLabel("Action", { exact: true });
  await expect(action).toHaveValue("asset.policy.replace");
  const request = page.waitForRequest(
    (request) =>
      new URL(request.url()).pathname === "/v1/audit/events/page" &&
      new URL(request.url()).searchParams.get("action") === "asset.policy.replace",
  );
  await page.getByRole("button", { name: "Apply filters", exact: true }).click();
  await request;
  await action.fill("custom.exact.action");
  await expect(page.getByLabel("Action preset", { exact: true })).toHaveValue("");
  await expect(action).toHaveValue("custom.exact.action");
});

test("settings section links preserve drafts and expose independent dirty state", async ({
  page,
}) => {
  await authenticatedApi(page, { admin: true, configuredSettings: true });
  await page.goto("/#settings");
  await page.getByLabel("Ticket TTL (seconds)").fill("45");
  const sections = page.getByRole("navigation", { name: "Settings sections" });
  await expect(sections.getByRole("link", { name: "Runtime limits · Unsaved" })).toBeVisible();
  await sections.getByRole("link", { name: "Authentication providers · Saved" }).click();
  await expect(page.getByRole("heading", { name: "Authentication providers" })).toBeInViewport();
  await sections.getByRole("link", { name: "Runtime limits · Unsaved" }).click();
  await expect(page.getByLabel("Ticket TTL (seconds)")).toHaveValue("45");
  await expect(page.getByRole("dialog")).toHaveCount(0);
});

test("account menu owns theme selection and sign out remains directly reachable", async ({
  page,
}) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("combobox", { name: "Color theme" })).toHaveCount(0);
  await chooseTheme(page, "Dark theme");
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
  await expect(page.getByRole("button", { name: "Sign out", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Account menu", exact: true }).click();
  await expect(
    page.getByRole("menuitemradio", { name: "Dark theme", exact: true }),
  ).toHaveAttribute("aria-checked", "true");
  await page.getByRole("menuitemradio", { name: "Dark theme", exact: true }).focus();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Account menu", exact: true })).toBeFocused();
});
