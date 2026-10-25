import { expect, type Page } from "@playwright/test";

export async function chooseTheme(page: Page, theme: string) {
  await page.getByRole("button", { name: "Account menu", exact: true }).click();
  await page.getByRole("menuitemradio", { name: theme, exact: true }).click();
  await expect(page.getByRole("menu")).toBeHidden();
}
