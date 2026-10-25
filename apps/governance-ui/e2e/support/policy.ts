import type { Page } from "@playwright/test";

export async function savePolicy(page: Page, options: { clickCount?: number } = {}) {
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  const submit = page
    .getByRole("dialog", { name: "Review policy changes", exact: true })
    .getByRole("button", { name: "Save policy", exact: true });
  if (options.clickCount === 2) {
    // Repeat activation on the same control, without clicking through a closing modal.
    await submit.evaluate((element) => {
      const button = element as HTMLButtonElement;
      button.click();
      button.click();
    });
  } else await submit.click();
}
