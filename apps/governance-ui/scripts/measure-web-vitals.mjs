import { chromium } from "@playwright/test";
import { assertWebVitals, observeWebVitals, WEB_VITALS_LIMITS } from "./web-vitals-budget.ts";

const url = process.env.DAL_OBSCURA_WEB_VITALS_URL ?? "http://127.0.0.1:4173/#assets";
const width = Number(process.env.DAL_OBSCURA_WEB_VITALS_WIDTH ?? 1440);
const height = Number(process.env.DAL_OBSCURA_WEB_VITALS_HEIGHT ?? 900);

const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH,
});

try {
  const page = await browser.newPage({ viewport: { width, height } });
  await page.addInitScript(observeWebVitals);

  await page.goto(url, { waitUntil: "networkidle" });
  await page.evaluate(() => document.fonts.ready);
  const metrics = await page.evaluate(() => ({
    lcpMs: window.__dalObscuraWebVitals.lcp,
    cls: window.__dalObscuraWebVitals.cls,
    navigationMs: performance.getEntriesByType("navigation")[0]?.duration ?? null,
  }));
  console.log(
    JSON.stringify(
      { url, viewport: { width, height }, limits: WEB_VITALS_LIMITS, metrics },
      null,
      2,
    ),
  );
  assertWebVitals(metrics);
} finally {
  await browser.close();
}
