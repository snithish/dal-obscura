import { chromium } from "@playwright/test";

const url = process.env.DAL_OBSCURA_WEB_VITALS_URL ?? "http://127.0.0.1:4173/#assets";
const width = Number(process.env.DAL_OBSCURA_WEB_VITALS_WIDTH ?? 1440);
const height = Number(process.env.DAL_OBSCURA_WEB_VITALS_HEIGHT ?? 900);

const browser = await chromium.launch({
  headless: true,
  executablePath: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH,
});

try {
  const page = await browser.newPage({ viewport: { width, height } });
  await page.addInitScript(() => {
    window.__dalObscuraWebVitals = { lcp: null, cls: 0 };
    new PerformanceObserver((list) => {
      const entries = list.getEntries();
      const last = entries.at(-1);
      if (last) window.__dalObscuraWebVitals.lcp = last.startTime;
    }).observe({ type: "largest-contentful-paint", buffered: true });
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (!entry.hadRecentInput) window.__dalObscuraWebVitals.cls += entry.value;
      }
    }).observe({ type: "layout-shift", buffered: true });
  });

  await page.goto(url, { waitUntil: "networkidle" });
  await page.evaluate(() => document.fonts.ready);
  const metrics = await page.evaluate(() => ({
    lcpMs: window.__dalObscuraWebVitals.lcp,
    cls: window.__dalObscuraWebVitals.cls,
    navigationMs: performance.getEntriesByType("navigation")[0]?.duration ?? null,
  }));
  if (metrics.lcpMs === null) throw new Error("Largest Contentful Paint was not reported by the browser");
  if (metrics.cls > 0.1) throw new Error(`Cumulative Layout Shift ${metrics.cls} exceeds 0.1`);

  console.log(JSON.stringify({ url, viewport: { width, height }, metrics }, null, 2));
} finally {
  await browser.close();
}
