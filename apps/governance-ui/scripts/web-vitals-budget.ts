export const WEB_VITALS_LIMITS = Object.freeze({ lcpMs: 2500, cls: 0.1 });

export function assertWebVitals(metrics: { lcpMs: unknown; cls: unknown }) {
  if (typeof metrics.lcpMs !== "number" || !Number.isFinite(metrics.lcpMs) || metrics.lcpMs < 0) {
    throw new Error("Largest Contentful Paint must be a finite, non-negative measurement");
  }
  if (metrics.lcpMs > WEB_VITALS_LIMITS.lcpMs) {
    throw new Error(
      `Largest Contentful Paint ${metrics.lcpMs} exceeds ${WEB_VITALS_LIMITS.lcpMs} ms`,
    );
  }
  if (typeof metrics.cls !== "number" || !Number.isFinite(metrics.cls) || metrics.cls < 0) {
    throw new Error("Cumulative Layout Shift must be a finite, non-negative measurement");
  }
  if (metrics.cls > WEB_VITALS_LIMITS.cls)
    throw new Error(`Cumulative Layout Shift ${metrics.cls} exceeds ${WEB_VITALS_LIMITS.cls}`);
}

declare global {
  interface Window {
    __dalObscuraWebVitals: { lcp: number | null; cls: number };
  }
}

type LayoutShift = PerformanceEntry & { hadRecentInput: boolean; value: number };

export function observeWebVitals() {
  window.__dalObscuraWebVitals = { lcp: null, cls: 0 };
  new PerformanceObserver((list) => {
    const last = list.getEntries().at(-1);
    if (last) window.__dalObscuraWebVitals.lcp = last.startTime;
  }).observe({ type: "largest-contentful-paint", buffered: true });
  new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      const shift = entry as LayoutShift;
      if (!shift.hadRecentInput) window.__dalObscuraWebVitals.cls += shift.value;
    }
  }).observe({ type: "layout-shift", buffered: true });
}
