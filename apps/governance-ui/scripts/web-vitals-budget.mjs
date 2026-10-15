export const WEB_VITALS_LIMITS = Object.freeze({ lcpMs: 2500, cls: 0.1 });

export function assertWebVitals(metrics) {
  if (!Number.isFinite(metrics.lcpMs) || metrics.lcpMs < 0) {
    throw new Error("Largest Contentful Paint must be a finite, non-negative measurement");
  }
  if (metrics.lcpMs > WEB_VITALS_LIMITS.lcpMs) {
    throw new Error(
      `Largest Contentful Paint ${metrics.lcpMs} exceeds ${WEB_VITALS_LIMITS.lcpMs} ms`,
    );
  }
  if (!Number.isFinite(metrics.cls) || metrics.cls < 0) {
    throw new Error("Cumulative Layout Shift must be a finite, non-negative measurement");
  }
  if (metrics.cls > WEB_VITALS_LIMITS.cls)
    throw new Error(`Cumulative Layout Shift ${metrics.cls} exceeds ${WEB_VITALS_LIMITS.cls}`);
}
