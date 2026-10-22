const phase = process.env.DAL_OBSCURA_REVIEW_PHASE ?? "after";
if (phase !== "before" && phase !== "after") {
  throw new Error("DAL_OBSCURA_REVIEW_PHASE must be before or after");
}
export const reviewPhase = phase;
