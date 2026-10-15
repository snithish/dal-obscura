import type { Page, Request } from "@playwright/test";

export type DeferredResponse = {
  started: Promise<void>;
  completed: Promise<Request>;
  release: () => void;
  wait: () => Promise<void>;
  markStarted: (page: Page, request: Request) => void;
};

export function deferredResponse(): DeferredResponse {
  let startedResolve!: () => void;
  let completedResolve!: (request: Request) => void;
  let release!: () => void;
  const started = new Promise<void>((resolve) => {
    startedResolve = resolve;
  });
  const completed = new Promise<Request>((resolve) => {
    completedResolve = resolve;
  });
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let tracked = false;
  function markStarted(page: Page, request: Request) {
    if (tracked) return;
    tracked = true;
    const terminal = (candidate: Request) => {
      if (candidate !== request) return;
      page.off("requestfinished", terminal);
      page.off("requestfailed", terminal);
      completedResolve(request);
    };
    page.on("requestfinished", terminal);
    page.on("requestfailed", terminal);
    startedResolve();
  }
  return { started, completed, release, wait: () => released, markStarted };
}
