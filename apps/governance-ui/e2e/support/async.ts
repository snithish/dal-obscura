export type DeferredResponse = {
  started: Promise<void>;
  release: () => void;
  wait: () => Promise<void>;
  markStarted: () => void;
};

export function deferredResponse(): DeferredResponse {
  let markStarted!: () => void;
  let release!: () => void;
  const started = new Promise<void>((resolve) => { markStarted = resolve; });
  const released = new Promise<void>((resolve) => { release = resolve; });
  return { started, release, wait: () => released, markStarted };
}
