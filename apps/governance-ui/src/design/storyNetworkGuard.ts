type StoryRequest = string | URL | Request;

export function localStoryUrl(input: StoryRequest): URL {
  const raw = input instanceof Request ? input.url : input instanceof URL ? input.href : input;
  const url = new URL(raw, window.location.href);
  if (url.origin !== window.location.origin) {
    throw new Error(`Unexpected story network request: ${url.href}`);
  }
  return url;
}

function assertStoryRequest(input: StoryRequest): void {
  const url = localStoryUrl(input);
  if (url.pathname.startsWith("/v1/") || url.pathname.startsWith("/auth/")) {
    throw new Error(`Unexpected story network request: ${url.href}`);
  }
}

/** Prevent synthetic stories from calling the app, an IdP, or a remote origin. */
export function installStoryNetworkGuard(): () => void {
  const originalFetch = window.fetch;
  const originalOpen = XMLHttpRequest.prototype.open;
  const originalBeacon = navigator.sendBeacon;

  window.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    assertStoryRequest(input as StoryRequest);
    return originalFetch.call(window, input, init);
  }) as typeof window.fetch;

  XMLHttpRequest.prototype.open = function (
    method: string,
    url: string | URL,
    asyncFlag: boolean = true,
    username?: string | null,
    password?: string | null,
  ) {
    assertStoryRequest(url);
    return Reflect.apply(originalOpen, this, [method, url, asyncFlag, username, password]);
  };

  navigator.sendBeacon = ((url: string | URL, data?: BodyInit | null) => {
    assertStoryRequest(url);
    return originalBeacon.call(navigator, url, data);
  }) as typeof navigator.sendBeacon;

  return () => {
    window.fetch = originalFetch;
    XMLHttpRequest.prototype.open = originalOpen;
    navigator.sendBeacon = originalBeacon;
  };
}
