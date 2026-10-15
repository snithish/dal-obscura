import { WorkspaceErrorBoundary } from "./WorkspaceErrorBoundary";
import {
  createContext,
  Fragment,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { flushSync } from "react-dom";
import { QueryClient } from "@tanstack/react-query";
import { controlPlane, type Session, type SessionOptions, type UiAuthConfig } from "../../data/api";
import { isAbortError } from "../../data/async";
import { recoveryMessage } from "../../data/recovery";
import { useConfirmation } from "../../shared/ConfirmationProvider";

function useSessionBoundary() {
  const { cancelConfirmation } = useConfirmation();
  const [session, setSession] = useState<Session | null>(null);
  const [generation, setGeneration] = useState(0);
  const currentGeneration = useRef(0);
  const controllers = useRef(new Set<AbortController>());
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: { retry: false, staleTime: 0 },
          mutations: { retry: false },
        },
      }),
  );
  const [status, setStatus] = useState<"loading" | "unavailable" | "ready">("loading");
  const [notice, setNotice] = useState("Loading workspace…");
  const [sessionOptions, setSessionOptions] = useState<SessionOptions | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [bootstrapToken, setBootstrapToken] = useState("");
  const [authError, setAuthError] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const loginInFlight = useRef(false);
  const [logoutPending, setLogoutPending] = useState(false);
  const logoutInFlight = useRef(false);

  const isCurrent = useCallback((captured: number) => captured === currentGeneration.current, []);
  const registerRequest = useCallback(() => {
    const controller = new AbortController();
    controllers.current.add(controller);
    return {
      signal: controller.signal,
      finish: () => {
        controllers.current.delete(controller);
      },
    };
  }, []);
  const invalidate = useCallback(() => {
    // Fence continuations before aborting, clearing caches or unmounting children.
    const next = ++currentGeneration.current;
    for (const controller of controllers.current) controller.abort();
    controllers.current.clear();
    void queryClient.cancelQueries();
    queryClient.clear();
    cancelConfirmation();
    loginInFlight.current = false;
    setLoggingIn(false);
    setSession(null);
    setGeneration(next);
    setStatus("unavailable");
    return next;
  }, [cancelConfirmation, queryClient]);

  const loadPublicOptions = useCallback(
    async (captured: number) => {
      const request = registerRequest();
      try {
        const options = await controlPlane.getSessionOptions(request.signal).catch(() => null);
        if (!isCurrent(captured)) return;
        const config =
          options?.oidc ?? (await controlPlane.getUiAuthConfig(request.signal).catch(() => null));
        if (!isCurrent(captured)) return;
        setSessionOptions(options);
        setAuthConfig(config);
      } finally {
        request.finish();
      }
    },
    [isCurrent, registerRequest],
  );

  const reconnect = useCallback(async () => {
    const captured = invalidate();
    setStatus("loading");
    const request = registerRequest();
    try {
      const loaded = await controlPlane.getSession(request.signal);
      if (!isCurrent(captured)) return;
      setSession(loaded);
      setStatus("ready");
    } catch (error) {
      if (isAbortError(error) || !isCurrent(captured)) return;
      setStatus("unavailable");
      setNotice(
        recoveryMessage(
          error,
          "Workspace unavailable. Sign in or reconnect to the control plane; no demo data is shown automatically.",
        ),
      );
      await loadPublicOptions(captured);
    } finally {
      request.finish();
    }
  }, [invalidate, isCurrent, loadPublicOptions, registerRequest]);

  async function bootstrapLogin() {
    if (loginInFlight.current || logoutInFlight.current) return;
    const token = bootstrapToken.trim();
    if (!token) {
      setAuthError("Enter the local control-plane token to continue.");
      return;
    }
    loginInFlight.current = true;
    setLoggingIn(true);
    setAuthError("");
    const captured = currentGeneration.current;
    const request = registerRequest();
    try {
      await controlPlane.bootstrapLogin(token, request.signal);
      if (!isCurrent(captured)) return;
      setBootstrapToken("");
      setAuthError("");
      await reconnect();
    } catch (error) {
      if (isAbortError(error) || !isCurrent(captured)) return;
      setAuthError(
        (error as { status?: number })?.status === 429
          ? "Too many sign-in attempts. Wait a moment and try again."
          : "That local token was not accepted. Check the control-plane configuration and try again.",
      );
      setStatus("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      request.finish();
      if (isCurrent(captured)) {
        loginInFlight.current = false;
        setLoggingIn(false);
      }
    }
  }

  async function logout() {
    if (logoutInFlight.current) return;
    logoutInFlight.current = true;
    flushSync(() => {
      invalidate();
    });
    const captured = currentGeneration.current;
    // Public recovery stays available while revocation is in flight.
    void loadPublicOptions(captured);
    const request = registerRequest();
    try {
      const result = await controlPlane.logout(request.signal);
      if (!isCurrent(captured)) return;
      setLogoutPending(false);
      if (result.logout_url) {
        window.location.assign(result.logout_url);
        return;
      }
      setNotice("Signed out. No policy data remains loaded in this browser.");
    } catch (error) {
      if (isAbortError(error) || !isCurrent(captured)) return;
      setLogoutPending(true);
      setNotice(
        "Sign out could not be confirmed. Private data is hidden; retry sign out before closing this browser.",
      );
    } finally {
      request.finish();
      logoutInFlight.current = false;
    }
  }

  useEffect(() => {
    void reconnect();
    const expired = (event: Event) => {
      const detail = (event as CustomEvent<{ code?: string }>).detail;
      flushSync(() => {
        invalidate();
      });
      const captured = currentGeneration.current;
      setNotice(
        detail?.code === "auth_challenge"
          ? recoveryMessage(
              detail,
              "Your session expired or was revoked. Sign in again to continue.",
            )
          : "Your session expired or was revoked. Sign in again to continue.",
      );
      void loadPublicOptions(captured);
    };
    window.addEventListener("dal-obscura-auth-expired", expired);
    return () => {
      window.removeEventListener("dal-obscura-auth-expired", expired);
      ++currentGeneration.current;
      for (const controller of controllers.current) controller.abort();
      controllers.current.clear();
      void queryClient.cancelQueries();
      queryClient.clear();
    };
  }, [invalidate, loadPublicOptions, queryClient, reconnect]);

  return {
    session,
    generation,
    queryClient,
    registerRequest,
    isCurrent,
    status,
    notice,
    sessionOptions,
    authConfig,
    bootstrapToken,
    setBootstrapToken,
    authError,
    loggingIn,
    logoutPending,
    bootstrapLogin,
    logout,
    reconnect,
    invalidate,
  };
}

const SessionScope = createContext<ReturnType<typeof useSessionBoundary> | null>(null);

export function SessionBoundary({ children }: { children: ReactNode }) {
  const scope = useSessionBoundary();
  return (
    <SessionScope.Provider value={scope}>
      <WorkspaceErrorBoundary onFailure={scope.invalidate} onRecover={() => void scope.reconnect()}>
        <Fragment key={scope.generation}>{children}</Fragment>
      </WorkspaceErrorBoundary>
    </SessionScope.Provider>
  );
}

export function useSessionScope() {
  const scope = useContext(SessionScope);
  if (!scope) throw new Error("useSessionScope requires SessionBoundary");
  return scope;
}
