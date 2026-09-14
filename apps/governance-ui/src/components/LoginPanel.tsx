import type { UiAuthConfig, SessionOptions } from "../api";
import { controlPlane } from "../api";

export type LoginPanelProps = {
  showAuth?: boolean;
  title: string;
  message: string;
  retry?: () => void;
  authConfig?: UiAuthConfig | null;
  sessionOptions?: SessionOptions | null;
  bootstrapToken?: string;
  onBootstrapToken?: (value: string) => void;
  onBootstrapLogin?: () => void;
  loggingIn?: boolean;
  authError?: string;
};

export function LoginPanel({
  showAuth = false,
  title,
  message,
  retry,
  authConfig,
  sessionOptions,
  bootstrapToken,
  onBootstrapToken,
  onBootstrapLogin,
  loggingIn,
  authError,
}: LoginPanelProps) {
  const hasLoginMethod = Boolean(authConfig?.authority || sessionOptions?.bootstrap_enabled);
  const showBootstrap =
    showAuth &&
    sessionOptions?.bootstrap_enabled &&
    !authConfig?.authority &&
    onBootstrapToken &&
    onBootstrapLogin;

  return (
    <section className={showAuth ? "coming-soon auth-panel" : "coming-soon"}>
      <span className="eyebrow">{showAuth ? "WORKSPACE ACCESS" : "WORKSPACE"}</span>
      <h2>{title}</h2>
      <p>{message}</p>
      {showAuth && authConfig?.authority && (
        <button className="primary login-shortcut" onClick={controlPlane.startLogin}>
          Sign in with SSO
        </button>
      )}
      {showBootstrap && (
        <form
          className="bootstrap-login"
          onSubmit={(event) => {
            event.preventDefault();
            onBootstrapLogin();
          }}
        >
          <label>
            Local control-plane token
            <input
              type="password"
              autoComplete="current-password"
              value={bootstrapToken ?? ""}
              onChange={(event) => onBootstrapToken(event.target.value)}
              placeholder="Paste the configured local token"
            />
          </label>
          <button className="secondary" type="submit" disabled={loggingIn}>
            {loggingIn ? "Signing in…" : "Sign in locally"}
          </button>
        </form>
      )}
      {showAuth && authError && (
        <p className="auth-error" role="alert" aria-live="assertive">
          {authError}
        </p>
      )}
      {showAuth && !hasLoginMethod && (
        <p className="help">
          No browser identity provider is configured. Ask an operator to configure OIDC before
          signing in.
        </p>
      )}
      {retry && (
        <button className="secondary" onClick={() => void retry()}>
          Retry connection
        </button>
      )}
    </section>
  );
}
