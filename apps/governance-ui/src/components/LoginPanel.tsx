import type { UiAuthConfig, SessionOptions } from "../api";
import { controlPlane } from "../api";
import { Alert, Button, Paper, Stack, Text, TextInput, Title } from "@mantine/core";

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
    <Paper component="section" className="login-panel" withBorder p="xl" radius="md">
      <Stack gap="md">
      <Text size="sm" fw={500} c="dimmed">{showAuth ? "Workspace access" : "Workspace"}</Text>
      <Title order={2}>{title}</Title>
      <Text c="dimmed">{message}</Text>
      {showAuth && authConfig?.authority && (
        <Button type="button" onClick={controlPlane.startLogin}>
          Sign in with SSO
        </Button>
      )}
      {showBootstrap && (
        <form
          className="bootstrap-login"
          onSubmit={(event) => {
            event.preventDefault();
            onBootstrapLogin();
          }}
        >
            <TextInput
              label="Local control-plane token"
              type="password"
              autoComplete="current-password"
              value={bootstrapToken ?? ""}
              onChange={(event) => onBootstrapToken(event.target.value)}
              placeholder="Paste the configured local token"
            />
          <Button type="submit" loading={loggingIn}>
            {loggingIn ? "Signing in…" : "Sign in locally"}
          </Button>
        </form>
      )}
      {showAuth && authError && (
        <Alert color="red" role="alert" aria-live="assertive">
          {authError}
        </Alert>
      )}
      {showAuth && !hasLoginMethod && (
        <p className="help">
          No browser identity provider is configured. Ask an operator to configure OIDC before
          signing in.
        </p>
      )}
      {retry && (
        <Button type="button" variant="default" onClick={() => void retry()}>
          Retry connection
        </Button>
      )}
      </Stack>
    </Paper>
  );
}
