import { StrictMode, useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent, waitFor } from "storybook/test";
import { SessionBoundary, useSessionScope } from "./SessionBoundary";
import type { Session } from "../../data/api";

const session: Session = {
  principal: "reviewer",
  groups: [],
  capabilities: [],
  platform_admin: false,
};

function Probe() {
  const scope = useSessionScope();
  const [crash, setCrash] = useState(false);
  const [cached, setCached] = useState(false);
  if (crash) throw new Error("Synthetic render failure");
  return (
    <section aria-label="Session probe">
      <p role="status">{scope.session?.principal ?? scope.status}</p>
      <button
        onClick={() => {
          scope.queryClient.setQueryData(["private"], "secret");
          setCrash(true);
        }}
      >
        Crash feature
      </button>
      <button
        disabled={cached}
        onClick={() => {
          scope.queryClient.setQueryData(["private"], "secret");
          setCached(true);
        }}
      >
        Cache private data
      </button>
      <button onClick={() => void scope.logout()}>Sign out</button>
      <p>Cached: {scope.queryClient.getQueryCache().getAll().length}</p>
    </section>
  );
}
const meta = {
  title: "Workflows/Session boundary",
  render: () => (
    <StrictMode>
      <SessionBoundary>
        <Probe />
      </SessionBoundary>
    </StrictMode>
  ),
  beforeEach: () => {
    const original = window.fetch;
    let signedIn = true;
    window.fetch = fn(async (input: RequestInfo | URL) => {
      const path = new URL(String(input), window.location.href).pathname;
      if (path === "/v1/session")
        return new Response(
          JSON.stringify(
            signedIn
              ? session
              : {
                  error: {
                    code: "request_rejected",
                    message: "Signed out",
                    request_id: "test-request",
                  },
                },
          ),
          {
            status: signedIn ? 200 : 403,
          },
        );
      if (path === "/v1/logout") {
        signedIn = false;
        return Response.json({ logout_url: null });
      }
      if (path === "/v1/session/options")
        return Response.json({ bootstrap_enabled: false, oidc: null });
      if (path === "/v1/ui-auth-config")
        return Response.json(
          {
            error: {
              code: "request_rejected",
              message: "Not configured",
              request_id: "test-request",
            },
          },
          { status: 404 },
        );
      throw new Error(`Unexpected synthetic request: ${path}`);
    });
    return () => {
      window.fetch = original;
    };
  },
} satisfies Meta;
export default meta;
type Story = StoryObj<typeof meta>;

export const RenderRecovery: Story = {
  play: async ({ canvas }) => {
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("reviewer"));
    await userEvent.click(canvas.getByRole("button", { name: "Crash feature" }));
    await expect(canvas.getByRole("alert")).toHaveTextContent("Workspace could not be displayed");
    await expect(canvas.getByRole("alert")).toHaveFocus();
    await userEvent.click(canvas.getByRole("button", { name: "Reconnect to workspace" }));
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("reviewer"));
    await expect(canvas.getByText("Cached: 0")).toBeVisible();
  },
};
export const Logout: Story = {
  play: async ({ canvas }) => {
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("reviewer"));
    await userEvent.click(canvas.getByRole("button", { name: "Cache private data" }));
    await expect(canvas.getByText("Cached: 1")).toBeVisible();
    await userEvent.click(canvas.getByRole("button", { name: "Sign out" }));
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("unavailable"));
    await expect(canvas.getByText("Cached: 0")).toBeVisible();
  },
};

function FailedFeature(): never {
  throw new Error("Synthetic initial render failure");
}

export const RecoveryView: Story = {
  render: () => (
    <StrictMode>
      <SessionBoundary>
        <FailedFeature />
      </SessionBoundary>
    </StrictMode>
  ),
};
