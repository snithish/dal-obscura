import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";
import { Stack, Text, Title } from "@mantine/core";

function NetworkBoundary() {
  return <Stack>
    <Title order={2}>Synthetic story boundary</Title>
    <Text>Stories may render deterministic fixtures, but they cannot call the application API, an identity provider, or a remote origin.</Text>
  </Stack>;
}

const meta = {
  title: "Contribution/Network boundary",
  component: NetworkBoundary,
  parameters: {
    docs: { description: { component: "The preview installs a cleanup-safe network guard. Unexpected API, auth, and cross-origin requests fail immediately so Storybook never becomes a customer or identity-provider client." } },
  },
} satisfies Meta<typeof NetworkBoundary>;
export default meta;
type Story = StoryObj<typeof meta>;

export const UnexpectedRequestsFail: Story = {
  play: async () => {
    expect(() => fetch("/v1/session")).toThrow("Unexpected story network request");
    expect(() => fetch("https://idp.example.invalid/.well-known/openid-configuration"))
      .toThrow("Unexpected story network request");
  },
};
