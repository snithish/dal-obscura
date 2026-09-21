import { Code, Group, Paper, Stack, Text, Title, useComputedColorScheme } from "@mantine/core";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { Icon } from "../components/Icon";
import { palette } from "./theme";

function Foundations() {
  const scheme = useComputedColorScheme("light");
  return <Stack>
    <Title order={1}>Governance, clearly expressed</Title>
    <Text c="dimmed">IBM Plex Sans for work; IBM Plex Mono for identifiers and expressions.</Text>
    <Code>customer.address.country = 'NL'</Code>
    <Group><Icon name="database" /><Icon name="history" /><Icon name="settings" /><Text>Lucide is the only product icon family.</Text></Group>
    <Title order={2}>Semantic color roles · {scheme}</Title>
    {Object.entries(palette[scheme]).map(([role, value]) => <Paper key={role} withBorder p="sm">
      <Group><span aria-hidden="true" style={{ background: value, width: 32, height: 32, border: "1px solid var(--control-border)" }} /><Text>{role}</Text><Code>{value}</Code></Group>
    </Paper>)}
  </Stack>;
}
const meta = { title: "Foundations/Theme", component: Foundations } satisfies Meta<typeof Foundations>;
export default meta;
type Story = StoryObj<typeof meta>;
export const Light: Story = {};
export const Dark: Story = { globals: { theme: "dark" } };
