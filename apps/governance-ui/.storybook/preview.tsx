import { useEffect } from "react";
import { useMantineColorScheme } from "@mantine/core";
import type { Preview } from "@storybook/react-vite";
import { AppProviders } from "../src/design/AppProviders";
import { installStoryNetworkGuard } from "../src/design/storyNetworkGuard";
import "../src/styles.css";
import "../src/design/base.css";

function Theme({ choice }: { choice: "light" | "dark" | "auto" }) {
  const { setColorScheme } = useMantineColorScheme();
  useEffect(() => setColorScheme(choice), [choice, setColorScheme]);
  return null;
}

const preview: Preview = {
  globalTypes: {
    theme: { description: "Shared application theme", toolbar: {
      icon: "circlehollow", items: [{ value: "light", title: "Light" }, { value: "dark", title: "Dark" }, { value: "auto", title: "System" }], dynamicTitle: true,
    } },
  },
  initialGlobals: { theme: "light" },
  parameters: {
    layout: "padded",
    a11y: { test: "error" },
    options: { storySort: { order: ["Foundations", "Components", "Patterns", "Workflows", "Contribution"] } },
  },
  decorators: [(Story, context) => <AppProviders><Theme choice={context.globals.theme} /><Story /></AppProviders>],
  beforeEach: async () => {
    await document.fonts.ready;
    return installStoryNetworkGuard();
  },
};
export default preview;
