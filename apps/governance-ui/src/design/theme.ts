import { createTheme, type CSSVariablesResolver } from "@mantine/core";

export const palette = {
  light: {
    canvas: "#F6F7F9", surface: "#FFFFFF", "surface-subtle": "#EDF1F5",
    text: "#182230", "text-secondary": "#526076", border: "#D9E0E8",
    "control-border": "#7A8799", primary: "#175CD3", "primary-contrast": "#FFFFFF",
    "primary-hover": "#144CB0", selection: "#E8F0FF", "selection-text": "#184A9B",
    focus: "#175CD3", success: "#157347", "success-surface": "#E8F5ED",
    warning: "#8A4B00", "warning-surface": "#FFF3D6", danger: "#B42318", "danger-surface": "#FEECEB",
  },
  dark: {
    canvas: "#10151E", surface: "#171F2C", "surface-subtle": "#202C3D",
    text: "#E7ECF4", "text-secondary": "#ADB9CA", border: "#334155",
    "control-border": "#7D8FA7", primary: "#85B4FF", "primary-contrast": "#0C1525",
    "primary-hover": "#A8CAFF", selection: "#203D68", "selection-text": "#D7E7FF",
    focus: "#A8CAFF", success: "#8BDEB0", "success-surface": "#163627",
    warning: "#FFD084", "warning-surface": "#3B2B12", danger: "#FFB4AB", "danger-surface": "#42201F",
  },
};

export const theme = createTheme({
  primaryColor: "cobalt",
  primaryShade: { light: 6, dark: 3 },
  colors: { cobalt: ["#E8F0FF", "#D7E7FF", "#A8CAFF", "#85B4FF", "#528FF0", "#2871E0", "#175CD3", "#144CB0", "#184A9B", "#153A78"] },
  fontFamily: '"IBM Plex Sans", system-ui, sans-serif',
  fontFamilyMonospace: '"IBM Plex Mono", monospace',
  headings: {
    fontFamily: '"IBM Plex Sans", system-ui, sans-serif', fontWeight: "600",
    sizes: { h1: { fontSize: "1.75rem", lineHeight: "1.2857" }, h2: { fontSize: "1.25rem", lineHeight: "1.4" } },
  },
  defaultRadius: "sm",
  autoContrast: true,
});

const semanticVariables = (tokens: typeof palette.light) => ({
  ...Object.fromEntries(Object.entries(tokens).map(([key, value]) => [`--${key}`, value])),
  "--mantine-color-body": tokens.canvas,
  "--mantine-color-text": tokens.text,
  "--mantine-color-dimmed": tokens["text-secondary"],
  "--mantine-color-anchor": tokens.primary,
  "--mantine-color-default": tokens.surface,
  "--mantine-color-default-hover": tokens["surface-subtle"],
  "--mantine-color-default-color": tokens.text,
  "--mantine-color-default-border": tokens["control-border"],
  "--mantine-color-cobalt-filled": tokens.primary,
  "--mantine-color-cobalt-filled-hover": tokens["primary-hover"],
  "--mantine-color-cobalt-light": tokens.selection,
  "--mantine-color-cobalt-light-color": tokens["selection-text"],
});

export const cssVariablesResolver: CSSVariablesResolver = () => ({
  variables: {}, light: semanticVariables(palette.light), dark: semanticVariables(palette.dark),
});
