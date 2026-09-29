import type { ReactNode } from "react";
import { MantineProvider, localStorageColorSchemeManager } from "@mantine/core";
import { setNonce } from "get-nonce";
import { cssVariablesResolver, theme } from "./theme";
import { ConfirmationProvider } from "../components/ConfirmationProvider";
import "@mantine/core/styles.css";
import "@fontsource/ibm-plex-sans/latin-400.css";
import "@fontsource/ibm-plex-sans/latin-500.css";
import "@fontsource/ibm-plex-sans/latin-600.css";
import "@fontsource/ibm-plex-mono/latin-400.css";

const colorSchemeManager = localStorageColorSchemeManager({ key: "dal-obscura-theme" });

export function AppProviders({ children }: { children: ReactNode }) {
  const nonce = document.querySelector<HTMLMetaElement>('meta[name="csp-nonce"]')?.content;
  // Only the serving gateway supplies this value. It is never a session credential.
  const styleNonce = nonce && /^[a-f0-9]{32}$/.test(nonce) ? nonce : undefined;
  if (styleNonce) setNonce(styleNonce); // Mantine's scroll-lock stylesheet uses this public API.
  return (
    <MantineProvider theme={theme} cssVariablesResolver={cssVariablesResolver}
      colorSchemeManager={colorSchemeManager} defaultColorScheme="auto"
      getStyleNonce={styleNonce ? () => styleNonce : undefined}>
      <ConfirmationProvider>{children}</ConfirmationProvider>
    </MantineProvider>
  );
}
