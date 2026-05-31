import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        app: "#f7f8fa",
        border: "#d7dde5",
        ink: "#17202a",
        muted: "#5e6b78",
        panel: "#ffffff",
        soft: "#eef2f6",
        accent: "#116b5b",
        attention: "#b35c16",
        danger: "#b42318",
        info: "#315b8c",
      },
      borderRadius: {
        card: "8px",
      },
      boxShadow: {
        panel: "0 10px 24px rgba(19, 31, 44, 0.07)",
        soft: "0 8px 18px rgba(19, 31, 44, 0.12)",
      },
    },
  },
  plugins: [],
} satisfies Config;
