import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        border: "#d8e1e7",
        ink: "#14212a",
        muted: "#63717d",
        panel: "#ffffff",
        soft: "#eef3f6",
        accent: "#17866e",
      },
      borderRadius: {
        card: "8px",
      },
      boxShadow: {
        panel: "0 12px 30px rgba(38, 50, 58, 0.08)",
      },
    },
  },
  plugins: [],
} satisfies Config;
