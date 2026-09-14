import type { SVGProps } from "react";

export type IconName =
  | "activity"
  | "database"
  | "history"
  | "plug"
  | "search"
  | "settings"
  | "shield-check"
  | "log-out"
  | "circle-alert"
  | "circle-check";

const paths: Record<IconName, string[]> = {
  activity: ["M3 12h4l3-9 4 18 3-9h4"],
  database: ["M4 5c0-1.1 3.6-2 8-2s8 .9 8 2-3.6 2-8 2-8-.9-8-2", "M4 5v7c0 1.1 3.6 2 8 2s8-.9 8-2V5", "M4 12v7c0 1.1 3.6 2 8 2s8-.9 8-2v-7"],
  history: ["M3 12a9 9 0 1 0 3-6.7", "M3 4v5h5", "M12 7v5l3 2"],
  plug: ["M12 22v-5", "M9 8V2", "M15 8V2", "M18 8v4a6 6 0 0 1-12 0V8z"],
  search: ["m21 21-4.3-4.3", "M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16z"],
  settings: ["M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7z", "M19.4 15a1.7 1.7 0 0 0 .3 1.9l.1.1-1.8 1.8-.1-.1a1.7 1.7 0 0 0-1.9-.3 1.7 1.7 0 0 0-1 1.5v.2h-2.5v-.2a1.7 1.7 0 0 0-1-1.5 1.7 1.7 0 0 0-1.9.3l-.1.1-1.8-1.8.1-.1A1.7 1.7 0 0 0 8.6 15a1.7 1.7 0 0 0-1.5-1H7v-2.5h.2a1.7 1.7 0 0 0 1.5-1 1.7 1.7 0 0 0-.3-1.9l-.1-.1 1.8-1.8.1.1a1.7 1.7 0 0 0 1.9.3 1.7 1.7 0 0 0 1-1.5v-.2h2.5v.2a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.9-.3l.1-.1 1.8 1.8-.1.1a1.7 1.7 0 0 0-.3 1.9 1.7 1.7 0 0 0 1.5 1h.2V14h-.2a1.7 1.7 0 0 0-1.5 1z"],
  "shield-check": ["M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z", "m9 12 2 2 4-4"],
  "log-out": ["M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4", "m16 17 5-5-5-5", "M21 12H9"],
  "circle-alert": ["M12 8v4", "M12 16h.01", "M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z"],
  "circle-check": ["m9 12 2 2 4-4", "M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z"],
};

export function Icon({ name, size = 18, ...props }: { name: IconName; size?: number } & Omit<SVGProps<SVGSVGElement>, "name">) {
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false" {...props}>{paths[name].map((path) => <path key={path} d={path} />)}</svg>;
}
