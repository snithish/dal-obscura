import type { SVGProps } from "react";
import type { LucideIcon } from "lucide-react";
import { Activity, CircleAlert, CircleCheck, Database, History, LogOut, Plug, Search, Settings, ShieldCheck } from "lucide-react";

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

const icons: Record<IconName, LucideIcon> = {
  activity: Activity,
  database: Database,
  history: History,
  plug: Plug,
  search: Search,
  settings: Settings,
  "shield-check": ShieldCheck,
  "log-out": LogOut,
  "circle-alert": CircleAlert,
  "circle-check": CircleCheck,
};

export function Icon({ name, size = 18, ...props }: { name: IconName; size?: number } & Omit<SVGProps<SVGSVGElement>, "name">) {
  const Glyph = icons[name];
  return <Glyph size={size} aria-hidden="true" focusable="false" {...props} />;
}
