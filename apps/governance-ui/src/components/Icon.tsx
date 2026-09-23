import type { SVGProps } from "react";
import type { LucideIcon } from "lucide-react";
import {
  Activity,
  ArrowDown,
  ArrowUp,
  Check,
  ChevronDown,
  CircleAlert,
  CircleCheck,
  ClipboardCheck,
  Copy,
  Database,
  Filter,
  History,
  KeyRound,
  LogIn,
  LogOut,
  Menu,
  Pencil,
  Play,
  Plug,
  Plus,
  Redo2,
  RefreshCw,
  Save,
  Search,
  Settings,
  ShieldCheck,
  Trash2,
  Undo2,
  X,
} from "lucide-react";

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
  | "circle-check"
  | "menu"
  | "arrow-down"
  | "arrow-up"
  | "check"
  | "chevron-down"
  | "clipboard-check"
  | "copy"
  | "filter"
  | "key-round"
  | "log-in"
  | "pencil"
  | "play"
  | "plus"
  | "redo"
  | "refresh-cw"
  | "save"
  | "trash"
  | "undo"
  | "x";

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
  menu: Menu,
  "arrow-down": ArrowDown,
  "arrow-up": ArrowUp,
  check: Check,
  "chevron-down": ChevronDown,
  "clipboard-check": ClipboardCheck,
  copy: Copy,
  filter: Filter,
  "key-round": KeyRound,
  "log-in": LogIn,
  pencil: Pencil,
  play: Play,
  plus: Plus,
  redo: Redo2,
  "refresh-cw": RefreshCw,
  save: Save,
  trash: Trash2,
  undo: Undo2,
  x: X,
};

export function Icon({ name, size = 18, ...props }: { name: IconName; size?: number } & Omit<SVGProps<SVGSVGElement>, "name">) {
  const Glyph = icons[name];
  return <Glyph size={size} aria-hidden="true" focusable="false" {...props} />;
}
