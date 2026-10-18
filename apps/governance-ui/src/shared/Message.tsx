import type { ReactNode } from "react";

export type MessageState = { tone: "success" | "error" | "info"; text: string };

export function Message({ tone, children }: { tone: MessageState["tone"]; children: ReactNode }) {
  return (
    <div className={`message msg-${tone}`} role={tone === "error" ? "alert" : "status"}>
      {children}
    </div>
  );
}
