import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { Button, Group, Modal, Text } from "@mantine/core";

type Confirmation = { title: string; message: string; confirmLabel: string; cancelLabel?: string; destructive?: boolean };
type Request = Confirmation & { owner: symbol; resolve: (confirmed: boolean) => void };
const Context = createContext<{
  ask: (options: Confirmation, owner: symbol) => Promise<boolean>;
  cancel: (owner: symbol) => void;
} | null>(null);

export function ConfirmationProvider({ children }: { children: ReactNode }) {
  const [request, setRequest] = useState<Request | null>(null);
  const [opened, setOpened] = useState(false);
  const pending = useRef<Request | null>(null);
  const settle = useCallback((confirmed: boolean) => {
    const current = pending.current;
    pending.current = null;
    setOpened(false);
    current?.resolve(confirmed);
  }, []);
  const ask = useCallback((options: Confirmation, owner: symbol) => {
    // A second click must not queue another destructive action.
    if (pending.current) return Promise.resolve(false);
    return new Promise<boolean>((resolve) => {
      const next = { ...options, owner, resolve };
      pending.current = next;
      setRequest(next);
      setOpened(true);
    });
  }, []);
  const cancel = useCallback((owner: symbol) => {
    if (pending.current?.owner === owner) settle(false);
  }, [settle]);
  useEffect(() => () => { pending.current?.resolve(false); pending.current = null; }, []);
  return <Context.Provider value={{ ask, cancel }}>
    {children}
    <Modal opened={opened} onClose={() => settle(false)} title={request?.title}
      centered size="md" transitionProps={{ duration: 0 }} closeButtonProps={{ "aria-label": "Cancel action" }}>
      <Text>{request?.message}</Text>
      <Group justify="flex-end" mt="lg">
        <Button variant="default" data-autofocus onClick={() => settle(false)}>{request?.cancelLabel ?? "Cancel"}</Button>
        <Button variant={request?.destructive ? "default" : "filled"} className={request?.destructive ? "danger" : undefined} onClick={() => settle(true)}>{request?.confirmLabel}</Button>
      </Group>
    </Modal>
  </Context.Provider>;
}

export function useConfirmation() {
  const context = useContext(Context);
  if (!context) throw new Error("ConfirmationProvider is required");
  const { ask, cancel } = context;
  const owner = useRef(Symbol("confirmation"));
  const confirm = useCallback((options: Confirmation) => ask(options, owner.current), [ask]);
  const cancelConfirmation = useCallback(() => cancel(owner.current), [cancel]);
  useEffect(() => cancelConfirmation, [cancelConfirmation]);
  return { confirm, cancelConfirmation };
}

export const discardChanges = (scope: string, action = "leave this view"): Confirmation => ({
  title: "Discard unsaved changes?",
  message: `Your ${scope} changes have not been saved. Discard them to ${action}.`,
  confirmLabel: "Discard changes",
  cancelLabel: "Keep editing",
  destructive: true,
});
