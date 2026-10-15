import { Component, createRef, type ReactNode } from "react";
import { Button } from "@mantine/core";

/** Session owner fences private work before recovery can mount a fresh workspace. */
export class WorkspaceErrorBoundary extends Component<
  {
    children: ReactNode;
    onFailure: () => void;
    onRecover: () => void;
  },
  { failed: boolean }
> {
  state = { failed: false };
  private recoveryView = createRef<HTMLElement>();

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch() {
    this.props.onFailure();
    this.recoveryView.current?.focus();
  }

  render() {
    if (this.state.failed)
      return (
        <section ref={this.recoveryView} tabIndex={-1} className="coming-soon" role="alert">
          <h1>Workspace could not be displayed</h1>
          <p>
            Private data has been cleared. Unsaved edits may be lost. Reconnect to load the current
            saved state.
          </p>
          <Button
            onClick={() => {
              this.props.onRecover();
              this.setState({ failed: false });
            }}
          >
            Reconnect to workspace
          </Button>
          <Button variant="default" onClick={() => window.location.reload()}>
            Reload page
          </Button>
        </section>
      );
    return this.props.children;
  }
}
