import { useState } from "react";

import { Icon } from "./Icon";
import "./server-notice.css";

interface Props {
  /** What to run in a terminal to get the server back. */
  command: string;
  onDismiss: () => void;
}

/**
 * Shown once the ambers server has stopped answering: what happened, and
 * the command that brings it back, ready to copy.
 */
export function ServerNotice({ command, onDismiss }: Props) {
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");

  const copyCommand = async () => {
    try {
      await navigator.clipboard.writeText(command);
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    }
  };

  return (
    <aside className="server-notice" role="alert" aria-label="The ambers server has stopped">
      <span className="server-notice-icon">
        <Icon name="stop" size={17} />
      </span>
      <div className="server-notice-content">
        <div className="server-notice-title">
          <strong>Server stopped</strong>
        </div>
        <p>
          Your data is still available. Run <code>{command}</code> in your terminal to reconnect.
        </p>
      </div>
      <button className="server-command" onClick={copyCommand} aria-label="Copy restart command">
        <Icon name={copyState === "copied" ? "check" : "copy"} size={14} />
        {copyState === "copied"
          ? "Copied"
          : copyState === "failed"
            ? "Copy it from the text"
            : "Copy command"}
      </button>
      <button
        className="server-notice-close"
        aria-label="Dismiss server notice"
        onClick={onDismiss}
      >
        <Icon name="close" size={16} />
      </button>
    </aside>
  );
}
