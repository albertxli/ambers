import { useEffect, useRef } from "react";

import { Icon } from "../../components/Icon";
import { shortcutGroups } from "./shortcuts";

const MAC = /Mac|iPhone|iPad/.test(navigator.platform);

/** The keys of the Data View, over the page until it is closed. */
export function ShortcutsPanel({ onClose }: { onClose: () => void }) {
  const closeButton = useRef<HTMLButtonElement>(null);
  useEffect(() => closeButton.current?.focus(), []);

  return (
    <div
      className="shortcuts-backdrop"
      onPointerDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        className="shortcuts-panel"
        role="dialog"
        aria-modal="true"
        aria-label="Keyboard shortcuts"
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            onClose();
          }
        }}
      >
        <header>
          <h2>Keyboard shortcuts</h2>
          <button
            ref={closeButton}
            className="icon-button"
            aria-label="Close keyboard shortcuts"
            onClick={onClose}
          >
            <Icon name="close" size={14} />
          </button>
        </header>
        <div className="shortcuts-groups">
          {shortcutGroups(MAC).map((group) => (
            <section key={group.title}>
              <h3>{group.title}</h3>
              <dl>
                {group.shortcuts.map((shortcut) => (
                  <div key={shortcut.does}>
                    <dt>
                      {shortcut.keys.map((key) => (
                        <kbd key={key}>{key}</kbd>
                      ))}
                    </dt>
                    <dd>{shortcut.does}</dd>
                  </div>
                ))}
              </dl>
            </section>
          ))}
        </div>
      </div>
    </div>
  );
}
