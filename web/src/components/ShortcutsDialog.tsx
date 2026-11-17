import { useId } from "react";

import { Icon } from "./Icon";
import { shortcutGroups } from "./shortcuts";
import { backdropClose, useModal } from "./useModal";
import "./shortcuts.css";

const MAC = /Mac|iPhone|iPad/.test(navigator.platform);

/** The keys of the grids, over the page until it is closed. */
export function ShortcutsDialog({ onClose }: { onClose: () => void }) {
  const dialog = useModal();
  const title = useId();
  return (
    <dialog
      ref={dialog}
      className="shortcuts-dialog"
      aria-labelledby={title}
      onCancel={onClose}
      onClick={backdropClose(onClose)}
    >
      <header>
        <div>
          <span>WORK A LITTLE FASTER</span>
          <h2 id={title}>Keyboard shortcuts</h2>
        </div>
        <button className="icon-button" aria-label="Close keyboard shortcuts" onClick={onClose}>
          <Icon name="close" size={17} />
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
      <footer>Selection and copying always follow the values currently on screen.</footer>
    </dialog>
  );
}
