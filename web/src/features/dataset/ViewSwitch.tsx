import { useRef } from "react";

import { Icon, type IconName } from "../../components/Icon";

export interface ViewTab {
  id: string;
  label: string;
  icon: IconName;
}

interface Props {
  tabs: readonly ViewTab[];
  value: string;
  onChange: (id: string) => void;
  /** Starts the ids that tie each tab to its panel. */
  prefix: string;
}

/** The id of a view's tab and of its panel. */
export function tabId(prefix: string, view: string): string {
  return `${prefix}-${view}-tab`;
}

export function panelId(prefix: string, view: string): string {
  return `${prefix}-${view}-panel`;
}

/** The tabs that choose between the views of a file: Data, Variables. */
export function ViewSwitch({ tabs, value, onChange, prefix }: Props) {
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  const index = Math.max(
    tabs.findIndex((tab) => tab.id === value),
    0,
  );
  return (
    <div
      className="view-mode-switch"
      role="tablist"
      aria-label="Views of the file"
      onKeyDown={(event) => {
        let next = index;
        if (event.key === "ArrowRight") next = (index + 1) % tabs.length;
        else if (event.key === "ArrowLeft") next = (index + tabs.length - 1) % tabs.length;
        else if (event.key === "Home") next = 0;
        else if (event.key === "End") next = tabs.length - 1;
        else return;
        event.preventDefault();
        const tab = tabs[next];
        if (!tab) return;
        onChange(tab.id);
        buttons.current[next]?.focus();
      }}
    >
      <span className="view-mode-indicator" style={{ transform: `translateX(${index * 100}%)` }} />
      {tabs.map((tab, place) => (
        <button
          key={tab.id}
          ref={(element) => {
            buttons.current[place] = element;
          }}
          type="button"
          role="tab"
          id={tabId(prefix, tab.id)}
          aria-selected={value === tab.id}
          aria-controls={panelId(prefix, tab.id)}
          tabIndex={value === tab.id ? 0 : -1}
          onClick={() => onChange(tab.id)}
        >
          <Icon name={tab.icon} size={13} />
          {tab.label}
        </button>
      ))}
    </div>
  );
}
