import { type MouseEvent, useRef } from "react";

import type { CellMode } from "../../api/client";

const MODES: { value: CellMode; label: string; hint: string }[] = [
  { value: "labels", label: "Labels", hint: "Value labels; the value itself when it has none" },
  { value: "codes", label: "Codes", hint: "Stored values" },
  { value: "both", label: "Both", hint: "Value labels with their stored values" },
];

interface Props {
  value: CellMode;
  onChange: (mode: CellMode) => void;
  /** The file has no value labels: every mode would show the same. */
  disabled: boolean;
}

/**
 * For a control pressed with the mouse: the keyboard focus stays where it
 * was, so the arrow keys keep moving in the grid after a click on Labels /
 * Codes / Both. The buttons are still reached with Tab.
 */
function keepFocus(event: MouseEvent) {
  event.preventDefault();
}

/** What cells show: value labels, stored values, or both. */
export function ValueDisplaySwitch({ value, onChange, disabled }: Props) {
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  const index = MODES.findIndex((mode) => mode.value === value);
  return (
    <div
      className="value-display-switch"
      role="radiogroup"
      aria-label="What cells show"
      aria-disabled={disabled}
      title={disabled ? "This file has no value labels" : undefined}
      onKeyDown={(event) => {
        if (disabled) return;
        let next = index;
        if (event.key === "ArrowRight" || event.key === "ArrowDown") next = (index + 1) % MODES.length;
        else if (event.key === "ArrowLeft" || event.key === "ArrowUp")
          next = (index + MODES.length - 1) % MODES.length;
        else if (event.key === "Home") next = 0;
        else if (event.key === "End") next = MODES.length - 1;
        else return;
        event.preventDefault();
        const mode = MODES[next];
        if (!mode) return;
        onChange(mode.value);
        buttons.current[next]?.focus();
      }}
    >
      <span
        className="value-display-indicator"
        style={{ transform: `translateX(${index * 100}%)` }}
      />
      {MODES.map((mode, place) => (
        <button
          key={mode.value}
          ref={(element) => {
            buttons.current[place] = element;
          }}
          type="button"
          role="radio"
          aria-checked={value === mode.value}
          tabIndex={value === mode.value ? 0 : -1}
          disabled={disabled}
          title={disabled ? "This file has no value labels" : mode.hint}
          className={value === mode.value ? "is-selected" : ""}
          onMouseDown={keepFocus}
          onClick={() => onChange(mode.value)}
        >
          {mode.label}
        </button>
      ))}
    </div>
  );
}
