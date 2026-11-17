import {
  type MouseEvent,
  type PointerEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";

import type { Variable } from "../api/client";
import { count } from "../lib/format";
import { auditFindings, missingName, missingShare, storageName } from "../lib/variable";
import "./variable-card.css";

/** How long the pointer rests on a name before its card opens. */
const OPEN_AFTER = 220;
/** How long after the pointer left the card stays, to be reached. */
const CLOSE_AFTER = 140;
const WIDTH = 320;
const MAX_HEIGHT = 460;
const MARGIN = 12;

/** A variable's value labels: the first `limit`, and how many more there are. */
export function ValueLabels({
  variable,
  limit,
  heading = true,
  className = "",
}: {
  variable: Variable;
  limit: number;
  heading?: boolean;
  className?: string;
}) {
  const values = variable.values;
  const missing = variable.missing_count;
  if (values.length === 0 && missing === 0) return null;
  return (
    <div className={`variable-card-values ${className}`}>
      {heading && (
        <div>
          <span>VALUE LABELS</span>
          <small>{count(values.length)} defined</small>
        </div>
      )}
      {missing > 0 && (
        <p className="variable-system-missing">
          <code aria-label={`${missingName(variable)} value`}>.</code>
          <span>{missingName(variable)}</span>
          <small>
            {count(missing)} {missing === 1 ? "row" : "rows"}
          </small>
        </p>
      )}
      {values.slice(0, limit).map((value) => (
        <p key={value.code}>
          <code>{value.code}</code>
          <span>{value.label}</span>
          {value.user_missing && <small className="variable-user-missing">user-missing</small>}
        </p>
      ))}
      {values.length > limit && (
        <small className="variable-more-labels">
          +{count(values.length - limit)} more value{" "}
          {values.length - limit === 1 ? "label" : "labels"}
        </small>
      )}
    </div>
  );
}

interface CardProps {
  variable: Variable;
  /** Rows the counts cover. */
  countedRows: number;
  /** Which rows the counts cover, in words. */
  scope: string;
}

/** What is known about one variable, on a card beside its name. */
function Card({ variable, countedRows, scope }: CardProps) {
  const findings = auditFindings(variable);
  return (
    <>
      <div className="variable-card-heading">
        <code>{variable.name}</code>
        <span>
          {storageName(variable)} · {variable.format}
        </span>
      </div>
      <div className="variable-card-label">
        <span>VARIABLE LABEL</span>
        <p className={variable.label ? "" : "is-unavailable"}>
          {variable.label || "This variable has no label."}
        </p>
      </div>
      <ValueLabels variable={variable} limit={3} />
      <dl className="variable-card-facts">
        <div>
          <dt>Type</dt>
          <dd>{variable.kind}</dd>
        </div>
        <div>
          <dt>Measure</dt>
          <dd>{variable.measure}</dd>
        </div>
        <div>
          <dt>N</dt>
          <dd>{count(variable.valid)}</dd>
        </div>
        <div>
          <dt>Missing</dt>
          <dd>{missingShare(variable, countedRows)}</dd>
        </div>
        {variable.missing && (
          <div>
            <dt>User-missing</dt>
            <dd>{variable.missing}</dd>
          </div>
        )}
      </dl>
      {findings.length > 0 && (
        <p className="variable-card-warning">
          Label check: {variable.label_check}
        </p>
      )}
      <div className="variable-card-footer">
        <span className="variable-metadata-dot" /> Counts cover {scope}
      </div>
    </>
  );
}

interface Options {
  /** The variables of the file, by position: the card shows the latest. */
  variables: readonly Variable[];
  countedRows: number;
  scope: string;
  /** False while something else has the pointer (a column being resized). */
  enabled: boolean;
}

/** What an element is given so that pointing at it opens a variable's card. */
export interface CardTrigger {
  onPointerEnter: (event: PointerEvent<HTMLElement>) => void;
  onPointerLeave: () => void;
  onClick: (event: MouseEvent<HTMLElement>) => void;
  "aria-describedby": string | undefined;
}

/**
 * One card for a whole grid. A file has hundreds of variables; each
 * header only says "show it for me" (`trigger`), and the one card is
 * placed beside whichever asked.
 */
export function useVariableCard(options: Options): {
  trigger: (variable: Variable) => CardTrigger;
  card: ReactNode;
} {
  const { variables, enabled } = options;
  const id = useId();
  const [open, setOpen] = useState<{ index: number; anchor: HTMLElement } | null>(null);
  const [place, setPlace] = useState({ left: MARGIN, top: 0, width: WIDTH, maxHeight: MAX_HEIGHT });
  const card = useRef<HTMLDivElement>(null);
  const timer = useRef(0);

  const cancel = () => window.clearTimeout(timer.current);
  const closeSoon = () => {
    cancel();
    timer.current = window.setTimeout(() => setOpen(null), CLOSE_AFTER);
  };
  useEffect(() => cancel, []);
  useEffect(() => {
    if (!enabled) {
      cancel();
      setOpen(null);
    }
  }, [enabled]);

  const anchor = open?.anchor;
  const measure = useCallback(() => {
    if (!anchor) return;
    const box = anchor.getBoundingClientRect();
    const width = Math.min(WIDTH, window.innerWidth - 2 * MARGIN);
    const height = Math.min(
      card.current?.getBoundingClientRect().height ?? 250,
      MAX_HEIGHT,
      window.innerHeight - 2 * MARGIN,
    );
    const below = box.bottom + 9;
    // Under the name, or above it when there is no room.
    const top =
      below + height <= window.innerHeight - MARGIN
        ? below
        : Math.max(MARGIN, box.top - height - 9);
    setPlace({
      left: Math.max(MARGIN, Math.min(box.left, window.innerWidth - width - MARGIN)),
      top,
      width,
      maxHeight: Math.min(MAX_HEIGHT, window.innerHeight - top - MARGIN),
    });
  }, [anchor]);
  const index = open?.index;
  useLayoutEffect(measure, [measure, index]);

  useEffect(() => {
    if (!anchor) return;
    const close = () => setOpen(null);
    const onPointerDown = (event: Event) => {
      const target = event.target as Node;
      if (!anchor.contains(target) && !card.current?.contains(target)) close();
    };
    const onScroll = (event: Event) => {
      // The card's own list of labels may scroll.
      if (event.target instanceof Node && card.current?.contains(event.target)) return;
      close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", close);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", close);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [anchor]);

  const trigger = (variable: Variable): CardTrigger => ({
    onPointerEnter: (event) => {
      if (!enabled || event.pointerType === "touch") return;
      const element = event.currentTarget;
      cancel();
      timer.current = window.setTimeout(
        () => setOpen({ index: variable.index, anchor: element }),
        OPEN_AFTER,
      );
    },
    onPointerLeave: closeSoon,
    onClick: (event) => {
      if (!enabled) return;
      cancel();
      setOpen({ index: variable.index, anchor: event.currentTarget });
    },
    "aria-describedby": open?.index === variable.index ? id : undefined,
  });

  const shown = open ? variables[open.index] : undefined;
  return {
    trigger,
    card:
      shown &&
      createPortal(
        <div
          ref={card}
          id={id}
          role="tooltip"
          className="variable-hover-card"
          style={place}
          onPointerEnter={cancel}
          onPointerLeave={closeSoon}
        >
          <Card variable={shown} countedRows={options.countedRows} scope={options.scope} />
        </div>,
        document.body,
      ),
  };
}
