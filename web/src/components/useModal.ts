// A `<dialog>` shown over the page: the browser keeps the keyboard inside
// it, closes it on Esc (`onCancel`) and dims what is behind.

import { type MouseEvent, type RefObject, useEffect, useRef } from "react";

/**
 * Opens the dialog it is given to when that appears, and closes it when
 * it goes. The keyboard returns to where it was.
 */
export function useModal(): RefObject<HTMLDialogElement | null> {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const element = dialog.current;
    const opener = document.activeElement;
    element?.showModal();
    return () => {
      element?.close();
      if (opener instanceof HTMLElement && opener.isConnected && opener.getClientRects().length) {
        opener.focus({ preventScroll: true });
      }
    };
  }, []);
  return dialog;
}

/** For a dialog's `onClick`: a click on the dimmed page around it closes it. */
export function backdropClose(onClose: () => void) {
  return (event: MouseEvent<HTMLDialogElement>) => {
    // A click on the dialog's own content has another target.
    if (event.target !== event.currentTarget) return;
    const box = event.currentTarget.getBoundingClientRect();
    const outside =
      event.clientX < box.left ||
      event.clientX > box.right ||
      event.clientY < box.top ||
      event.clientY > box.bottom;
    if (outside) onClose();
  };
}
