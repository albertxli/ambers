# 0004: The open Dracula palette, not Dracula Pro

Date: 2026-11-12
Status: accepted

## Decision

The viewers use the open Dracula palette (draculatheme.com, MIT), in the terminal (`view/theme.rs`) and in the browser. Dracula Pro colour values are not put in this repository, even though the design mock-ups were drawn with them.

In the browser, every colour is a named token in one theme file, so a different palette can be swapped in without touching components.

## Alternatives considered

- **Copy the colours from the design mock-up.** They are Dracula Pro, a paid theme. Publishing its palette in an MIT-licensed public repository is not ours to do.
- **Design a palette of our own.** Possible later; nothing in the code depends on the palette being Dracula.

## Why

ambers is MIT-licensed and public. Colours taken from a paid product do not belong in it. The open palette is close in character and free to use.
