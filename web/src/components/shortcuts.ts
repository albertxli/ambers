// The keys of the two grids (Data View, Variable View), as the shortcuts
// dialog lists them. `mod` is written Ctrl, or ⌘ on a Mac.

export interface Shortcut {
  keys: string[];
  does: string;
}

export interface ShortcutGroup {
  title: string;
  shortcuts: Shortcut[];
}

export function shortcutGroups(mac: boolean): ShortcutGroup[] {
  const mod = mac ? "⌘" : "Ctrl";
  return [
    {
      title: "Moving",
      shortcuts: [
        { keys: ["↑", "↓", "←", "→"], does: "Move one cell" },
        { keys: ["Page Up", "Page Down"], does: "Move one screen" },
        { keys: ["Home", "End"], does: "First or last column of the row" },
        { keys: [`${mod} + ↑`, `${mod} + ↓`], does: "First or last row" },
        { keys: [`${mod} + ←`, `${mod} + →`], does: "First or last column" },
        { keys: [`${mod} + Home`, `${mod} + End`], does: "First or last cell" },
      ],
    },
    {
      title: "Selecting and copying",
      shortcuts: [
        { keys: ["Drag"], does: "Select a block of cells" },
        { keys: ["Shift + click"], does: "Extend the selection to a cell" },
        { keys: ["Shift + arrows"], does: "Extend the selection by one cell" },
        { keys: [`${mod} + A`], does: "Select every cell" },
        { keys: ["Esc"], does: "Back to one cell" },
        { keys: [`${mod} + C`], does: "Copy the selection, to paste into a spreadsheet" },
      ],
    },
    {
      title: "Columns",
      shortcuts: [
        { keys: ["Drag header edge"], does: "Resize a column" },
        { keys: ["Double-click edge"], does: "Back to the column's own width" },
        { keys: ["←", "→"], does: "On a header edge: resize by 10 pixels (Shift: 50)" },
        { keys: ["Enter"], does: "On a header edge: back to its own width" },
      ],
    },
    {
      title: "Filtering and searching",
      shortcuts: [
        { keys: [`${mod} + K`], does: "Data: add a filter. Variables: search" },
        { keys: ["↑", "↓", "Enter"], does: "Choose in the filter builder" },
        { keys: ["Esc"], does: "Close the filter builder, a card or a dialog" },
      ],
    },
    {
      title: "This list",
      shortcuts: [{ keys: ["?"], does: "Show or hide the keyboard shortcuts" }],
    },
  ];
}
