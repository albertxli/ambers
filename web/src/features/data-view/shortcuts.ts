// The keys of the Data View, as the shortcuts panel lists them. `mod` is
// written Ctrl, or ⌘ on a Mac.

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
        { keys: ["Home", "End"], does: "First or last variable of the row" },
        { keys: [`${mod} + ↑`, `${mod} + ↓`], does: "First or last row" },
        { keys: [`${mod} + ←`, `${mod} + →`], does: "First or last variable" },
        { keys: [`${mod} + Home`, `${mod} + End`], does: "First or last cell of the file" },
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
      title: "Filtering",
      shortcuts: [
        { keys: [`${mod} + K`], does: "Add a filter" },
        { keys: ["↑", "↓", "Enter"], does: "Choose in the filter builder" },
        { keys: ["Esc"], does: "Close the filter builder" },
      ],
    },
    {
      title: "This panel",
      shortcuts: [{ keys: ["?"], does: "Show or hide the keyboard shortcuts" }],
    },
  ];
}
