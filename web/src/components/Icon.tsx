// The few line icons the interface uses, drawn here so nothing is loaded
// from an icon font or a network.

const PATHS = {
  "arrow-left": <path d="M19 12H5m0 0 6-6m-6 6 6 6" />,
  "arrow-right": <path d="M5 12h14m0 0-6-6m6 6-6 6" />,
  check: <path d="m5 12.5 4.5 4.500L19 7" />,
  "chevron-down": <path d="m6 9 6 6 6-6" />,
  "chevron-right": <path d="m9 6 6 6-6 6" />,
  columns: (
    <>
      <rect x="3.5" y="5" width="17" height="14" rx="2" />
      <path d="M9 5v14m6-14v14" />
    </>
  ),
  copy: (
    <>
      <rect x="9" y="9" width="11" height="12" rx="1.5" />
      <path d="M15 9V4.500A1.500 1.500 0 0 0 13.500 3h-9A1.500 1.500 0 0 0 3 4.500v10A1.500 1.500 0 0 0 4.500 16H9" />
    </>
  ),
  filter: <path d="M4 6h16M7 12h10m-7 6h4" />,
  list: <path d="M4 5h16M4 12h16M4 19h16M8 3v18" />,
  more: (
    <>
      <circle cx="5.5" cy="12" r="1.2" fill="currentColor" />
      <circle cx="12" cy="12" r="1.2" fill="currentColor" />
      <circle cx="18.5" cy="12" r="1.2" fill="currentColor" />
    </>
  ),
  table: <path d="M4 4h16v16H4V4Zm0 5h16M9 4v16" />,
  plus: <path d="M12 5v14M5 12h14" />,
  stop: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <rect x="8.5" y="8.5" width="7" height="7" rx="1" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 7.5V12l3 2" />
    </>
  ),
  close: <path d="m7 7 10 10M17 7 7 17" />,
  database: (
    <>
      <ellipse cx="12" cy="6" rx="7.5" ry="3" />
      <path d="M4.5 6v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3V6" />
      <path d="M4.5 12v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" />
    </>
  ),
  files: (
    <>
      <path d="M8 3.5h7l4 4V17a1.5 1.5 0 0 1-1.5 1.5H8A1.5 1.5 0 0 1 6.500 17V5A1.5 1.5 0 0 1 8 3.5Z" />
      <path d="M15 3.5v4h4M4 7.5v12A1.5 1.5 0 0 0 5.500 21H15" />
    </>
  ),
  folder: <path d="M3.5 7A1.5 1.5 0 0 1 5 5.500h4.500l2 2.500H19a1.5 1.5 0 0 1 1.5 1.5v8A1.5 1.5 0 0 1 19 19H5a1.5 1.5 0 0 1-1.500-1.500Z" />,
  search: (
    <>
      <circle cx="11" cy="11" r="6.5" />
      <path d="m16 16 4 4" />
    </>
  ),
  star: (
    <path d="m12 3.800 2.500 5.200 5.700.800-4.100 4 1 5.700-5.100-2.700-5.100 2.700 1-5.700-4.100-4 5.700-.800Z" />
  ),
} as const;

export type IconName = keyof typeof PATHS;

interface Props {
  name: IconName;
  size?: number;
  /** Fill the shape, for a star that is on. */
  filled?: boolean;
}

export function Icon({ name, size = 16, filled = false }: Props) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill={filled ? "currentColor" : "none"}
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {PATHS[name]}
    </svg>
  );
}
