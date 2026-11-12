const grouped = new Intl.NumberFormat("en-US");

/** A count with thousands separators: `395,330`. */
export function count(value: number): string {
  return grouped.format(value);
}

/** A share from 0 to 1 as a percentage with one decimal: `12.5%`. */
export function percent(share: number): string {
  return `${(share * 100).toFixed(1)}%`;
}
