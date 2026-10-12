/** Display formatting shared by the cards and charts. */

const integer = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const compact = new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 });

export function formatInteger(value: number): string {
  return integer.format(value);
}

/** 950, 1.2K, 3.4M: for axis ticks and dense labels. */
export function formatCompact(value: number): string {
  return compact.format(value);
}

/** Decimal units, like the server's own sizes (a segment file is 1,000,000,000 bytes: "1 GB"). */
export function formatBytes(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1000 && unit < units.length - 1) {
    value /= 1000;
    unit++;
  }
  return `${value.toFixed(value >= 100 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

/** Microseconds, switching to milliseconds from 1,000 µs. */
export function formatMicroseconds(us: number): string {
  if (us >= 1000) return `${(us / 1000).toFixed(us >= 10_000 ? 0 : 1)} ms`;
  if (us >= 10) return `${us.toFixed(0)} µs`;
  return `${us.toFixed(us >= 1 ? 1 : 2)} µs`;
}

export function formatTime(time: number): string {
  return new Date(time).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
}
