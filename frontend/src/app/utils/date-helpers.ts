export function normalizeDateKey(rawValue: string): string | null {
  const trimmed = String(rawValue || '').trim();
  if (!trimmed) {
    return null;
  }

  const exactIsoDateMatch = trimmed.match(/^(\d{4}-\d{2}-\d{2})$/);
  if (exactIsoDateMatch) {
    return exactIsoDateMatch[1];
  }

  const embeddedIsoDateMatch = trimmed.match(/(\d{4}-\d{2}-\d{2})/);
  if (embeddedIsoDateMatch) {
    return embeddedIsoDateMatch[1];
  }

  const parsedDate = new Date(trimmed);
  if (!Number.isNaN(parsedDate.getTime())) {
    return parsedDate.toISOString().slice(0, 10);
  }

  return null;
}

export function dateKeyFromDate(baseDateKey: string, offsetDays: number): string {
  const normalizedBaseDateKey = normalizeDateKey(baseDateKey) ?? new Date().toISOString().slice(0, 10);
  const date = new Date(`${normalizedBaseDateKey}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + offsetDays);
  return date.toISOString().slice(0, 10);
}
