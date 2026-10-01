// Helpers every fixture module shares.

/** The key convention every keyed fixture shares: text before the first ':'. */
export const prefix = (input) => String(input).split(':')[0];

/**
 * A kept instance that sleeps after `FLEZHA_FIXTURE_SLEEP_AFTER_MS`, so a
 * test can watch one sleep; the class default when the variable is unset.
 */
export function fixtureDurable() {
  const sleepAfter = Number.parseInt(process.env.FLEZHA_FIXTURE_SLEEP_AFTER_MS ?? '', 10);
  return sleepAfter > 0 ? { local: { sleepAfter } } : 'local';
}
