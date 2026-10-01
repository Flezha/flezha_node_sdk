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

export const RELEASE_ENV = 'FLEZHA_STEPS_E2E_RELEASE';

/**
 * Hold this activation until a runner boots with the release variable set.
 *
 * The harness kills the first runner and launches its replacement with the
 * variable present, so the gate is a boot-time decision: never settling on the
 * first runner, returning at once on the second. It SUSPENDS rather than holds
 * — every other activation on this event loop, the control pings among them,
 * has to keep running while a fixture waits here.
 */
export const blockUntilReleased = () =>
  process.env[RELEASE_ENV] ? Promise.resolve() : new Promise(() => {});
