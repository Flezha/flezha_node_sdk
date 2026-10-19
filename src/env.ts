/**
 * A knob read from the environment and parsed by the core's one rule, so a
 * Node runner answers `"0"`, `"on"` or a typo exactly as every other runtime
 * does. A knob's name, type and default are declared once in
 * `Flezha.Config.Keys`.
 */

import { loadNative } from './native.js';

/** A count from the environment, with `fallback` for anything that is not one. */
export function envPositive(name: string, fallback: number): number {
  return loadNative().knob('positive', process.env[name] ?? '') ?? fallback;
}
