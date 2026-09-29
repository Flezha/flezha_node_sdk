/**
 * The one error this SDK raises for anything Flezha refuses: a controller's
 * answer, the core's, or a refusal this SDK makes itself. A caller catches
 * `FlezhaError` and branches on `code`, never on the message.
 *
 * Every one is built from the JSON error document the core renders from the
 * catalogue, so a code reads the same here as in `flz` and in every other SDK.
 */

import { loadNative } from './native.js';

/** What kind of failure a code is, and so what a caller can do about it. */
export type ErrorClass =
  'internal' | 'invalid' | 'unreachable' | 'denied' | 'not_found' | 'conflict' | 'unavailable';

/** The five fields of the error model, as the JSON error document carries them. */
export interface ErrorFields {
  code: string;
  class: ErrorClass;
  message: string;
  details: Record<string, unknown>;
  hint: string | null;
}

export class FlezhaError extends Error {
  /** A stable `family.snake` code. */
  readonly code: string;
  readonly class: ErrorClass;
  /** The values the message names; a refusal of several issues lists them under `issues`. */
  readonly details: Record<string, unknown>;
  /** The next command to run, when there is one. */
  readonly hint: string | null;

  /** `message` is prefixed with the code, so the text an author reads names it. */
  constructor(fields: ErrorFields, options?: ErrorOptions) {
    super(`${fields.code}: ${fields.message}`, options);
    this.name = 'FlezhaError';
    this.code = fields.code;
    this.class = fields.class;
    this.details = fields.details;
    this.hint = fields.hint;
  }

  /** The error a JSON error document carries, or `null` when `text` is not one. */
  static fromDocument(text: string, options?: ErrorOptions): FlezhaError | null {
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      return null;
    }
    const fields = (parsed as { error?: Partial<ErrorFields> } | null)?.error;
    if (!fields || typeof fields.code !== 'string' || typeof fields.message !== 'string') {
      return null;
    }
    return new FlezhaError(
      {
        code: fields.code,
        class: fields.class as ErrorClass,
        message: fields.message,
        details: fields.details ?? {},
        hint: fields.hint ?? null,
      },
      options,
    );
  }
}

/**
 * The error `code` names, its message rendered by the core from `details`. A
 * code the catalogue does not know renders as `internal.uncatalogued`.
 */
export function flezhaError(
  code: string,
  details: Record<string, unknown> = {},
  options?: ErrorOptions,
): FlezhaError {
  const document = loadNative().errorDocument(code, JSON.stringify(details));
  return FlezhaError.fromDocument(document, options) as FlezhaError;
}

/**
 * Whether `value` is a `FlezhaError`, from either of this package's builds: a
 * module may have loaded the other one, where `instanceof` answers false.
 */
export function isFlezhaError(value: unknown): value is FlezhaError {
  return (
    value instanceof FlezhaError ||
    (value instanceof Error &&
      value.name === 'FlezhaError' &&
      typeof (value as { code?: unknown }).code === 'string')
  );
}

/**
 * A native failure as this SDK's error: the addon's message is the core's
 * JSON error document. Anything else, a JavaScript argument error the binding
 * raised before reaching the core, passes through unchanged.
 */
export function fromNative(error: unknown): unknown {
  if (!(error instanceof Error) || isFlezhaError(error)) return error;
  return FlezhaError.fromDocument(error.message, { cause: error }) ?? error;
}
