/**
 * Declared state: how it is built, how it is restored, and how it is guarded.
 *
 * ## The mutation guard
 *
 * Declared state is mutated only through declared changes, because the change
 * log IS how the state is rebuilt: a field assigned directly is live in this
 * process and gone after a restart, and nothing would report the difference
 * until the restart. So a handler holds a view that refuses assignment, naming
 * the field and the change-based path, while `applyChange` and the restore
 * fold hold the value itself.
 *
 * The guard is deep: an assignment two objects down is the same silent loss as
 * one at the top.
 */

import { decodeValue } from './codec.js';
import { flezhaError } from './errors.js';
import type { TaskletDefinition } from './types.js';

const RAW = Symbol('flezha.state.raw');

/**
 * Declared state is rebuilt from its recorded changes on restart, so a direct
 * assignment is lost there and reported nowhere: it is refused where it is made.
 */
function mutated(tasklet: string, at: string, property: string | symbol): Error {
  return flezhaError('runtime.state_mutated', {
    tasklet,
    path: at ? `${at}.${String(property)}` : String(property),
  });
}

function guard<T extends object>(tasklet: string, target: T, path: string): T {
  const cache = new WeakMap<object, unknown>();
  const wrap = (value: unknown, at: string): unknown => {
    if (value === null || typeof value !== 'object') return value;
    if (value instanceof Date || ArrayBuffer.isView(value)) return value;
    const already = cache.get(value);
    if (already) return already;
    const proxied = new Proxy(value as object, {
      get(inner, property, receiver) {
        if (property === RAW) return inner;
        const next = Reflect.get(inner, property, receiver);
        if (typeof next === 'function') return next.bind(inner);
        return wrap(next, at ? `${at}.${String(property)}` : String(property));
      },
      set(_inner, property) {
        throw mutated(tasklet, at, property);
      },
      defineProperty(_inner, property) {
        throw mutated(tasklet, at, property);
      },
      deleteProperty(_inner, property) {
        throw mutated(tasklet, at, property);
      },
    });
    cache.set(value, proxied);
    return proxied;
  };
  return wrap(target, path) as T;
}

/**
 * The read-only view a handler receives. A non-object state (a number, a
 * string) is returned as-is: there is nothing to assign to.
 */
export function readOnlyState<S>(tasklet: string, state: S): S {
  if (state === null || typeof state !== 'object') return state;
  return guard(tasklet, state as unknown as object, '') as unknown as S;
}

/** The value behind a view, for `applyChange` and the restore fold. */
export function rawState<S>(state: S): S {
  if (state === null || typeof state !== 'object') return state;
  return ((state as unknown as Record<symbol, unknown>)[RAW] as S) ?? state;
}

/** A durable payload as the core hands one across for a restore. */
export interface RestoreBody {
  bytes: Uint8Array;
  /** Absent when the body was written at the running schema version. */
  fromVersion?: number;
}

type AnyDefinition = TaskletDefinition<never, never, unknown, unknown>;

/**
 * Decode one durable body, choosing between the three things it could be:
 * a payload from an older schema version, one the tasklet declared its own
 * codec for, or the MessagePack default.
 *
 * One function rather than the same three-way choice written at each of the
 * four places a body is decoded — the two halves of a restore fold, on the
 * dispatch path and in the test harness.
 */
export function decodeBody(
  definition: AnyDefinition,
  which: 'state' | 'change',
  body: RestoreBody,
  tasklet: string,
): unknown {
  if (body.fromVersion !== undefined) {
    const migration = which === 'state' ? definition.migrateState : definition.migrateChange;
    if (!migration) {
      throw new Error(
        `${tasklet} was handed a ${which} record written at schema version ` +
          `${body.fromVersion} and declares no migrate${which === 'state' ? 'State' : 'Change'}; ` +
          'declare one, or raise minReadableSchemaVersion so the record is refused rather ' +
          'than misread',
      );
    }
    return migration(body.fromVersion, body.bytes);
  }
  const codec = which === 'state' ? definition.stateCodec : definition.changeCodec;
  return codec ? codec.decode(body.bytes) : decodeValue(body.bytes);
}

/**
 * Rebuild a cell's state: the declared starting value, the snapshot
 * baseline when there is one, then every change folded on top in order.
 *
 * This IS the restore, and both callers are the real thing — the dispatcher
 * answering a restore fold, and the test harness constructing from a payload,
 * which is why the harness proves what a warm restart does rather than a
 * second implementation that agrees with it.
 */
export function foldRestore(
  definition: AnyDefinition,
  payload: { snapshot?: RestoreBody; changes?: RestoreBody[] },
  cellKey: string | undefined,
  tasklet: string,
): unknown {
  let state = definition.initState?.(cellKey);
  if (payload.snapshot) {
    state = decodeBody(definition, 'state', payload.snapshot, tasklet);
  }
  for (const body of payload.changes ?? []) {
    if (!definition.applyChange) {
      throw new Error(
        `${tasklet} was handed a change log but declares no applyChange; a tasklet that ` +
          'records no changes should have no durable records to fold',
      );
    }
    definition.applyChange(
      state as never,
      decodeBody(definition, 'change', body, tasklet) as never,
    );
  }
  return state;
}
