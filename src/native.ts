/**
 * Loading the native addon, and the typed shape of what it exports.
 *
 * The addon is `flz_core_ffi` built with its `napi` feature — the same binary
 * the Java SDK loads under its `jni` feature, with a second veneer over the
 * same C entry points. Nothing here re-expresses dispatch, ordering, overflow,
 * the retry ladder, durability or the steps driver; every function below is
 * one call into the core.
 */

import {
  NATIVE_PLATFORMS,
  SDK_PACKAGE_NAME,
  nativePlatformFor,
  platformPackageName,
} from './platforms.js';
import { fromNative } from './errors.js';
import { requireNative, runtimeDir } from './runtime-shim.js';

/** One metadata pair, in declared order. The schema allows a repeated key. */
export interface NativeMeta {
  key: string;
  value: string;
}

export interface NativeWorkItem {
  kind: 'input' | 'timer';
  channel?: number;
  payload?: Buffer;
  metadata?: NativeMeta[];
  consumedOffset?: bigint;
  replay?: boolean;
  /** The core-evaluated declared clock for this item; what `ctx.now()` serves. */
  now?: bigint;
  timerKind?: 'event_time' | 'processing_time';
  scheduledAt?: bigint;
}

export interface NativeRestoreBody {
  bytes: Buffer;
  /** Absent when the body was written at the running schema version. */
  fromVersion?: number;
}

export interface NativeKeyRequest {
  channel: number;
  payloads: Buffer[];
  wantKeys: boolean;
  wantEventTime: boolean;
}

export type NativeActivationKind =
  | 'work'
  | 'restore_fold'
  | 'snapshot_request'
  | 'live_state_read'
  | 'apply_state'
  | 'migration_serialize'
  | 'key_request'
  | 'compensate_run';

export interface NativeActivation {
  token: bigint;
  relay: string;
  tasklet: number;
  cellKey?: string;
  changeSeq: bigint;
  factory: string;
  kind: NativeActivationKind;
  items?: NativeWorkItem[];
  snapshot?: NativeRestoreBody;
  changes?: NativeRestoreBody[];
  stateJson?: Buffer;
  keyRequest?: NativeKeyRequest;
  outputs?: Buffer[];
}

/**
 * One output, in both directions. The payload a host writes is whatever its
 * encoder produced — the glue's reader takes any typed array, a `Buffer`
 * included — so nothing is copied into a `Buffer` first. The payload the core
 * hands back is always a `Buffer`, which `local.ts` is the only reader of.
 */
export interface NativeOutput {
  target: 'edge' | 'plugin';
  channel?: number;
  consumer?: number;
  pluginChannel?: string;
  key?: string;
  eventTime?: bigint;
  metadata?: NativeMeta[];
  payload: Uint8Array;
}

/** A `NativeOutput` the core itself built: its payload is always a `Buffer`. */
export type CoreOutput = NativeOutput & { payload: Buffer };

export interface NativeTimerOp {
  kind: 'event_time' | 'processing_time';
  scheduledAt: bigint;
  register: boolean;
}

export interface NativeDlqEntry {
  channel: string;
  error: string;
  attempts: number;
  payload?: Buffer;
}

export interface NativeItemEffect {
  /**
   * `unreadable` is answered only for an entry channel's decode or its edge
   * transforms: the core parks the payload rather than counting it as a
   * tasklet failure.
   */
  verdict: 'ok' | 'retryable' | 'non_retryable' | 'unreadable';
  message?: string;
  changes?: Uint8Array[];
  outputs?: NativeOutput[];
  consumedOffset?: bigint;
  eventTime: bigint;
  timers?: NativeTimerOp[];
  checkpoint?: boolean;
  dlq?: NativeDlqEntry;
  /** The encoded result of `ctx.complete`, absent when it was not called. */
  complete?: Uint8Array;
}

export interface NativePrepareAnswer {
  keys?: string[];
  eventTime?: bigint;
  /** Present when this payload's prepare failed; the retry ladder takes it. */
  error?: string;
  nonRetryable?: boolean;
}

export type NativeOutcome =
  | 'work'
  | 'restored'
  | 'snapshot'
  | 'state_json'
  | 'state_applied'
  | 'migration_state'
  | 'prepared'
  | 'failed';

export interface NativeActivationResult {
  token: bigint;
  outcome: NativeOutcome;
  effects?: NativeItemEffect[];
  state?: Uint8Array;
  json?: Uint8Array;
  prepared?: NativePrepareAnswer[];
  error?: string;
  nonRetryable?: boolean;
}

export interface NativeExport {
  name: string;
  /** The C ABI entry point behind this export; absent for host bookkeeping. */
  abi?: string;
  blocking: boolean;
}

/** One diagnostic from a native thread. */
export interface NativeLogLine {
  level: number;
  message: string;
}

export interface NativeHostStats {
  fetchers: number;
  fetcherCeiling: number;
  dispatchLoops: number;
  poolSize: number;
  poolBusy: number;
  poolPeak: number;
  poolQueued: number;
  handedOff: number;
  abandoned: number;
}

/**
 * The addon's exports. Every blocking entry point is a promise whose work runs
 * on a thread the addon owns — never on this event loop, and never on libuv's
 * shared pool. `poll` and `waitStarvation` are absent by design.
 */
export interface Native {
  schemaVersion(): number;
  contextNew(shimSchemaVersion: number): void;
  contextFree(): void;
  declareClass(json: Buffer): void;
  /** Judge a declaration without registering it; answers the derived one. */
  checkClass(json: Buffer): Buffer;
  start(namespace: string): Promise<void>;
  /** The alternative to `start`: the declared graph runs here, no controller. */
  localStart(factoryId: string, relayId: string): void;
  localOnSink(channel: string): void;
  localFeed(channel: string, payload: Uint8Array): Promise<void>;
  /** The core built this one, so its payload is a `Buffer` and says so. */
  localNextOutput(timeoutMs: number): Promise<CoreOutput | null>;
  localRunUntilIdle(timeoutMs: number): Promise<void>;
  localStop(): void;
  complete(result: NativeActivationResult): Promise<void>;
  abandon(token: bigint): void;
  shutdown(): void;
  /**
   * Request a spawn from the live activation `token`; answers its handle. Never waits.
   * `offset` is the input-log offset of the item the request runs under, if it has one.
   */
  spawnRequest(
    token: bigint,
    factoryId: string,
    id: string,
    payload: Uint8Array,
    ifRunning: string,
    replay: boolean,
    offset: bigint | undefined,
  ): bigint;
  /** The handle's answer as JSON once it has arrived, `null` before. Never waits. */
  spawnTryResult(handle: bigint): string | null;
  /** Send a part of work item `item` of the live activation `token`. Never waits. */
  part(token: bigint, item: number, payload: Uint8Array): void;
  spawnWait(handle: bigint, timeoutMs: number): Promise<string | null>;
  adminCall(verb: string, args: string): Promise<string>;
  /**
   * Send one message, through the controller or locally, answering as JSON:
   * the verdict, or `null` for a local send.
   */
  sendMessage(
    relay: string,
    channel: string,
    message: Uint8Array,
    msgId: string | null,
    key: string | null,
    local: boolean,
  ): Promise<string>;
  /** The ids of the relay instances bound on this runner. */
  relays(): string[];
  monotonicNanos(): bigint;
  lastError(): string | null;
  /** The JSON error document of `code`, its message rendered from `detailsJson`. */
  errorDocument(code: string, detailsJson: string): string;
  setLogCallback(callback: ((line: NativeLogLine) => void) | null): void;
  setHostDrainedCallback(callback: (() => void) | null): void;
  watchStdinEof(graceMs: number): void;
  registerDispatch(callback: (activation: NativeActivation) => void): number;
  unregisterDispatch(id: number): void;
  dispatchLoopCount(): number;
  startHost(fetchers?: number): void;
  /** Raise the fetchers' own flag, so they leave without the core's word. */
  stopFetching(): void;
  stopHost(): Promise<void>;
  hostStats(): NativeHostStats;
  nativeSurface(): NativeExport[];
}

/**
 * The per-platform package holding this host's binary. One is installed, not
 * five: the others are optional dependencies npm skips.
 */
export function nativePackageName(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): string | null {
  const entry = nativePlatformFor(platform, arch);
  return entry ? platformPackageName(entry.suffix) : null;
}

/**
 * Where the addon is looked for, in order: an explicitly configured binary
 * first, so this repository's own suites run against a freshly built core;
 * then the installed per-platform package; then a locally built binary beside
 * the package. The published package carries no binary of its own, so that
 * last candidate resolves in a checkout and nowhere else.
 */
export function nativeCandidates(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
  configured: string | undefined = process.env.FLEZHA_NATIVE_BINARY,
): string[] {
  const pkg = nativePackageName(platform, arch);
  return [
    ...(configured ? [configured] : []),
    ...(pkg ? [pkg] : []),
    `${runtimeDir}/../../native/flz_core_ffi.node`,
  ];
}

/**
 * What to do when none of the candidates resolved: name the host, the package
 * that was looked for and how to obtain a binary. npm treats an optional
 * dependency it cannot resolve as a non-event, so an installation missing its
 * platform package is indistinguishable from a healthy one until this throws.
 */
export function nativeResolutionError(
  attempts: string[],
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): Error {
  const pkg = nativePackageName(platform, arch);
  const published = NATIVE_PLATFORMS.map((entry) => `${entry.os}-${entry.cpu}`).join(', ');
  return new Error(
    `${SDK_PACKAGE_NAME} could not load its native binary for ${platform}-${arch}.\n` +
      (pkg
        ? `Expected the optional dependency ${pkg} to be installed; npm skips one ` +
          'it cannot resolve without failing the install.\n'
        : `No prebuilt binary is published for this platform. Published: ${published}.\n`) +
      'The published package carries no binary of its own, so a source build means ' +
      'a stable Rust toolchain and the wrapper crate this package ships:\n' +
      '  cargo build --manifest-path native/Cargo.toml --release\n' +
      '  node scripts/install-native.mjs\n' +
      'Set FLEZHA_NATIVE_BINARY to an already-built addon to skip resolution.\n' +
      `Tried:\n  ${attempts.join('\n  ')}`,
  );
}

let loaded: Native | null = null;

/** Load the addon, or throw naming this platform and what a source build needs. */
export function loadNative(): Native {
  if (loaded) return loaded;
  const attempts: string[] = [];
  for (const candidate of nativeCandidates()) {
    try {
      loaded = raisingFlezhaErrors(requireNative(candidate) as Native);
      return loaded;
    } catch (cause) {
      // First line only: a `require` failure carries a multi-line resolution
      // stack, and five of those are the loader noise this refusal replaces.
      attempts.push(`${candidate}: ${(cause as Error).message.split('\n', 1)[0]}`);
    }
  }
  throw nativeResolutionError(attempts);
}

/**
 * The addon with every failure it throws or rejects with turned into a
 * `FlezhaError`: each export's message is the core's JSON error document.
 */
function raisingFlezhaErrors(addon: Native): Native {
  const wrapped: Record<string, unknown> = {};
  for (const name of Object.getOwnPropertyNames(addon)) {
    const value = (addon as unknown as Record<string, unknown>)[name];
    wrapped[name] =
      typeof value !== 'function'
        ? value
        : (...args: unknown[]) => {
            let answer: unknown;
            try {
              answer = (value as (...a: unknown[]) => unknown)(...args);
            } catch (error) {
              throw fromNative(error);
            }
            return answer instanceof Promise
              ? answer.catch((error: unknown) => {
                  throw fromNative(error);
                })
              : answer;
          };
  }
  return wrapped as unknown as Native;
}

/** Test seam: forget the loaded addon so the next call resolves again. */
export function resetNativeForTests(): void {
  loaded = null;
}
