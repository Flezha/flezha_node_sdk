/**
 * The authoring surface's public types.
 *
 * Handler input and output types drive the graph: `.into(tasklet)` accepts a
 * tasklet whose declared input the producing edge's value satisfies, so a
 * mismatched edge is a compile error rather than a registration-time one.
 */

import type { Codec } from './codec.js';
import type { SpawnHandle } from './ctx.js';

export type OverflowPolicy = 'drop_nothing' | 'drop_newest' | 'drop_oldest';
/** A payload format a channel names on `input` and `sink`. */
export type ChannelCodec = 'bytes' | 'text' | 'json' | 'msgpack';
/** The options `input` and `sink` take to name a channel's codec. */
export interface CodecOptions {
  codec: ChannelCodec;
}
export type ClockKind = 'wall' | 'logical' | 'event_time';
export type TimerKind = 'event_time' | 'processing_time';
export type BeyondWindowPolicy = 'process_as_arrival' | 'drop';
export type LoopMode = 'once' | 'per_input' | 'chained';
export type RollbackMode = 'all_or_nothing' | 'keep_progress';
export type IfRunning = 'deliver' | 'error';

/** How a spawn reached an instance. */
export type SpawnOutcome = 'spawned' | 'delivered' | 'skipped_on_replay';

/** A resolved spawn: the id it resolved to, given or generated, and how it got there. */
export interface SpawnResult {
  id: string;
  outcome: SpawnOutcome;
}

/**
 * The instance a spawn names. Without an `id` the controller generates one and
 * always starts a new instance, so `ifRunning` needs an `id`, and defaults to
 * `'deliver'` when one is given.
 */
export interface SpawnOptions {
  id?: string;
  ifRunning?: IfRunning;
}

/** A timer firing. `now` is the SCHEDULED time, never the wall clock. */
export interface TimerFiring {
  kind: TimerKind;
  scheduledAt: bigint;
}

export interface EmitOptions {
  /** Routing key for the destination. Defaults to this cell's own key. */
  key?: string;
  /** Which declared output channel, when the tasklet has more than one. */
  channel?: string;
  /** User metadata. Keys under `flz.` are reserved and refused. */
  metadata?: Record<string, string>;
}

/**
 * The invocation context. Every deterministic effect is served from here — the
 * ambient clock and ambient randomness are not replay-stable, and the SDK
 * warns when a handler reaches for them.
 */
export interface Ctx<TOut = never, TState = void, TChange = never> {
  /** The cell's state. Assigning to a field is refused; record a change. */
  readonly state: TState;
  /** This cell's key, or undefined for a single-cell tasklet. */
  readonly cellKey: string | undefined;
  /** True while the controller is re-forwarding a journaled slice. */
  readonly replay: boolean;

  /** The core-evaluated declared clock for this item. Never ambient time. */
  now(): bigint;
  /** A replay-stable identifier, distinct per invocation within the cell. */
  stableId(): string;

  /** Record one declared change: journaled, then folded into the state. */
  change(change: TChange): void;

  /** Emit one output value on the tasklet's declared output. */
  emit(value: TOut, options?: EmitOptions): void;
  /** Emit onto a named channel — the spelling for a tasklet with several. */
  emitTo(channel: string, value: unknown, options?: EmitOptions): void;

  /** Register a processing-time timer `ms` from `now()`. Returns its time. */
  timerAfter(ms: number | bigint): bigint;
  /** Register an event-time timer at an absolute time. Returns it. */
  timerAtEventTime(at: number | bigint): bigint;
  /** Cancel a registered timer by the value its registration returned. */
  cancelTimer(kind: TimerKind, at: bigint): void;

  /** Close the open checkpoint interval (checkpoint ack-granularity classes). */
  checkpoint(): void;

  /**
   * End the workflow instance this tasklet runs in, with `result` as the
   * result its completion reports, encoded as a sink naming no codec encodes a
   * value: bytes as they are, a string as UTF-8 and anything else as JSON;
   * `undefined` or `null` carries no result. The rest of the iteration still
   * runs and commits; once that commit is durable the instance ends instead of
   * waiting for its next input. In a `once` workflow it sets the result the pass ends
   * with. A second call in one item replaces the first; calls from several
   * cells in one iteration end the instance once, with the result of the
   * tasklet latest in the chain and, within it, the cell whose key sorts
   * last.
   *
   * A stream never completes: a stream tasklet calling this fails its input,
   * without a retry, with an error beginning `CompleteOutsideWorkflow`.
   */
  complete(result?: unknown): void;

  /**
   * Ask the controller for a workflow instance of `factoryId` with `payload` as
   * its first input, or as one more input of the running instance an `id` names
   * under `ifRunning: 'deliver'`. Returns the handle at once: a failure comes out
   * of its reads, never out of this call. `ifRunning` without an `id` throws a
   * `TypeError` and sends nothing. A replayed invocation sends nothing and its
   * handle answers `skipped_on_replay`. Read the handle inside this invocation
   * or not at all.
   *
   * A spawn without an id always starts a new instance and can never deliver
   * into a running one. Reissuing it after a lost or timed-out answer starts a
   * second instance, and a replay of it answers an empty id, because the
   * generated id exists only in the live run's answer. A caller who needs at
   * most one instance, or the id on replay, passes its own `id`.
   *
   * `payload` is an ordinary message on the instance's first entry channel,
   * encoded as a sink naming no codec sends a value: bytes as they are, a
   * string as UTF-8, anything else as JSON, which a `text` or `json` entry
   * reads.
   */
  spawnWorkflow(factoryId: string, payload: unknown, options?: SpawnOptions): SpawnHandle;
}

/** Static cell mode: a fixed key set, plus the fan-out an input belongs to. */
export interface StaticCells<TIn> {
  keys: string[];
  cellsFor?: (input: TIn) => string[];
}

/** Dynamic cell mode: cells appear on first key, with optional limits. */
export interface DynamicCells {
  idleTtlMs?: number;
  maxCells?: number;
}

export interface TaskletDefinition<TIn, TOut, TState, TChange> {
  /** The tasklet's node identity within its operative. */
  name: string;

  /** Fixed key set. Dynamic keying is declared on the wiring with `keyBy`. */
  cells?: StaticCells<TIn>;
  /** Lazy cells. The key comes from the wiring's `keyBy`. */
  dynamicCells?: DynamicCells;

  clock?: ClockKind;
  /** Required by an event-time clock: the event time of an input. */
  eventTime?: (input: TIn) => bigint | number;

  mailboxCapacity?: number;

  /** Stamped into every durable record this tasklet writes. */
  schemaVersion?: number;
  /** The oldest version this tasklet still reads. Defaults to its own. */
  minReadableSchemaVersion?: number;
  migrateState?: (fromVersion: number, bytes: Uint8Array) => TState;
  migrateChange?: (fromVersion: number, bytes: Uint8Array) => TChange;

  /** The cell's starting state. Its absence declares a stateless tasklet. */
  initialState?: (cellKey?: string) => TState;
  /** The replay path: fold one recorded change into the state. */
  applyChange?: (state: TState, change: TChange) => void;

  stateCodec?: Codec<TState>;
  changeCodec?: Codec<TChange>;

  /**
   * The body. A returned value is emitted; `undefined` emits nothing, which is
   * the warmup gate. A body that emits more than once per input uses
   * `ctx.emit` and returns nothing.
   */
  run: (
    input: TIn,
    ctx: Ctx<TOut, TState, TChange>,
  ) => TOut | undefined | void | Promise<TOut | undefined | void>;

  /** A per-cell deadline fired. `ctx.now()` is the scheduled time. */
  onTimer?: (firing: TimerFiring, ctx: Ctx<TOut, TState, TChange>) => void | Promise<void>;

  /** Saga compensation for this tasklet's recorded forward outputs. */
  compensate?: (outputs: TOut[], ctx: Ctx<TOut, TState, TChange>) => void | Promise<void>;
}

/** A defined tasklet. Opaque to authors beyond its identity and types. */
export interface Tasklet<TIn, TOut = never, TState = void, TChange = never> {
  readonly definition: TaskletDefinition<TIn, TOut, TState, TChange>;
  readonly name: string;
  /** Present only to carry the input and output types; never read. */
  readonly __types?: { input: TIn; output: TOut };
}

/** An edge carrying values of type `T` into whatever consumes it next. */
export interface Stream<T> {
  /**
   * Override the channel name this edge creates. The default is derived from
   * the producing node's identity.
   */
  named(name: string): Stream<T>;
  /** Transform every value on this edge. Evaluated producer-side. */
  map<U>(transform: (value: T) => U): Stream<U>;
  /**
   * Drop the values a predicate rejects, before this edge's key is computed.
   * A dropped value is consumed and reaches no handler.
   */
  filter(predicate: (value: T) => boolean): Stream<T>;
  /**
   * Replace every value on this edge with the values an iterable yields, each
   * reaching the consumer on its own. An empty iterable drops the value.
   */
  flatMap<U>(transform: (value: T) => Iterable<U>): Stream<U>;
  /** Route by a computed key: the consumer becomes a dynamic keyed tasklet. */
  keyBy(key: (value: T) => string): Stream<T>;
  onFull(policy: OverflowPolicy): Stream<T>;
  retryMax(attempts: number): Stream<T>;
  retryBackoffMs(ms: number): Stream<T>;
  /** Stamp `flz.cell` / `flz.seq` / `flz.key` onto this channel's plugin frames. */
  keyed(): Stream<T>;
  /**
   * Wire this edge into a tasklet whose declared input accepts `T`.
   *
   * Two unnamed mentions of one tasklet are one node, so a fan-out and
   * fan-back-in describes a diamond rather than two instances. `nodeName`
   * gives a second mention its own node.
   */
  into<TOut, TState, TChange>(
    tasklet: Tasklet<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut>;
  /**
   * Fan out: each arm receives this same edge, so `branch` builds exactly the
   * topology the separate `into` statements would. At least two arms.
   */
  branch(arms: Array<(edge: Stream<T>) => void>): Stream<T>;
  /**
   * Terminate this edge's channel on the plugin layer, and return the handle:
   * `.sink(...).into(...)` wires a consumer behind the sink, so one emit
   * reaches both. A channel with no consumer behind it is the plugin-bound
   * terminal — terminality is the absence of further wiring, not a different
   * method.
   *
   * A name given here IS the channel's name, which is the address a plugin
   * filter and a local-mode callback use; the no-argument form leaves it
   * derived. A function in the first position is the serializer.
   *
   * With neither a serializer nor a `{ codec }`, bytes are sent as they are, a
   * string as UTF-8 and any other value as JSON, so a sink needs neither for a
   * string, bytes or a JSON value.
   */
  sink(name: string, encode?: (value: T) => Uint8Array): Stream<T>;
  sink(name: string, options: CodecOptions): Stream<T>;
  sink(encode: (value: T) => Uint8Array): Stream<T>;
  sink(options: CodecOptions): Stream<T>;
  sink(): Stream<T>;
}

/**
 * Several streams feeding one tasklet, each lifted into its input `I` by a
 * mapping of its own. A source is an entry stream or a tasklet's output
 * declared on the same builder.
 */
export interface Merge<I> {
  /** Add a source: its values reach the tasklet as `map` answers them. */
  from<S>(source: Stream<S>, map: (value: S) => I): Merge<I>;
  /** Route every source by a key computed on the mapped value. */
  keyBy(key: (value: I) => string): Merge<I>;
  /** Close the merge on a tasklet, as `Stream.into` does, and answer its output. */
  into<TOut, TState, TChange>(
    tasklet: Tasklet<I, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut>;
}

/** What `routeLatest` hands on: the retained document, for the key that earned it. */
export interface RoutedDocument<D> {
  key: string;
  document: D;
}

export interface ChannelBuilder {
  /** Effectively-once delivery tier. */
  effectivelyOnce(): ChannelBuilder;
  /**
   * Acknowledge at the checkpoint rather than per input; the interval closes
   * only on an explicit `ctx.checkpoint()`. Requires `effectivelyOnce()`.
   */
  checkpointAck(): ChannelBuilder;
  /** Close a checkpoint interval every `n` inputs. Implies `checkpointAck()`. */
  checkpointEvery(n: number): ChannelBuilder;
  /**
   * Hold outputs until the interval closes, with an optional deadline.
   * Implies `checkpointAck()`.
   */
  outputsAtCheckpoint(maxDelayMs?: number): ChannelBuilder;
  /** Late-input correction window, in milliseconds. Event-time classes. */
  correctionWindow(ms: number): ChannelBuilder;
  beyondWindowPolicy(policy: BeyondWindowPolicy): ChannelBuilder;
  compactAfterBytes(bytes: number): ChannelBuilder;
  /**
   * Declare a controller-fed entry channel carrying values of type `T`.
   * `{ codec }` names the payload's format instead of a decoder; with neither,
   * the payload is read as JSON.
   */
  input(name: string, options: { codec: 'text' }): Stream<string>;
  input(name: string, options: { codec: 'bytes' }): Stream<Uint8Array>;
  input<T = unknown>(name: string, options: { codec: 'json' | 'msgpack' }): Stream<T>;
  input<T>(name: string, decode?: (bytes: Uint8Array) => T): Stream<T>;
  /** Start a merge of several streams into one tasklet. */
  merge<I = unknown>(): Merge<I>;
  /**
   * Hand the latest document to each key on that key's own next traffic item,
   * once per distinct document. A document equal to the retained one is inert,
   * and a key no traffic names receives nothing. The node is `route_latest`.
   */
  routeLatest<D, T>(
    documents: Stream<D>,
    traffic: Stream<T>,
    key: (value: T) => string,
  ): Stream<RoutedDocument<D>>;
  /** `routeLatest` under a node name of its own, for a second router in one operative. */
  routeLatestNamed<D, T>(
    documents: Stream<D>,
    traffic: Stream<T>,
    key: (value: T) => string,
    nodeName: string,
  ): Stream<RoutedDocument<D>>;
}

export interface OperativeDefinition {
  factoryId: string;
  /** Whether change bytes are flushed to the per-cell WAL. Default true. */
  persistent?: boolean;
  /**
   * Whether an idle instance is written out and dropped from memory until its
   * next input. Default `persistent` for a workflow, false for a stream.
   */
  sleepWhenIdle?: boolean;
  channels: (builder: ChannelBuilder) => void;
}

/**
 * The §5 short form, taken by `defineStream` and `defineWorkflow`: one
 * tasklet's fields and its `run` body in place of a `channels` body. It
 * generates an entry channel into a tasklet named `factoryId`, and that
 * tasklet's output into the `sink` plugin channel when one is written. Every
 * class-level setting stays at its default, but a workflow's `loopMode`, which
 * `defineWorkflow` also takes here; any other class-level setting is the full
 * form.
 */
export interface ShortFormDefinition<TIn, TOut, TState, TChange> extends Omit<
  TaskletDefinition<TIn, TOut, TState, TChange>,
  'name'
> {
  factoryId: string;
  /** The plugin sink the tasklet's output reaches. Absent declares none. */
  sink?: string;
  /** The generated entry channel's name. Defaults to `input`. */
  input?: string;
  /** The generated entry channel's codec, as `c.input(name, { codec })` takes it. */
  codec?: ChannelCodec;
}

/** The definition `defineStream` takes, and a workflow's own options. */
export interface WorkflowDefinition extends OperativeDefinition {
  loopMode?: LoopMode;
  rollbackMode?: RollbackMode;
  maxDurationMs?: number;
  wakeAfterMs?: number;
  /** Synchronous remote durability. */
  syncDurability?: boolean;
}

/**
 * An emit the declaration cannot carry: a reserved metadata key, a keyed
 * channel with no key, a value the channel's codec cannot send.
 */
export class EmitError extends Error {}

/**
 * Why a spawn reached no instance, thrown by a handle's reads and never by the
 * spawn call. `id` is the id the spawn named, empty when it named none.
 */
export class SpawnError extends Error {
  constructor(
    readonly id: string,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** The id is running and the spawn said `ifRunning: 'error'`. */
export class SpawnAlreadyRunningError extends SpawnError {}

/** No connected runner on any controller node registers the factory. */
export class SpawnFactoryUnknownError extends SpawnError {}

/** The factory is registered as a stream, and a spawn starts only a workflow. */
export class SpawnFactoryNotWorkflowError extends SpawnError {}

/** The controller refused the spawn: authorization, quota, shutdown or a failed start. */
export class SpawnRefusedError extends SpawnError {}

/** No answer arrived within the request's deadline. */
export class SpawnTimeoutError extends SpawnError {}

/** No control plane to request the spawn on. */
export class SpawnUnavailableError extends SpawnError {}

/** A registration refusal: names the offending handler and the defect. */
export class DeclarationError extends Error {
  constructor(
    readonly factoryId: string,
    readonly handler: string | null,
    readonly defect: string,
    /**
     * The core's vocabulary names for what was refused: the core's verdict, or
     * a refusal this SDK makes under one of them. Empty for a refusal of this
     * SDK's own surface.
     */
    readonly refusals: readonly string[] = [],
  ) {
    // A named refusal reads the way the core renders its verdict. The core's
    // own verdict arrives already rendered, header included.
    const header = `topology rejected [${refusals.join(', ')}]:`;
    const text =
      refusals.length === 0 || defect.startsWith(header) ? defect : `${header}\n  - ${defect}`;
    super(handler ? `${factoryId}: ${handler}: ${text}` : `${factoryId}: ${text}`);
    this.name = 'DeclarationError';
  }
}
