/**
 * The authoring surface's public types.
 *
 * Handler input and output types drive the graph: `.into(tasklet)` accepts a
 * tasklet whose declared input the producing edge's value satisfies, so a
 * mismatched edge is a compile error rather than a registration-time one.
 */

import type { CallOptions, CallTarget, Response } from './call.js';
import type { Codec } from './codec.js';
import type { SpawnHandle } from './ctx.js';

export type OverflowPolicy = 'drop_nothing' | 'drop_newest' | 'drop_oldest';
/** A payload format a channel names on `input` and `sink`. */
export type ChannelCodec = 'bytes' | 'text' | 'json' | 'msgpack';
/** The options `input` and `sink` take to name a channel's codec. */
export interface CodecOptions {
  codec: ChannelCodec;
}
export type TimerKind = 'event_time' | 'processing_time';
export type BeyondWindowPolicy = 'process_as_arrival' | 'drop';
/** How a steps run loops: one pass, one pass per input, or each pass seeding the next. */
export type Each = 'once' | 'per_input' | 'chained';
/** What a failed steps pass undoes. */
export type OnFailure = 'undo_all' | 'keep_progress';
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
  /**
   * Send a part: a preview of the output this invocation is producing, which
   * leaves now rather than when the handler returns. It previews the tasklet's
   * own output channel and is encoded as `emit(value)` would encode it there.
   * It never waits: a part that cannot leave at once is dropped, and a replayed
   * input sends none. A listener receives it, numbered from 0 within each
   * attempt; a sink never does, and nothing about it is durable. Throws a
   * `FlezhaError` with code `part.no_sink` when the output channel does not end
   * in a sink (a `response()` end is not one), and `part.outside_invocation`
   * through a context kept past its invocation.
   */
  part(value: TOut): void;

  /** Register a processing-time timer `ms` from `now()`. Returns its time. */
  timerAfter(ms: number | bigint): bigint;
  /** Register an event-time timer at an absolute time. Returns it. */
  timerAtEventTime(at: number | bigint): bigint;
  /** Cancel a registered timer by the value its registration returned. */
  cancelTimer(kind: TimerKind, at: bigint): void;

  /** Close the open checkpoint interval (checkpoint ack-granularity classes). */
  checkpoint(): void;

  /**
   * End the instance this tasklet runs in, with `result` as the
   * result its completion reports, encoded as a sink naming no codec encodes a
   * value: bytes as they are, a string as UTF-8 and anything else as JSON;
   * `undefined` or `null` carries no result. The rest of the iteration still
   * runs and commits; once that commit is durable the instance ends instead of
   * waiting for its next input; a flow instance first lets every Cell go
   * idle. In a `once` steps relay it sets the result the pass ends
   * with. A second call in one item replaces the first; calls from several
   * cells in one iteration end the instance once, with the result of the
   * tasklet latest in the chain and, within it, the cell whose key sorts
   * last.
   */
  complete(result?: unknown): void;

  /**
   * Ask the controller for a steps instance of `factoryId` with `payload` as
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
  spawnSteps(factoryId: string, payload: unknown, options?: SpawnOptions): SpawnHandle;
}

/** Static cell mode: a fixed key set, plus the fan-out an input belongs to. */
export interface StaticCells<TIn> {
  keys: string[];
  cellsFor?: (input: TIn) => string[];
}

export interface TaskletDefinition<TIn, TOut, TState, TChange> {
  /** The tasklet's node identity within its relay, and where its state is filed. */
  name: string;

  /**
   * Fixed key set. Without it, a tasklet reached by a `keyBy` edge gets a
   * cell per key, and any other has one cell.
   */
  cells?: StaticCells<TIn>;
  /** Seconds a keyed cell may sit idle before it is evicted. Unset never evicts. */
  idleTtlSeconds?: number;
  /** The most keyed cells held at once. */
  maxCells?: number;

  /** The event time of an input. Writing it puts the tasklet on the event-time clock. */
  eventTime?: (input: TIn) => bigint | number;
  /** Serve `ctx.now()` from the cell's change sequence. Needs `applyChange`. */
  logicalClock?: boolean;

  mailboxCapacity?: number;

  /** Stamped into every durable record this tasklet writes. */
  schemaVersion?: number;
  /** The oldest version this tasklet still reads. Defaults to its own. */
  minReadableSchemaVersion?: number;
  migrateState?: (fromVersion: number, bytes: Uint8Array) => TState;
  migrateChange?: (fromVersion: number, bytes: Uint8Array) => TChange;

  /** The cell's starting state. Its absence declares a stateless tasklet. */
  initState?: (cellKey?: string) => TState;
  /** The replay path: fold one recorded change into the state. */
  applyChange?: (state: TState, change: TChange) => void;

  stateCodec?: Codec<TState>;
  changeCodec?: Codec<TChange>;

  /**
   * The body. A value leaves only through `ctx.emit` or `ctx.emitTo`; what
   * `run` returns is ignored, and TypeScript refuses a `run` that returns one.
   */
  run: (input: TIn, ctx: Ctx<TOut, TState, TChange>) => void | Promise<void>;

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
  /**
   * Terminate this edge's channel on the answer to the outside call its input
   * carried: each value emitted on it answers that caller and reaches no
   * plugin. It takes no name, so the channel keeps the one it would have
   * without it, and returns the handle, so consumers may follow it exactly as
   * after `sink`. A channel ends in one sink or one response.
   *
   * It encodes as `sink` does: a serializer, a `{ codec }`, or neither.
   */
  response(encode?: (value: T) => Uint8Array): Stream<T>;
  response(options: CodecOptions): Stream<T>;
  /**
   * End this edge's chain in a call to another relay: each value that
   * reaches it, after this chain's transforms and key, is one request. The
   * call waits for nothing: its outcome arrives later as a `Response` on the
   * entry channel this answers, `<this channel>.response`, which must feed a
   * tasklet (the calling one included). A channel ends in one sink, one
   * response or one call; an entry channel ends in none.
   */
  call<A = unknown>(target: CallTarget, options: CallOptions): Stream<Response<T, A>>;
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
  /** `routeLatest` under a node name of its own, for a second router in one relay. */
  routeLatestNamed<D, T>(
    documents: Stream<D>,
    traffic: Stream<T>,
    key: (value: T) => string,
    nodeName: string,
  ): Stream<RoutedDocument<D>>;
}

/** When a flow acknowledges an input: one by one, or at the checkpoint. */
export type Ack =
  | 'per_input'
  | 'checkpoint'
  | {
      checkpoint: {
        /** Close an interval every `every` inputs; otherwise only `ctx.checkpoint()` closes one. */
        every?: number;
        /** The longest an output is held under `outputs: 'at_checkpoint'`. */
        maxDelayMs?: number;
        outputs?: 'as_produced' | 'at_checkpoint';
      };
    };

/** A flow's duplicate rule. */
export type Exactly =
  | 'at_least_once'
  | 'effectively_once'
  | {
      effectively_once: {
        ack?: Ack;
        /** Late-input correction window, in milliseconds. Event-time classes. */
        correctionMs?: number;
        beyondWindow?: BeyondWindowPolicy;
      };
    };

export interface FlowSettings {
  exactly?: Exactly;
}

export interface StepsSettings {
  each?: Each;
  onFailure?: OnFailure;
}

/** Milliseconds idle before an instance sleeps, or `false` to stay awake. */
export type SleepAfter = number | false;

/**
 * Where an instance keeps its work: nowhere (`false`), on this node's disk
 * (`local`, the default) or shipped to the durability backend (`remote`).
 * Unless `sleepAfter` says otherwise a kept flow instance never sleeps and a
 * kept steps instance sleeps after six idle hours.
 */
export type Durable =
  | false
  | 'local'
  | 'remote'
  | { local: { sleepAfter?: SleepAfter } }
  | { remote: { sleepAfter?: SleepAfter } };

/** The class settings every form takes but `run`, which `flow` or `steps` stands for. */
export interface ClassSettings {
  description?: string;
  /** Wall-clock stop: the instance is cancelled this long after it starts. */
  maxDurationMs?: number;
  durable?: Durable;
}

/**
 * How the class runs: as a flow (the default) or in steps, written as the
 * branch itself, since a short form's `run` is its tasklet body.
 */
export type RunSettings =
  { flow?: FlowSettings; steps?: never } | { steps: StepsSettings; flow?: never };

export type RelaySettings = ClassSettings & RunSettings;

export type RelayDefinition = RelaySettings & {
  factoryId: string;
  channels: (builder: ChannelBuilder) => void;
};

/**
 * The §5 short form, taken by `defineRelay`: one tasklet's fields and its
 * `run` body in place of a `channels` body, with the class settings the full
 * form takes. It generates an entry channel into a tasklet named `factoryId`,
 * and that tasklet's output into the `sink` plugin channel when one is written.
 */
export type ShortFormDefinition<TIn, TOut, TState, TChange> = Omit<
  TaskletDefinition<TIn, TOut, TState, TChange>,
  'name'
> &
  RelaySettings & {
    factoryId: string;
    /** The plugin sink the tasklet's output reaches. Absent declares none. */
    sink?: string;
    /** The generated entry channel's name. Defaults to `input`. */
    input?: string;
    /** The generated entry channel's codec, as `c.input(name, { codec })` takes it. */
    codec?: ChannelCodec;
  };
