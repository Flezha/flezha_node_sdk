/**
 * The factory-function authoring surface and the fluent channel builder.
 *
 * Factory functions rather than decorators: decorators need a build step, do
 * not apply to plain functions, and cannot reshape the decorated type — which
 * is exactly what a typed graph needs. A generic function can, so
 * `.into(tasklet)` refuses a producer whose value the tasklet's declared
 * input does not accept, before the program runs.
 */

import { CHANNEL_CODECS, channelDecoder, channelEncoder, type Codec } from './codec.js';
import { channelsOf, defineTasklet, type EitherForm } from './definition.js';
import { flezhaError } from './errors.js';
import { latestRouter, type RouterInput } from './route-latest.js';
import {
  type BeyondWindowPolicy,
  type ChannelBuilder,
  type ChannelCodec,
  type CodecOptions,
  type LoopMode,
  type Merge,
  type OperatorDefinition,
  type OverflowPolicy,
  type RollbackMode,
  type Tasklet,
  type RoutedDocument,
  type ShortFormDefinition,
  type Stream,
  type BoundedDefinition,
} from './types.js';

/** A tasklet as the graph holds it, with its position in the declaration. */
export interface TaskNode {
  index: number;
  /** The node identity — the tasklet's name, or the name `named()` gave. */
  name: string;
  tasklet: Tasklet<unknown, unknown, unknown, unknown>;
  /** The one channel this node produces on, created on first wiring. */
  output?: ChannelSpec;
}

/** One consumer of a channel, with the transforms and key of its own edge. */
export interface ConsumerEdge {
  tasklet: number;
  /** Producer-side transforms for this edge, applied in declaration order. */
  transforms: Array<(value: unknown) => unknown>;
  /** Dynamic routing key for this edge. */
  keyBy?: (value: unknown) => string;
}

export type ChannelKind = 'entry' | 'inter_task' | 'plugin';

/**
 * One link of a chain, exactly as the core deserializes it.
 *
 * A chain is one fluent sequence declared off a channel handle, in the order
 * the author called it; the core reads the consumers, the channel kind, the
 * sink and the name off them. Closures never enter the record — they stay on
 * the edge held under the chain that declared them, and `validate` attaches
 * them to the consumers the core derived from that same chain.
 */
export type Link =
  | { link: 'transform' }
  | { link: 'key' }
  | { link: 'named'; name: string }
  | { link: 'sink'; name?: string }
  | { link: 'response' }
  | { link: 'into'; tasklet: number }
  | { link: 'merge'; tasklet: number; group: number }
  | { link: 'branch'; arms: number };

/** The links that end a channel on the plugin layer. */
type PlugEnd = Extract<Link, { link: 'sink' | 'response' }>;

/** The links that close a chain onto a consumer edge. */
type EdgeLink = Extract<Link, { link: 'into' | 'merge' }>;

/** A channel as the graph holds it. Its position in the list IS its id. */
export interface ChannelSpec {
  index: number;
  name: string;
  kind: ChannelKind;
  producer?: number;
  /** The authoring record: one entry per fluent sequence, in call order. */
  chains: Link[][];
  /**
   * The edge each chain declared, by chain index. Held aside rather than
   * wired: the core answers which chains produced a consumer and in what
   * order, so no builder decides which of two re-declarations survives.
   */
  chainEdges: Map<number, ConsumerEdge>;
  /** The consumer edges the core derived. Empty until `validate` runs. */
  consumers: ConsumerEdge[];
  overflow: OverflowPolicy;
  retryMax?: number;
  retryBackoffMs?: number;
  keyed: boolean;
  /**
   * The channel's name once a plugin end, a sink or a response, is attached;
   * absent means none. The core's answer names no sink for a response.
   */
  sink?: string;
  /**
   * An entry channel's payload decoder: its own, its codec's, or JSON. Absent
   * on a channel between tasklets, whose payloads are the class codec's.
   */
  decode?: (bytes: Uint8Array) => unknown;
  /**
   * The plugin sink's payload encoder: its own, its codec's, or the default
   * that sends bytes as they are, a string as UTF-8 and anything else as JSON.
   * It belongs to the sink frame alone: an inter-tasklet frame is decoded
   * with the class codec, so encoding one with this hands the consuming
   * tasklet bytes it cannot read.
   */
  sinkEncode?: (value: unknown) => Uint8Array;
}

export interface BoundedSpec {
  loopMode: LoopMode;
  rollbackMode: RollbackMode;
  maxDurationMs?: number;
}

/** Everything the graph knows about one declared class. */
export interface ClassSpec {
  factoryId: string;
  kind: 'unbounded' | 'bounded';
  persistent: boolean;
  sleepWhenIdle: boolean;
  effectivelyOnce: boolean;
  /** The author's explicit `checkpointAck()`; the core derives the granularity. */
  checkpointAck: boolean;
  checkpointEvery?: number;
  checkpointOutputs: 'as_produced' | 'at_checkpoint';
  checkpointMaxDelayMs?: number;
  correctionWindowMs?: number;
  beyondWindowPolicy: BeyondWindowPolicy;
  compactAfterBytes?: number;
  syncDurability: boolean;
  tasklets: TaskNode[];
  channels: ChannelSpec[];
  bounded?: BoundedSpec;
}

/** One tasklet for every router, so two unnamed routers are one node. */
const LatestRouter = defineTasklet(latestRouter);

/** One pending edge's declared options, accumulated by the fluent calls. */
interface PendingEdge {
  /** Overrides the derived channel name, never the consuming node's identity. */
  channelName?: string;
  transforms: Array<(value: unknown) => unknown>;
  keyBy?: (value: unknown) => string;
  overflow?: OverflowPolicy;
  retryMax?: number;
  retryBackoffMs?: number;
  keyed?: boolean;
  /** This handle's chain in its channel's record, absent until a link joins. */
  chain?: number;
  /** How much of that chain is this handle's: a later link forks past it. */
  chainLength?: number;
}

class Builder implements ChannelBuilder {
  readonly tasklets: TaskNode[] = [];
  readonly channels: ChannelSpec[] = [];
  effectivelyOnceFlag = false;
  checkpointAckFlag = false;
  checkpointEveryValue?: number;
  checkpointOutputsValue: 'as_produced' | 'at_checkpoint' = 'as_produced';
  checkpointMaxDelayMsValue?: number;
  correctionWindowMsValue?: number;
  beyondWindowPolicyValue: BeyondWindowPolicy = 'process_as_arrival';
  compactAfterBytesValue?: number;
  /** The next merge's group id, which ties its sources together across channels. */
  mergeGroups = 0;

  constructor(readonly factoryId: string) {}

  effectivelyOnce(): ChannelBuilder {
    this.effectivelyOnceFlag = true;
    return this;
  }

  // Each of the three writes its own raw flag and nothing else: any checkpoint
  // key implies the acknowledgement granularity, and the core is where that is
  // stated.
  checkpointAck(): ChannelBuilder {
    this.checkpointAckFlag = true;
    return this;
  }

  checkpointEvery(n: number): ChannelBuilder {
    this.checkpointEveryValue = n;
    return this;
  }

  outputsAtCheckpoint(maxDelayMs?: number): ChannelBuilder {
    this.checkpointOutputsValue = 'at_checkpoint';
    if (maxDelayMs !== undefined) this.checkpointMaxDelayMsValue = maxDelayMs;
    return this;
  }

  correctionWindow(ms: number): ChannelBuilder {
    this.correctionWindowMsValue = ms;
    return this;
  }

  beyondWindowPolicy(policy: BeyondWindowPolicy): ChannelBuilder {
    this.beyondWindowPolicyValue = policy;
    return this;
  }

  compactAfterBytes(bytes: number): ChannelBuilder {
    this.compactAfterBytesValue = bytes;
    return this;
  }

  input<T>(
    name: string,
    decodeOrOptions?: ((bytes: Uint8Array) => T) | CodecOptions,
    options?: CodecOptions,
  ): Stream<T> {
    const channel: ChannelSpec = {
      index: this.channels.length,
      name,
      kind: 'entry',
      chains: [],
      chainEdges: new Map(),
      consumers: [],
      overflow: 'drop_nothing',
      keyed: false,
    };
    const [decode, codec] = this.channelCodec<(bytes: Uint8Array) => unknown>(channel, [
      decodeOrOptions,
      options,
    ]);
    channel.decode = decode ?? channelDecoder(codec, channel);
    this.channels.push(channel);
    return new EdgeStream<T>(this, { source: 'entry', channel }, { transforms: [] });
  }

  merge<I>(): Merge<I> {
    return new MergeHandle<I>(this);
  }

  routeLatest<D, T>(
    documents: Stream<D>,
    traffic: Stream<T>,
    key: (value: T) => string,
  ): Stream<RoutedDocument<D>> {
    return this.routeLatestNamed(documents, traffic, key);
  }

  // The key replaces the traffic item on its own edge, so the router holds no
  // application value but the document.
  routeLatestNamed<D, T>(
    documents: Stream<D>,
    traffic: Stream<T>,
    key: (value: T) => string,
    nodeName?: string,
  ): Stream<RoutedDocument<D>> {
    return this.merge<RouterInput>()
      .from(documents, (document) => ({ document }))
      .from(traffic.map(key), (trafficKey) => ({ key: trafficKey }))
      .into(LatestRouter, nodeName) as Stream<RoutedDocument<D>>;
  }

  /** The handle a node's output is wired from. */
  outputOf<T>(node: TaskNode): Stream<T> {
    return new EdgeStream<T>(this, { source: 'tasklet', node }, { transforms: [] });
  }

  /**
   * A channel's own callable and the codec it names, read from the arguments
   * after its name in either order. The codec is refused when it is unknown or
   * named beside the callable, which only a JavaScript caller can write.
   */
  channelCodec<F extends (value: never) => unknown>(
    channel: ChannelSpec,
    args: unknown[],
  ): [F | undefined, ChannelCodec | undefined] {
    const callable = args.find((arg) => typeof arg === 'function') as F | undefined;
    const codec = (
      args.find((arg) => typeof arg === 'object' && arg !== null) as CodecOptions | undefined
    )?.codec;
    if (codec === undefined) return [callable, undefined];
    if (callable !== undefined) {
      throw flezhaError('topology.codec_with_callable', { channel: channel.name });
    }
    if (!CHANNEL_CODECS.includes(codec)) {
      throw flezhaError('topology.unknown_codec', {
        channel: channel.name,
        codec,
        codecs: [...CHANNEL_CODECS],
      });
    }
    return [undefined, codec];
  }

  /**
   * The node for a tasklet. Two unnamed mentions of one tasklet are one
   * node — the naming convention every SDK mirrors — so a diamond that fans
   * out and back in has one instance, not two.
   */
  nodeFor(tasklet: Tasklet<unknown, unknown, unknown, unknown>, name?: string): TaskNode {
    const identity = name ?? tasklet.name;
    const existing = this.tasklets.find(
      (node) => node.name === identity && node.tasklet === tasklet,
    );
    if (existing) return existing;
    const collision = this.tasklets.find((node) => node.name === identity);
    if (collision) {
      // The node table is keyed by identity and physically cannot hold two, so
      // the record the core would judge is unwritable. The refusal answers
      // under the core's own name all the same.
      throw flezhaError('topology.node_identity_collision', { name: identity });
    }
    const node: TaskNode = { index: this.tasklets.length, name: identity, tasklet };
    this.tasklets.push(node);
    return node;
  }

  /**
   * The channel this node produces on, created on first use so a leaf node
   * never carries a phantom channel. Named by the node's identity until a
   * `named(...)` override, and reused by every later call on a handle whose
   * source is this node — which is what makes a fan-out one channel with two
   * consumers rather than two channels claiming one producer.
   */
  outputChannelFor(node: TaskNode): ChannelSpec {
    if (node.output) return node.output;
    const channel: ChannelSpec = {
      index: this.channels.length,
      name: node.name,
      kind: 'inter_task',
      producer: node.index,
      chains: [],
      chainEdges: new Map(),
      consumers: [],
      overflow: 'drop_nothing',
      keyed: false,
    };
    this.channels.push(channel);
    node.output = channel;
    return channel;
  }
}

/** Where an unbounded operator's values come from: an entry channel, or a tasklet. */
type Source = { source: 'entry'; channel: ChannelSpec } | { source: 'tasklet'; node: TaskNode };

class EdgeStream<T> implements Stream<T> {
  constructor(
    private readonly builder: Builder,
    private readonly from: Source,
    private readonly pending: PendingEdge,
  ) {}

  /**
   * This handle with one more channel option, applied now as the reference
   * applies it: an option no terminator follows would otherwise be dropped.
   */
  private with(next: Partial<PendingEdge>): Stream<T> {
    const stream = new EdgeStream<T>(this.builder, this.from, { ...this.pending, ...next });
    stream.channel();
    return stream;
  }

  /**
   * Record one link on this handle's chain, forking when another handle has
   * already extended it — which is what lets a shared prefix branch without
   * the arms colliding. Answers the chain fields the next handle carries.
   */
  private record(channel: ChannelSpec, link: Link): { chain: number; chainLength: number } {
    const { chain, chainLength = 0 } = this.pending;
    if (chain === undefined) {
      channel.chains.push([link]);
      return { chain: channel.chains.length - 1, chainLength: 1 };
    }
    const links = channel.chains[chain] as Link[];
    if (links.length === chainLength) {
      links.push(link);
      return { chain, chainLength: links.length };
    }
    channel.chains.push([...links.slice(0, chainLength), link]);
    return { chain: channel.chains.length - 1, chainLength: chainLength + 1 };
  }

  /**
   * This handle with one more chain link — the one place a transform's or a
   * key's result handle is built, so every link the author declared reaches
   * the record whether or not an `.into(...)` ever closes it.
   */
  private relink<U>(next: Partial<PendingEdge>, link: Link): Stream<U> {
    const channel = this.channel();
    return new EdgeStream<U>(this.builder, this.from, {
      ...this.pending,
      ...next,
      ...this.record(channel, link),
    });
  }

  named(name: string): Stream<T> {
    return this.with({ channelName: name });
  }

  map<U>(transform: (value: T) => U): Stream<U> {
    return this.relink<U>(
      { transforms: [...this.pending.transforms, transform as (value: unknown) => unknown] },
      { link: 'transform' },
    );
  }

  filter(predicate: (value: T) => boolean): Stream<T> {
    return this.relink<T>(
      {
        transforms: [
          ...this.pending.transforms,
          (value: unknown) => (predicate(value as T) ? value : DROPPED),
        ],
      },
      { link: 'transform' },
    );
  }

  flatMap<U>(transform: (value: T) => Iterable<U>): Stream<U> {
    return this.relink<U>(
      {
        transforms: [
          ...this.pending.transforms,
          (value: unknown) => new Fanned([...transform(value as T)]),
        ],
      },
      { link: 'transform' },
    );
  }

  keyBy(key: (value: T) => string): Stream<T> {
    return this.relink<T>({ keyBy: key as (value: unknown) => string }, { link: 'key' });
  }

  onFull(policy: OverflowPolicy): Stream<T> {
    return this.with({ overflow: policy });
  }

  retryMax(attempts: number): Stream<T> {
    return this.with({ retryMax: attempts });
  }

  retryBackoffMs(ms: number): Stream<T> {
    return this.with({ retryBackoffMs: ms });
  }

  keyed(): Stream<T> {
    return this.with({ keyed: true });
  }

  branch(arms: Array<(edge: Stream<T>) => void>): Stream<T> {
    // The sugar's arity is recorded, never judged: each arm wires itself with
    // `.into(...)` off this same handle, so the arms are ordinary chains.
    this.channel().chains.push([{ link: 'branch', arms: arms.length }]);
    for (const arm of arms) arm(this);
    return this;
  }

  /**
   * The channel this handle's values leave on, with everything the fluent
   * calls put aside applied to it. An entry handle already has one; a
   * tasklet handle's is created on first use and reused after.
   */
  private channel(): ChannelSpec {
    const channel =
      this.from.source === 'entry'
        ? this.from.channel
        : this.builder.outputChannelFor(this.from.node);
    const rename = this.pending.channelName;
    if (rename !== undefined && channel.name !== rename) {
      // The override is recorded once and applied here too, so the name this
      // host addresses the channel by is the one the core derives from the
      // record — for every terminator, not for all but the last.
      channel.chains.push([{ link: 'named', name: rename }]);
      channel.name = rename;
    }
    applyPending(channel, this.pending);
    return channel;
  }

  into<TOut, TState, TChange>(
    tasklet: Tasklet<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      tasklet as unknown as Tasklet<unknown, unknown, unknown, unknown>,
      nodeName,
    );
    this.close(node, { link: 'into', tasklet: node.index });
    return this.builder.outputOf<TOut>(node);
  }

  /** Close this handle's chain on `node` with `link`: an `into`, or one merge source. */
  close(node: TaskNode, link: EdgeLink): void {
    const channel = this.channel();
    const { chain } = this.record(channel, link);
    // The transforms and the key belong to this edge, not to the channel: one
    // channel may feed several tasklets, and a chain declared before one
    // `into` must not reach another. The edge is held under the chain that
    // declared it and wired by `validate` from the core's own answer, so a
    // re-declaration is judged there rather than here.
    channel.chainEdges.set(chain, {
      tasklet: node.index,
      transforms: this.pending.transforms,
      keyBy: this.pending.keyBy,
    });
  }

  sink(
    nameOrEncode?: string | ((value: T) => Uint8Array) | CodecOptions,
    encodeOrOptions?: ((value: T) => Uint8Array) | CodecOptions,
    options?: CodecOptions,
  ): Stream<T> {
    // A string first names the channel; a function is the serializer and an
    // object names a codec, in whichever position follows. Telling them apart
    // here is what keeps the shorter forms from spelling `sink(undefined, x)`.
    const name = typeof nameOrEncode === 'string' ? nameOrEncode : undefined;
    const rest = name === undefined ? [nameOrEncode, encodeOrOptions] : [encodeOrOptions, options];
    return this.plugEnd(name === undefined ? { link: 'sink' } : { link: 'sink', name }, name, rest);
  }

  response(encodeOrOptions?: ((value: T) => Uint8Array) | CodecOptions): Stream<T> {
    return this.plugEnd({ link: 'response' }, undefined, [encodeOrOptions]);
  }

  /**
   * Attach a plugin end, a sink or a response: both leave as a plugin output
   * encoded the same way, and the core refuses a second end on a channel.
   */
  private plugEnd(end: PlugEnd, name: string | undefined, rest: unknown[]): Stream<T> {
    if (this.from.source === 'entry') {
      // Not a topology rule the core states: a record whose entry channel
      // carries a sink derives a plugin channel with no producer, and there is
      // nothing in it that says which end fed the plugin.
      throw flezhaError('topology.sink_on_entry', { channel: this.from.channel.name });
    }
    const channel = this.channel();
    // A chain of its own: the sink terminates the channel rather than this
    // handle, so `.into(...)` still chains after it and the value reaches both
    // the consumer mailboxes and the sink. Terminality is the absence of
    // further wiring, not a different method.
    channel.chains.push([end]);
    // The sink names the channel: `sink('x')` IS the name, and `sink()` leaves
    // it derived. One string, which is what the core answers back.
    if (name !== undefined) channel.name = name;
    const [serialize, codec] = this.builder.channelCodec<(value: unknown) => Uint8Array>(
      channel,
      rest,
    );
    if (channel.sink === undefined) {
      channel.sink = channel.name;
      channel.sinkEncode = serialize ?? channelEncoder(codec, channel);
    }
    return this;
  }
}

/**
 * A merge under construction. It records and never judges: each source is an
 * ordinary chain ending in its mapping, and the core reads the group, the
 * arity and any repeated source off the `merge` links that close them.
 */
class MergeHandle<I> implements Merge<I> {
  private readonly sources: Stream<I>[] = [];
  /** Every `keyBy` declared, so a second one reaches the core as a second link. */
  private readonly keys: Array<(value: I) => string> = [];

  constructor(private readonly builder: Builder) {}

  from<S>(source: Stream<S>, map: (value: S) => I): Merge<I> {
    // The mapping composes after the source's own transforms, so a key
    // declared on the source is a transform after a key, refused by the core.
    this.sources.push(source.map(map));
    return this;
  }

  keyBy(key: (value: I) => string): Merge<I> {
    this.keys.push(key);
    return this;
  }

  into<TOut, TState, TChange>(
    tasklet: Tasklet<I, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      tasklet as unknown as Tasklet<unknown, unknown, unknown, unknown>,
      nodeName,
    );
    const link: EdgeLink = {
      link: 'merge',
      tasklet: node.index,
      group: this.builder.mergeGroups++,
    };
    for (const source of this.sources) {
      const keyed = this.keys.reduce((edge, key) => edge.keyBy(key), source);
      (keyed as EdgeStream<I>).close(node, link);
    }
    return this.builder.outputOf<TOut>(node);
  }
}

function applyPending(channel: ChannelSpec, pending: PendingEdge): void {
  if (pending.overflow) channel.overflow = pending.overflow;
  if (pending.retryMax !== undefined) channel.retryMax = pending.retryMax;
  if (pending.retryBackoffMs !== undefined) channel.retryBackoffMs = pending.retryBackoffMs;
  if (pending.keyed) channel.keyed = true;
}

/**
 * Define an unbounded operator from its channels graph, or, as the §5 short form,
 * from one tasklet's fields and `run` body.
 */
export function defineUnbounded(definition: OperatorDefinition): ClassSpec;
export function defineUnbounded<TIn, TOut = never, TState = void, TChange = never>(
  definition: ShortFormDefinition<TIn, TOut, TState, TChange>,
): ClassSpec;
export function defineUnbounded(definition: EitherForm<OperatorDefinition>): ClassSpec {
  return classSpec(definition, 'unbounded');
}

/**
 * The values each bounded mode takes, keyed by its type so a value the type
 * gains is listed here or does not compile. A `.mjs` author has no type
 * checker, so a misspelt mode is refused where it is written.
 */
const BOUNDED_MODES: { [F in 'loopMode' | 'rollbackMode']: Record<BoundedSpec[F], true> } = {
  loopMode: { once: true, per_input: true, chained: true },
  rollbackMode: { all_or_nothing: true, keep_progress: true },
};

/**
 * Define a bounded operator from the tasklets its channels graph wires,
 * or, as the §5 short form, a bounded operator of one tasklet's fields and `run`
 * body, `once` unless it writes its `loopMode`.
 */
export function defineBounded(definition: BoundedDefinition): ClassSpec;
export function defineBounded<TIn, TOut = never, TState = void, TChange = never>(
  definition: ShortFormDefinition<TIn, TOut, TState, TChange> & Pick<BoundedDefinition, 'loopMode'>,
): ClassSpec;
export function defineBounded(definition: EitherForm<BoundedDefinition>): ClassSpec {
  const spec = classSpec(definition, 'bounded');
  const bounded: BoundedSpec = {
    loopMode: definition.loopMode ?? 'once',
    rollbackMode: definition.rollbackMode ?? 'all_or_nothing',
    maxDurationMs: definition.maxDurationMs,
  };
  for (const [field, modes] of Object.entries(BOUNDED_MODES)) {
    const value = bounded[field as keyof typeof BOUNDED_MODES];
    if (!Object.hasOwn(modes, value)) {
      throw flezhaError('topology.invalid_mode', {
        factory_id: spec.factoryId,
        knob: field,
        value: String(value),
      });
    }
  }
  return { ...spec, bounded };
}

/**
 * The class a definition of either kind declares: every field the builder
 * recorded and `syncDurability` as written, so a setting the kind does not
 * carry reaches the core and is refused there by name, never dropped here.
 */
function classSpec(
  definition: EitherForm<OperatorDefinition> & Pick<BoundedDefinition, 'syncDurability'>,
  kind: ClassSpec['kind'],
): ClassSpec {
  const builder = new Builder(definition.factoryId);
  channelsOf(definition, kind)(builder);
  const persistent = definition.persistent ?? true;
  return {
    factoryId: definition.factoryId,
    kind,
    persistent,
    sleepWhenIdle: definition.sleepWhenIdle ?? (kind === 'bounded' && persistent),
    effectivelyOnce: builder.effectivelyOnceFlag,
    checkpointAck: builder.checkpointAckFlag,
    checkpointEvery: builder.checkpointEveryValue,
    checkpointOutputs: builder.checkpointOutputsValue,
    checkpointMaxDelayMs: builder.checkpointMaxDelayMsValue,
    correctionWindowMs: builder.correctionWindowMsValue,
    beyondWindowPolicy: builder.beyondWindowPolicyValue,
    compactAfterBytes: builder.compactAfterBytesValue,
    syncDurability: definition.syncDurability ?? false,
    tasklets: builder.tasklets,
    channels: builder.channels,
  };
}

export type { Codec };

/**
 * What a `flatMap` yields: the values one input became. A marker rather than
 * the array itself, so a `map` that returns an array is not mistaken for a fan.
 */
class Fanned {
  constructor(readonly values: readonly unknown[]) {}
}

/**
 * What a `filter` yields for a value it rejects: a fan of nothing. The item is
 * consumed without reaching a handler, and at the prepare crossing it answers
 * no key, which is how the core learns not to route it.
 */
const DROPPED = new Fanned([]);

/**
 * Run an edge's transform chain over one value, answering the values that
 * survive it.
 *
 * One implementation for the three places a chain runs — an entry payload
 * before its handler, a payload at the prepare crossing, and an emit on its way
 * out — so a filter cannot mean one thing on one path and another elsewhere.
 * A list because that is the shape every transform shares: a drop is empty, a
 * mapped value is one, a fan is many, and each later link runs over them all.
 */
export function applyTransforms(
  transforms: Array<(value: unknown) => unknown>,
  value: unknown,
): unknown[] {
  let values = [value];
  for (const transform of transforms) {
    const next: unknown[] = [];
    for (const current of values) {
      const yielded = transform(current);
      if (yielded instanceof Fanned) next.push(...yielded.values);
      else next.push(yielded);
    }
    values = next;
  }
  return values;
}

/**
 * Whether this class's producers release their values untransformed.
 *
 * A bounded operator's stage releases ONE value per emit and the driver fans it out to
 * every consumer itself, so each consumer edge — its transforms and its key —
 * belongs to the consumer. The producer and the consumer read this one
 * predicate, so they cannot disagree about which side applies the edge.
 */
export function isStructuredBounded(spec: ClassSpec): boolean {
  return spec.kind === 'bounded';
}

/**
 * Whether each invocation's output rides one effect record the controller
 * stamps at flush: under the effectively-once tier, and for a durable
 * bounded operator, whose stage commit names the record.
 */
export function recordsEffects(spec: ClassSpec): boolean {
  return spec.effectivelyOnce || (spec.kind === 'bounded' && spec.persistent);
}
