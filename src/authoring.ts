/**
 * The factory-function authoring surface and the fluent channel builder.
 *
 * Factory functions rather than decorators: decorators need a build step, do
 * not apply to plain functions, and cannot reshape the decorated type — which
 * is exactly what a typed graph needs. A generic function can, so
 * `.into(procedure)` refuses a producer whose value the procedure's declared
 * input does not accept, before the program runs.
 */

import {
  assertRepresentable,
  CHANNEL_CODECS,
  channelDecoder,
  channelEncoder,
  type Codec,
} from './codec.js';
import { latestRouter, type RouterInput } from './route-latest.js';
import {
  DeclarationError,
  type BeyondWindowPolicy,
  type ChannelBuilder,
  type ChannelCodec,
  type CodecOptions,
  type Merge,
  type OperativeDefinition,
  type OverflowPolicy,
  type Procedure,
  type ProcedureDefinition,
  type RoutedDocument,
  type Stream,
  type WorkflowDefinition,
} from './types.js';

/** A procedure as the graph holds it, with its position in the declaration. */
export interface ProcNode {
  index: number;
  /** The node identity — the procedure's name, or the name `named()` gave. */
  name: string;
  procedure: Procedure<unknown, unknown, unknown, unknown>;
  /** The one channel this node produces on, created on first wiring. */
  output?: ChannelSpec;
}

/** One consumer of a channel, with the transforms and key of its own edge. */
export interface ConsumerEdge {
  procedure: number;
  /** Producer-side transforms for this edge, applied in declaration order. */
  transforms: Array<(value: unknown) => unknown>;
  /** Dynamic routing key for this edge. */
  keyBy?: (value: unknown) => string;
}

export type ChannelKind = 'entry' | 'inter_proc' | 'plugin';

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
  | { link: 'into'; procedure: number }
  | { link: 'merge'; procedure: number; group: number }
  | { link: 'branch'; arms: number };

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
  /** The plugin sink attached to this channel; absent means none. */
  sink?: string;
  /**
   * An entry channel's payload decoder: its own, its codec's, or JSON. Absent
   * on a channel between procedures, whose payloads are the class codec's.
   */
  decode?: (bytes: Uint8Array) => unknown;
  /**
   * The plugin sink's payload encoder: its own, its codec's, or the default
   * that sends bytes as they are, a string as UTF-8 and anything else as JSON.
   * It belongs to the sink frame alone: an inter-procedure frame is decoded
   * with the class codec, so encoding one with this hands the consuming
   * procedure bytes it cannot read.
   */
  sinkEncode?: (value: unknown) => Uint8Array;
}

export interface WorkflowSpec {
  loopMode: 'once' | 'per_input' | 'chained';
  rollbackMode: 'all_or_nothing' | 'keep_progress';
  retryAttempts?: number;
  retryBackoffMs?: number;
  barrierRetryAttempts?: number;
  maxDurationMs?: number;
  wakeAfterMs?: number;
}

/** Everything the graph knows about one declared class. */
export interface ClassSpec {
  factoryId: string;
  kind: 'stream' | 'workflow';
  persistent: boolean;
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
  procedures: ProcNode[];
  channels: ChannelSpec[];
  workflow?: WorkflowSpec;
  run?: WorkflowDefinition['run'];
}

/**
 * Define a procedure.
 *
 * The declaration's own shape carries the inference: the handler's first
 * parameter is the input type, its return type the output. A handler that
 * emits through the context returns nothing, and names its output type through
 * the definition's type arguments.
 */
export function defineProcedure<TIn, TOut = never, TState = void, TChange = never>(
  definition: ProcedureDefinition<TIn, TOut, TState, TChange>,
): Procedure<TIn, TOut, TState, TChange> {
  refuseShortForm(definition);
  if (!definition.name || !definition.name.trim()) {
    throw new DeclarationError('<procedure>', null, 'a procedure declares a non-empty name');
  }
  if (typeof definition.handle !== 'function') {
    throw new DeclarationError('<procedure>', definition.name, 'a procedure declares a handler');
  }
  // State without a fold is legitimate: a value seeded at start and read but
  // never mutated has nothing to journal, and `ctx.change` already refuses at
  // the point of use. A fold with no state to fold into is not.
  if (!definition.initialState && definition.applyChange) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares applyChange but no initialState; there is nothing for a change to fold into',
    );
  }
  if (definition.entities && definition.dynamicEntities) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares both a static key set and dynamic entities; the two modes are exclusive',
    );
  }
  if (definition.entities && definition.entities.keys.length === 0) {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      'declares an empty static key set; a static procedure names its entities',
    );
  }
  // A state that cannot be encoded cannot be restored, and the first anyone
  // would learn of it is a restart. Walk the declared starting state now.
  if (definition.initialState && !definition.stateCodec) {
    assertRepresentable(`${definition.name}.state`, definition.initialState(undefined));
  }
  const procedure = {
    definition,
    name: definition.name,
  } as Procedure<TIn, TOut, TState, TChange>;
  if (definition.sink !== undefined) {
    // Built here, not at registration: a malformed short form is refused while
    // the module loads, as a `dup_node` graph already is.
    (procedure as { spec?: ClassSpec }).spec = shortFormSpec(definition, procedure);
  }
  return procedure;
}

/**
 * Every rule about what a `sink` needs and what needs a `sink`. Each refusal
 * carries the contract's name (`authoring_contract.md` §5.2) in `refusals` and
 * at the start of the message.
 *
 * `SinkWithoutOutput` is not among them: a `.mjs` author runs no type checker
 * and the SDK ships no build step, so a handler that returns nothing cannot be
 * seen here. The Node manifest does not list `shorthand_typed_output` and the
 * row skips by name.
 */
function refuseShortForm<TIn, TOut, TState, TChange>(
  definition: ProcedureDefinition<TIn, TOut, TState, TChange>,
): void {
  if (definition.sink === undefined) {
    const keys = (['kind', 'input', 'codec'] as const).filter(
      (key) => definition[key] !== undefined,
    );
    if (keys.length > 0) {
      throw new DeclarationError(
        '<procedure>',
        definition.name ?? null,
        `ShorthandKeyWithoutSink: \`${keys.join('`, `')}\` name(s) of the operative a \`sink\` ` +
          'generates; this procedure declares no `sink`, so there is nothing for them to describe',
        ['ShorthandKeyWithoutSink'],
      );
    }
    return;
  }
  if (!definition.name || !definition.name.trim()) {
    throw new DeclarationError(
      '<procedure>',
      null,
      'SinkWithoutId: a `sink` makes this procedure a whole operative, whose factory id is an ' +
        'address an operator types; write it as `name`',
      ['SinkWithoutId'],
    );
  }
  if (definition.kind !== 'stream' && definition.kind !== 'workflow') {
    throw new DeclarationError(
      '<procedure>',
      definition.name,
      "SinkWithoutKind: a `sink` makes this procedure a whole operative; write `kind: 'stream'` " +
        "or `kind: 'workflow'`",
      ['SinkWithoutKind'],
    );
  }
}

/** The operative a short form generates, on the same builder path a full form uses. */
function shortFormSpec<TIn, TOut, TState, TChange>(
  definition: ProcedureDefinition<TIn, TOut, TState, TChange>,
  procedure: Procedure<TIn, TOut, TState, TChange>,
): ClassSpec {
  const entry = definition.input ?? 'input';
  const sink = definition.sink as string;
  const channels = (c: ChannelBuilder): void => {
    const stream = definition.codec
      ? c.input(entry, { codec: definition.codec } as { codec: 'json' })
      : c.input(entry);
    stream.into(procedure as unknown as Procedure<unknown, unknown>).sink(sink);
  };
  return definition.kind === 'workflow'
    ? defineWorkflow({ factoryId: definition.name, channels })
    : defineOperative({ factoryId: definition.name, channels });
}

/** One procedure for every router, so two unnamed routers are one node. */
const LatestRouter = defineProcedure(latestRouter);

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
  readonly procedures: ProcNode[] = [];
  readonly channels: ChannelSpec[] = [];
  effectivelyOnceFlag = false;
  checkpointAckFlag = false;
  checkpointEveryValue?: number;
  checkpointOutputsValue: 'as_produced' | 'at_checkpoint' = 'as_produced';
  checkpointMaxDelayMsValue?: number;
  correctionWindowMsValue?: number;
  beyondWindowPolicyValue: BeyondWindowPolicy = 'process_as_arrival';
  compactAfterBytesValue?: number;
  syncDurabilityFlag = false;
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

  syncDurability(): ChannelBuilder {
    this.syncDurabilityFlag = true;
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
  outputOf<T>(node: ProcNode): Stream<T> {
    return new EdgeStream<T>(this, { source: 'procedure', node }, { transforms: [] });
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
      throw new DeclarationError(
        this.factoryId,
        channel.name,
        'names both a codec and its own callable; give one',
      );
    }
    if (!CHANNEL_CODECS.includes(codec)) {
      throw new DeclarationError(
        this.factoryId,
        channel.name,
        `unknown codec \`${codec}\`; a channel codec is one of ${CHANNEL_CODECS.join(', ')}`,
      );
    }
    return [undefined, codec];
  }

  /**
   * The node for a procedure. Two unnamed mentions of one procedure are one
   * node — the naming convention every SDK mirrors — so a diamond that fans
   * out and back in has one instance, not two.
   */
  nodeFor(procedure: Procedure<unknown, unknown, unknown, unknown>, name?: string): ProcNode {
    const identity = name ?? procedure.name;
    const existing = this.procedures.find(
      (node) => node.name === identity && node.procedure === procedure,
    );
    if (existing) return existing;
    const collision = this.procedures.find((node) => node.name === identity);
    if (collision) {
      // The node table is keyed by identity and physically cannot hold two, so
      // the record the core would judge is unwritable. The refusal answers
      // under the core's own name all the same.
      throw new DeclarationError(
        this.factoryId,
        identity,
        'two different procedures claim this node identity; name one of them with ' +
          '`named(...)` so each node has its own',
        ['NodeIdentityCollision'],
      );
    }
    const node: ProcNode = { index: this.procedures.length, name: identity, procedure };
    this.procedures.push(node);
    return node;
  }

  /**
   * The channel this node produces on, created on first use so a leaf node
   * never carries a phantom channel. Named by the node's identity until a
   * `named(...)` override, and reused by every later call on a handle whose
   * source is this node — which is what makes a fan-out one channel with two
   * consumers rather than two channels claiming one producer.
   */
  outputChannelFor(node: ProcNode): ChannelSpec {
    if (node.output) return node.output;
    const channel: ChannelSpec = {
      index: this.channels.length,
      name: node.name,
      kind: 'inter_proc',
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

/** Where a stream's values come from: an entry channel, or a procedure. */
type Source = { source: 'entry'; channel: ChannelSpec } | { source: 'procedure'; node: ProcNode };

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
   * procedure handle's is created on first use and reused after.
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
    procedure: Procedure<T, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      procedure as unknown as Procedure<unknown, unknown, unknown, unknown>,
      nodeName,
    );
    this.close(node, { link: 'into', procedure: node.index });
    return this.builder.outputOf<TOut>(node);
  }

  /** Close this handle's chain on `node` with `link`: an `into`, or one merge source. */
  close(node: ProcNode, link: EdgeLink): void {
    const channel = this.channel();
    const { chain } = this.record(channel, link);
    // The transforms and the key belong to this edge, not to the channel: one
    // channel may feed several procedures, and a chain declared before one
    // `into` must not reach another. The edge is held under the chain that
    // declared it and wired by `validate` from the core's own answer, so a
    // re-declaration is judged there rather than here.
    channel.chainEdges.set(chain, {
      procedure: node.index,
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
    if (this.from.source !== 'procedure') {
      // Not a topology rule the core states: a record whose entry channel
      // carries a sink derives a plugin channel with no producer, and there is
      // nothing in it that says which end fed the plugin.
      throw new DeclarationError(
        this.builder.factoryId,
        name ?? '<derived>',
        'a plugin sink is fed by a procedure; wire the entry channel into one first',
      );
    }
    const channel = this.channel();
    // A chain of its own: the sink terminates the channel rather than this
    // handle, so `.into(...)` still chains after it and the value reaches both
    // the consumer mailboxes and the sink. Terminality is the absence of
    // further wiring, not a different method.
    channel.chains.push(name === undefined ? [{ link: 'sink' }] : [{ link: 'sink', name }]);
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
    procedure: Procedure<I, TOut, TState, TChange>,
    nodeName?: string,
  ): Stream<TOut> {
    const node = this.builder.nodeFor(
      procedure as unknown as Procedure<unknown, unknown, unknown, unknown>,
      nodeName,
    );
    const link: EdgeLink = {
      link: 'merge',
      procedure: node.index,
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

function buildGraph(factoryId: string, channels: OperativeDefinition['channels']): Builder {
  const builder = new Builder(factoryId);
  channels(builder);
  return builder;
}

/** Define a stream operative from its channels graph. */
export function defineOperative(definition: OperativeDefinition): ClassSpec {
  const builder = buildGraph(definition.factoryId, definition.channels);
  return {
    factoryId: definition.factoryId,
    kind: 'stream',
    persistent: definition.persistent ?? true,
    effectivelyOnce: builder.effectivelyOnceFlag,
    checkpointAck: builder.checkpointAckFlag,
    checkpointEvery: builder.checkpointEveryValue,
    checkpointOutputs: builder.checkpointOutputsValue,
    checkpointMaxDelayMs: builder.checkpointMaxDelayMsValue,
    correctionWindowMs: builder.correctionWindowMsValue,
    beyondWindowPolicy: builder.beyondWindowPolicyValue,
    compactAfterBytes: builder.compactAfterBytesValue,
    syncDurability: builder.syncDurabilityFlag,
    procedures: builder.procedures,
    channels: builder.channels,
  };
}

/** Define a workflow operative — a run-sugar body, or a structured chain. */
export function defineWorkflow(definition: WorkflowDefinition): ClassSpec {
  if (definition.run && definition.channels) {
    throw new DeclarationError(
      definition.factoryId,
      null,
      'declares both a run body and a channels graph; a workflow is one or the other',
    );
  }
  if (!definition.run && !definition.channels) {
    throw new DeclarationError(
      definition.factoryId,
      null,
      'declares neither a run body nor a channels graph',
    );
  }
  const builder = definition.channels
    ? buildGraph(definition.factoryId, definition.channels)
    : new Builder(definition.factoryId);
  return {
    factoryId: definition.factoryId,
    kind: 'workflow',
    persistent: definition.persistent ?? true,
    effectivelyOnce: builder.effectivelyOnceFlag,
    checkpointAck: false,
    checkpointOutputs: 'as_produced',
    beyondWindowPolicy: 'process_as_arrival',
    syncDurability: definition.syncDurability ?? false,
    procedures: builder.procedures,
    channels: builder.channels,
    workflow: {
      loopMode: definition.loopMode ?? 'once',
      rollbackMode: definition.rollbackMode ?? 'all_or_nothing',
      retryAttempts: definition.retryAttempts,
      retryBackoffMs: definition.retryBackoffMs,
      barrierRetryAttempts: definition.barrierRetryAttempts,
      maxDurationMs: definition.maxDurationMs,
      wakeAfterMs: definition.wakeAfterMs,
    },
    run: definition.run,
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
 * A structured workflow's stage releases ONE value per emit and the driver
 * fans it out to every consumer itself, so each consumer edge — its transforms
 * and its key — belongs to the consumer. The producer and the consumer read
 * this one predicate, so they cannot disagree about which side applies the
 * edge.
 */
export function isStructuredWorkflow(spec: ClassSpec): boolean {
  return spec.kind === 'workflow' && spec.channels.length > 0;
}
