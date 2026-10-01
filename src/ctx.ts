/**
 * The invocation context: every deterministic effect a handler is allowed.
 *
 * The context accumulates one item's effects — recorded changes, outputs,
 * timer operations, the checkpoint flag, a request to end the steps operator — and
 * the dispatcher turns them into the `ItemEffect` the core folds. Nothing here
 * reaches the wire on its own; an emit is a value in a list until the
 * completion carries it.
 */

import { encodeValue, plainPayload } from './codec.js';
import { applyTransforms, runsInSteps, recordsEffects } from './authoring.js';
import type { ChannelSpec, ClassSpec, ConsumerEdge, TaskNode } from './authoring.js';
import type {
  Native,
  NativeItemEffect,
  NativeMeta,
  NativeOutput,
  NativeTimerOp,
} from './native.js';
import { flezhaError } from './errors.js';
import { cellIdentity, stableId } from './identity.js';
import { readOnlyState } from './state.js';
import type {
  Ctx,
  EmitOptions,
  SpawnOptions,
  SpawnOutcome,
  SpawnResult,
  TimerKind,
} from './types.js';

/** Metadata keys the runtime owns. A handler that sets one is refused. */
const RESERVED_PREFIX = 'flz.';

/** The addon entries a spawn and a part reach. */
export type CoreBridge = Pick<Native, 'spawnRequest' | 'spawnTryResult' | 'spawnWait' | 'part'>;

/** The item index of an invocation that runs no work item; the core refuses its part. */
export const NO_ITEM = 0xffff_ffff;

/** The core ends a read at the request's own deadline, so a read this long always answers. */
const WAIT_MS = 0xffff_ffff;

/** What the core answers a resolved read with; a failed one is thrown as a `FlezhaError`. */
type SpawnAnswer = { id: string; outcome: SpawnOutcome };

/**
 * A spawn's answer, read inside the invocation that made it. Every read asks
 * the core, which answers a resolved handle the same way until that invocation
 * ends. Nothing is awaited until `wait()` is called, so a handle nobody reads
 * leaves no promise to reject unobserved: the core logs its failure once instead.
 */
export class SpawnHandle {
  readonly #bridge: CoreBridge;
  readonly #handle: bigint;

  constructor(bridge: CoreBridge, handle: bigint) {
    this.#bridge = bridge;
    this.#handle = handle;
  }

  /** The result once the answer has arrived, `null` while pending. Throws the failure as a `FlezhaError`. */
  tryResult(): SpawnResult | null {
    const json = this.#bridge.spawnTryResult(this.#handle);
    return json === null ? null : resultOf(json);
  }

  /** Resolves to the result, or rejects with the failure as a `FlezhaError`. The wait runs off the event loop. */
  async wait(): Promise<SpawnResult> {
    let json: string | null = null;
    while (json === null) {
      json = await this.#bridge.spawnWait(this.#handle, WAIT_MS);
    }
    return resultOf(json);
  }
}

/** The result a resolved answer carries. */
function resultOf(json: string): SpawnResult {
  const { id, outcome } = JSON.parse(json) as SpawnAnswer;
  return { id, outcome };
}

export interface InvocationInput {
  spec: ClassSpec;
  node: TaskNode;
  /** Channels this tasklet produces on, in declaration order. */
  outgoing: ChannelSpec[];
  operator: string;
  cellKey: string | undefined;
  changeSeq: bigint;
  now: bigint;
  replay: boolean;
  state: unknown;
  /** The activation this invocation runs under, which a spawn it makes belongs to. */
  token: bigint;
  /** The index of this invocation's work item in its activation, which a part names. */
  item: number;
  /**
   * The input-log offset of the item this invocation runs, which a spawn it makes carries so a
   * crash re-run of the same input resolves to the worker the first run spawned.
   */
  offset?: bigint;
  bridge: CoreBridge;
  /** Set for a `compensate` hook, which registers no timer. */
  compensating?: boolean;
}

function metaList(metadata: Record<string, string> | undefined): NativeMeta[] {
  if (!metadata) return [];
  return Object.entries(metadata).map(([key, value]) => {
    // The runtime stamps cell identity, sequence, key and offset there itself.
    if (key.startsWith(RESERVED_PREFIX))
      throw flezhaError('runtime.reserved_metadata_key', { key });
    return { key, value };
  });
}

/**
 * One invocation's context. Created per work item, discarded with it: the
 * per-invocation ordinal `stableId` counts is exactly this object's lifetime.
 */
export class Invocation implements Ctx<unknown, unknown, unknown> {
  readonly changes: Uint8Array[] = [];
  readonly outputs: NativeOutput[] = [];
  readonly timers: NativeTimerOp[] = [];
  checkpointed = false;

  private ordinal = 0;
  private emitOrdinal = 0;
  private identityBytes: Uint8Array | null = null;
  private live: unknown;
  private completion: Uint8Array | undefined;

  constructor(private readonly input: InvocationInput) {
    this.live = input.state;
  }

  /** The state after this item's changes folded — what the cell keeps. */
  get currentState(): unknown {
    return this.live;
  }

  get state(): unknown {
    return readOnlyState(this.input.node.name, this.live);
  }

  get cellKey(): string | undefined {
    return this.input.cellKey;
  }

  get replay(): boolean {
    return this.input.replay;
  }

  now(): bigint {
    return this.input.now;
  }

  stableId(): string {
    if (!this.identityBytes) {
      // Lazily, because most invocations never ask and the identity is the
      // one part of the derivation that costs an allocation.
      this.identityBytes = cellIdentity(
        this.input.operator,
        this.input.node.name,
        this.input.cellKey ?? '',
      );
    }
    return stableId(this.identityBytes, this.input.changeSeq, this.ordinal++);
  }

  change(change: unknown): void {
    const definition = this.input.node.tasklet.definition;
    if (!definition.applyChange) {
      throw flezhaError('runtime.change_without_apply_change', { tasklet: this.input.node.name });
    }
    const codec = definition.changeCodec;
    // The rule every host->core bytes site here follows: msgpack's own
    // output goes over as it stands, and a user encoder's is copied into a
    // `Buffer`, because what it returned is only typed as bytes.
    this.changes.push(codec ? Buffer.from(codec.encode(change)) : encodeValue(change));
    // Fold it live through the same function the restore fold uses, so the
    // live path and the replay path cannot drift.
    definition.applyChange(this.live, change);
  }

  /**
   * Hand one input to the tasklet's `run` body. Its outputs are what it passed
   * to `emit`; what it returns is ignored.
   */
  async handle(value: unknown): Promise<void> {
    await this.input.node.tasklet.definition.run(value as never, this as never);
  }

  emit(value: unknown, options?: EmitOptions): void {
    const targets = options?.channel
      ? this.input.outgoing.filter((channel) => channel.name === options.channel)
      : this.input.outgoing;
    if (targets.length === 0) {
      throw options?.channel
        ? flezhaError('runtime.channel_not_produced', {
            tasklet: this.input.node.name,
            channel: options.channel,
          })
        : flezhaError('runtime.no_output_channel', { tasklet: this.input.node.name });
    }
    for (const channel of targets) {
      // In steps a value rides the edge path to its first consumer, and an
      // inter-tasklet terminal's value to the core as the pass's handoff (a
      // chained loop's carry); a plugin-bound terminal releases nothing there.
      if (runsInSteps(this.input.spec)) {
        const consumer = channel.consumers[0]?.tasklet ?? this.handoffProducer(channel);
        if (consumer !== undefined) this.outputs.push(this.stepsOutput(channel, consumer, value));
      } else {
        for (const edge of channel.consumers) {
          this.outputs.push(...this.edgeOutputs(channel, edge, value, options));
        }
      }
      if (channel.callEnd !== undefined) {
        this.outputs.push(...this.callOutputs(channel, value, options));
      } else if (channel.sink !== undefined) {
        this.outputs.push(
          this.pluginOutput(channel, value, options, options?.key ?? this.input.cellKey),
        );
      }
    }
  }

  /**
   * A call end's requests: the value through the call chain's transforms,
   * each keyed by the chain alone. The core stamps them as calls at settle.
   */
  private callOutputs(
    channel: ChannelSpec,
    value: unknown,
    options: EmitOptions | undefined,
  ): NativeOutput[] {
    const end = channel.callEnd!;
    return applyTransforms(end.transforms, value).map((request) =>
      this.pluginOutput(channel, request, options, end.keyBy?.(request)),
    );
  }

  /** The producer of an inter-tasklet channel nothing consumes; the core reads no consumer there. */
  private handoffProducer(channel: ChannelSpec): number | undefined {
    return channel.kind === 'inter_task' && channel.sink === undefined ? channel.producer : undefined;
  }

  private stepsOutput(channel: ChannelSpec, consumer: number, value: unknown): NativeOutput {
    return {
      target: 'edge',
      channel: channel.index,
      consumer,
      pluginChannel: undefined,
      key: undefined,
      eventTime: undefined,
      metadata: [],
      payload: encodeValue(value),
    };
  }

  part(value: unknown): void {
    // The core previews the tasklet's first output channel and refuses one that ends in no
    // sink, so a channel with no plugin end sends no bytes.
    const channel = this.input.outgoing[0];
    const payload =
      channel?.sink !== undefined ? Buffer.from(channel.sinkEncode!(value)) : new Uint8Array();
    this.input.bridge.part(this.input.token, this.input.item, payload);
  }

  emitTo(channel: string, value: unknown, options?: EmitOptions): void {
    this.emit(value, { ...options, channel });
  }

  /**
   * One consumer's frames, one per value its transforms leave: its own key,
   * its own clock. A dropped value leaves none.
   */
  private edgeOutputs(
    channel: ChannelSpec,
    edge: ConsumerEdge,
    value: unknown,
    options: EmitOptions | undefined,
  ): NativeOutput[] {
    const extractor = this.input.spec.tasklets[edge.tasklet]?.tasklet.definition.eventTime;

    // Producer-side, in declaration order: the core routes on `(edge, key)`
    // and never evaluates a host closure.
    return applyTransforms(edge.transforms, value).map((transformed): NativeOutput => ({
      target: 'edge',
      channel: channel.index,
      consumer: edge.tasklet,
      pluginChannel: undefined,
      key: options?.key ?? edge.keyBy?.(transformed) ?? this.input.cellKey,
      eventTime: extractor ? BigInt(extractor(transformed)) : undefined,
      metadata: metaList(options?.metadata),
      payload: encodeValue(transformed),
    }));
  }

  /**
   * The plugin frame, serialized once from the value as emitted: an edge's
   * transforms belong to that edge, so they never reach the sink.
   */
  private pluginOutput(
    channel: ChannelSpec,
    value: unknown,
    options: EmitOptions | undefined,
    key: string | undefined,
  ): NativeOutput {
    if (channel.keyed && key === undefined) {
      throw flezhaError('runtime.key_required_on_keyed_channel', { channel: channel.name });
    }
    const metadata = metaList(options?.metadata);
    this.stampIdentity(metadata, channel, key);
    return {
      target: 'plugin',
      channel: undefined,
      consumer: undefined,
      pluginChannel: channel.name,
      key,
      eventTime: undefined,
      metadata,
      // A plugin output leaves only on a sink, and every `sink` sets its encoder.
      payload: Buffer.from(channel.sinkEncode!(value)),
    };
  }

  /**
   * Output identity for one plugin frame, appended after the user's metadata.
   * Written only when the class records no effects: under the tier, and for a
   * durable steps operator, the controller derives the identity from the durable
   * effect record, and a second answer here would contradict it. The ordinal
   * advances on every plugin output, stamped or not, so hosts of one topology
   * agree on the sequence.
   */
  private stampIdentity(
    metadata: NativeMeta[],
    channel: ChannelSpec,
    key: string | undefined,
  ): void {
    if (channel.keyed && !recordsEffects(this.input.spec)) {
      const cell = `${this.input.operator}/${this.input.node.name}/${this.input.cellKey ?? ''}`;
      metadata.push(
        { key: 'flz.cell', value: cell },
        { key: 'flz.seq', value: `${this.input.changeSeq}.${this.emitOrdinal}` },
        { key: 'flz.key', value: key ?? '' },
      );
    }
    this.emitOrdinal += 1;
  }

  /** A rollback is no place to arm a deadline: a `compensate` hook refuses every timer call. */
  private refuseTimersInCompensate(): void {
    if (this.input.compensating) throw flezhaError('runtime.timers_unavailable');
  }

  timerAfter(ms: number | bigint): bigint {
    this.refuseTimersInCompensate();
    // Anchored on `now()`, never an ambient read: that is what makes a
    // replayed invocation register the timestamp the live one did.
    const at = this.input.now + BigInt(ms);
    this.timers.push({ kind: 'processing_time', scheduledAt: at, register: true });
    return at;
  }

  timerAtEventTime(at: number | bigint): bigint {
    this.refuseTimersInCompensate();
    const scheduledAt = BigInt(at);
    this.timers.push({ kind: 'event_time', scheduledAt, register: true });
    return scheduledAt;
  }

  cancelTimer(kind: TimerKind, at: bigint): void {
    this.refuseTimersInCompensate();
    this.timers.push({ kind, scheduledAt: at, register: false });
  }

  checkpoint(): void {
    this.checkpointed = true;
  }

  complete(result?: unknown): void {
    this.completion =
      result === undefined || result === null
        ? new Uint8Array()
        : plainPayload(result, `tasklet '${this.input.node.name}'`);
  }

  spawnSteps(factoryId: string, payload: unknown, options: SpawnOptions = {}): SpawnHandle {
    const { id, ifRunning } = options;
    if (ifRunning !== undefined && !id) {
      throw new TypeError(
        `${this.input.node.name}: spawnSteps was given ifRunning without an id; a spawn ` +
          'without an id always starts a new instance, so pass an id or leave ifRunning out',
      );
    }
    // The replay flag goes to the core, which sends nothing for a replayed
    // invocation: a spawn is an external side effect, where a timer is
    // internal state and is registered again. The payload is the spawned
    // instance's first message on its entry channel, encoded as a sink
    // naming no codec sends a value, so a `text` or `json` entry reads it.
    const handle = this.input.bridge.spawnRequest(
      this.input.token,
      factoryId,
      id ?? '',
      plainPayload(payload, `tasklet '${this.input.node.name}'`),
      ifRunning ?? '',
      this.input.replay,
      this.input.offset,
    );
    return new SpawnHandle(this.input.bridge, handle);
  }

  /** The effect record for this item, as the core folds it. */
  effect(consumedOffset: bigint | undefined): NativeItemEffect {
    return {
      verdict: 'ok',
      changes: this.changes,
      outputs: this.outputs,
      consumedOffset,
      eventTime: this.input.now,
      timers: this.timers,
      checkpoint: this.checkpointed,
      complete: this.completion,
    };
  }
}
