/**
 * The invocation context: every deterministic effect a handler is allowed.
 *
 * The context accumulates one item's effects — recorded changes, outputs,
 * timer operations, the checkpoint flag, a request to end the workflow — and
 * the dispatcher turns them into the `ItemEffect` the core folds. Nothing here
 * reaches the wire on its own; an emit is a value in a list until the
 * completion carries it.
 */

import { encodeValue, plainPayload } from './codec.js';
import { applyTransforms, isStructuredWorkflow } from './authoring.js';
import type { ChannelSpec, ClassSpec, ConsumerEdge, TaskNode } from './authoring.js';
import type {
  Native,
  NativeItemEffect,
  NativeMeta,
  NativeOutput,
  NativeTimerOp,
} from './native.js';
import { cellIdentity, stableId } from './identity.js';
import { readOnlyState } from './state.js';
import {
  EmitError,
  SpawnAlreadyRunningError,
  SpawnFactoryNotWorkflowError,
  SpawnFactoryUnknownError,
  SpawnRefusedError,
  SpawnTimeoutError,
  SpawnUnavailableError,
  type Ctx,
  type EmitOptions,
  type SpawnOptions,
  type SpawnOutcome,
  type SpawnResult,
  type TimerKind,
} from './types.js';

/** Metadata keys the runtime owns. A handler that sets one is refused. */
const RESERVED_PREFIX = 'flz.';

/** The addon entries a spawn reads. */
export type SpawnBridge = Pick<Native, 'spawnRequest' | 'spawnTryResult' | 'spawnWait'>;

/** The core ends a read at the request's own deadline, so a read this long always answers. */
const WAIT_MS = 0xffff_ffff;

const FAILURES = {
  already_running: SpawnAlreadyRunningError,
  factory_unknown: SpawnFactoryUnknownError,
  factory_not_workflow: SpawnFactoryNotWorkflowError,
  refused: SpawnRefusedError,
  timeout: SpawnTimeoutError,
  unavailable: SpawnUnavailableError,
};

/** What the core answers a resolved read with. */
type SpawnAnswer =
  | { id: string; outcome: SpawnOutcome }
  | { id: string; error: keyof typeof FAILURES; message: string };

/**
 * A spawn's answer, read inside the invocation that made it. Every read asks
 * the core, which answers a resolved handle the same way until that invocation
 * ends. Nothing is awaited until `wait()` is called, so a handle nobody reads
 * leaves no promise to reject unobserved: the core logs its failure once instead.
 */
export class SpawnHandle {
  readonly #bridge: SpawnBridge;
  readonly #handle: bigint;

  constructor(bridge: SpawnBridge, handle: bigint) {
    this.#bridge = bridge;
    this.#handle = handle;
  }

  /** The result once the answer has arrived, `null` while pending. Throws the failure. */
  tryResult(): SpawnResult | null {
    const json = this.#bridge.spawnTryResult(this.#handle);
    return json === null ? null : resultOf(json);
  }

  /** Resolves to the result, or rejects with the failure. The wait runs off the event loop. */
  async wait(): Promise<SpawnResult> {
    let json: string | null = null;
    while (json === null) {
      json = await this.#bridge.spawnWait(this.#handle, WAIT_MS);
    }
    return resultOf(json);
  }
}

/** The result a resolved answer carries, or its failure thrown. */
function resultOf(json: string): SpawnResult {
  const answer = JSON.parse(json) as SpawnAnswer;
  if ('outcome' in answer) return { id: answer.id, outcome: answer.outcome };
  throw new FAILURES[answer.error](answer.id, answer.message);
}

export interface InvocationInput {
  spec: ClassSpec;
  node: TaskNode;
  /** Channels this tasklet produces on, in declaration order. */
  outgoing: ChannelSpec[];
  operative: string;
  cellKey: string | undefined;
  changeSeq: bigint;
  now: bigint;
  replay: boolean;
  state: unknown;
  /** The activation this invocation runs under, which a spawn it makes belongs to. */
  token: bigint;
  bridge: SpawnBridge;
  /** Set for a `compensate` hook, which registers no timer. */
  compensating?: boolean;
}

function metaList(metadata: Record<string, string> | undefined, tasklet: string): NativeMeta[] {
  if (!metadata) return [];
  return Object.entries(metadata).map(([key, value]) => {
    if (key.startsWith(RESERVED_PREFIX)) {
      throw new EmitError(
        `${tasklet}: metadata key \`${key}\` is inside the reserved \`${RESERVED_PREFIX}\` ` +
          'namespace, which the runtime stamps itself (cell identity, sequence, key and ' +
          'offset); choose a key outside it',
      );
    }
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
        this.input.operative,
        this.input.node.name,
        this.input.cellKey ?? '',
      );
    }
    return stableId(this.identityBytes, this.input.changeSeq, this.ordinal++);
  }

  change(change: unknown): void {
    const definition = this.input.node.tasklet.definition;
    if (!definition.applyChange) {
      throw new EmitError(
        `${this.input.node.name}: recorded a change but declares no applyChange; a change ` +
          'that cannot be folded cannot rebuild the state it describes',
      );
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
   * Hand one input to the tasklet's `run` body. A returned value is emitted;
   * `undefined` emits nothing, which is the warmup gate. A body that emits
   * through the context returns nothing, so the two spellings compose rather
   * than conflict.
   */
  async handle(value: unknown): Promise<void> {
    const definition = this.input.node.tasklet.definition;
    const returned = await definition.run(value as never, this as never);
    if (returned !== undefined) this.emit(returned);
  }

  emit(value: unknown, options?: EmitOptions): void {
    const targets = options?.channel
      ? this.input.outgoing.filter((channel) => channel.name === options.channel)
      : this.input.outgoing;
    if (targets.length === 0) {
      throw new EmitError(
        options?.channel
          ? `${this.input.node.name}: emits on channel \`${options.channel}\`, which it does ` +
              'not produce on'
          : `${this.input.node.name}: emits but declares no output channel; wire its edge ` +
              'with .into(...) or .sink(...)',
      );
    }
    for (const channel of targets) {
      // A channel with no consumer releases nothing on the edge path: on a
      // workflow that is the plugin-bound terminal, and seeding the next stage
      // from it would hand the driver a terminal output.
      if (isStructuredWorkflow(this.input.spec)) {
        const edge = channel.consumers[0];
        if (edge) this.outputs.push(this.workflowOutput(channel, edge, value));
      } else {
        for (const edge of channel.consumers) {
          this.outputs.push(...this.edgeOutputs(channel, edge, value, options));
        }
      }
      if (channel.sink !== undefined) this.outputs.push(this.pluginOutput(channel, value, options));
    }
  }

  private workflowOutput(channel: ChannelSpec, edge: ConsumerEdge, value: unknown): NativeOutput {
    return {
      target: 'edge',
      channel: channel.index,
      consumer: edge.tasklet,
      pluginChannel: undefined,
      key: undefined,
      eventTime: undefined,
      metadata: [],
      payload: encodeValue(value),
    };
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
      metadata: metaList(options?.metadata, this.input.node.name),
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
  ): NativeOutput {
    const key = options?.key ?? this.input.cellKey;
    if (channel.keyed && key === undefined) {
      throw new EmitError(
        `${this.input.node.name}: emits on keyed channel \`${channel.name}\` with no key, ` +
          'and has no cell key of its own to fall back on; pass { key } on the emit',
      );
    }
    const metadata = metaList(options?.metadata, this.input.node.name);
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
   * Written only off the tier: under it the controller derives the identity
   * from the durable effect record, and a second answer here would contradict
   * it. The ordinal advances on every plugin output, stamped or not, so hosts
   * of one topology agree on the sequence.
   */
  private stampIdentity(
    metadata: NativeMeta[],
    channel: ChannelSpec,
    key: string | undefined,
  ): void {
    if (channel.keyed && !this.input.spec.effectivelyOnce) {
      const cell = `${this.input.operative}/${this.input.node.name}/${this.input.cellKey ?? ''}`;
      metadata.push(
        { key: 'flz.cell', value: cell },
        { key: 'flz.seq', value: `${this.input.changeSeq}.${this.emitOrdinal}` },
        { key: 'flz.key', value: key ?? '' },
      );
    }
    this.emitOrdinal += 1;
  }

  /** A rollback is no place to arm a deadline: a `compensate` hook refuses every timer call. */
  private refuseTimersInCompensate(call: string): void {
    if (this.input.compensating) {
      throw new Error(
        `TimersUnavailable: ${this.input.node.name}: ${call} inside compensate; only a ` +
          "tasklet's run / onTimer register timers",
      );
    }
  }

  timerAfter(ms: number | bigint): bigint {
    this.refuseTimersInCompensate('timerAfter');
    // Anchored on `now()`, never an ambient read: that is what makes a
    // replayed invocation register the timestamp the live one did.
    const at = this.input.now + BigInt(ms);
    this.timers.push({ kind: 'processing_time', scheduledAt: at, register: true });
    return at;
  }

  timerAtEventTime(at: number | bigint): bigint {
    this.refuseTimersInCompensate('timerAtEventTime');
    const scheduledAt = BigInt(at);
    this.timers.push({ kind: 'event_time', scheduledAt, register: true });
    return scheduledAt;
  }

  cancelTimer(kind: TimerKind, at: bigint): void {
    this.refuseTimersInCompensate('cancelTimer');
    this.timers.push({ kind, scheduledAt: at, register: false });
  }

  checkpoint(): void {
    this.checkpointed = true;
  }

  complete(result?: unknown): void {
    this.completion =
      result === undefined || result === null
        ? new Uint8Array()
        : plainPayload(result, this.input.node.name);
  }

  spawnWorkflow(factoryId: string, payload: unknown, options: SpawnOptions = {}): SpawnHandle {
    const { id, ifRunning } = options;
    if (ifRunning !== undefined && !id) {
      throw new TypeError(
        `${this.input.node.name}: spawnWorkflow was given ifRunning without an id; a spawn ` +
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
      plainPayload(payload, this.input.node.name),
      ifRunning ?? '',
      this.input.replay,
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
