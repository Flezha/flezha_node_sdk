/**
 * The tasklet test harness.
 *
 * A defined handler is an ordinary function: a plain unit test can call it
 * with no runner started and no controller running. What the harness adds is
 * the parts a runtime would otherwise own — the cell's state, the recorded
 * change log, the emits, the clock a timer fires on — and the replay-equality
 * gate, which is the conformance gate for every tasklet.
 *
 * It drives the same `Invocation` the dispatcher does, so what a test exercises
 * is the production path rather than a second one written to agree with it.
 */

import { isDeepStrictEqual } from 'node:util';

import { defineRelay, type ChannelSpec, type ClassSpec, type TaskNode } from './authoring.js';
import { decodeValue } from './codec.js';
import { Invocation, type CoreBridge, type InvocationInput } from './ctx.js';
import { invocation } from './determinism.js';
import { flezhaError } from './errors.js';
import { foldRestore, rawState, type RestoreBody } from './state.js';
import type { NativeEffectStart } from './native.js';
import type { Tasklet, TimerKind, TimerFiring } from './types.js';

/** The recorded change log did not rebuild the state the handler produced. */
export class ReplayMismatch extends Error {
  constructor(
    readonly tasklet: string,
    readonly live: unknown,
    readonly replayed: unknown,
  ) {
    super(
      `${tasklet}: folding the recorded change log against a fresh initial state did not ` +
        `reproduce the handler's state. The engine rebuilds a cell exactly this way on a ` +
        `warm restart, so the difference is what a restart would lose.\n` +
        `  live:     ${JSON.stringify(live, replacer)}\n` +
        `  replayed: ${JSON.stringify(replayed, replacer)}`,
    );
    this.name = 'ReplayMismatch';
  }
}

/**
 * A replayed input made an outside call its live run kept through `ctx.effect`
 * some other way: its function ran again, or the kept result was never asked for.
 */
export class EffectReplayMismatch extends Error {
  constructor(
    readonly tasklet: string,
    readonly effect: string,
    what: string,
  ) {
    super(
      `${tasklet}: on the replay, effect '${effect}' ${what}. A re-run reads an outside ` +
        `call's result only through ctx.effect, with the same name in the same order.`,
    );
    this.name = 'EffectReplayMismatch';
  }
}

function replacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? `${value}n` : value;
}

/** The longest effect name, in bytes, as the core refuses it. */
const MAX_EFFECT_NAME = 255;

/**
 * One input's effect results, by name in call order, and the invocation's count
 * of each name and the call whose function is running.
 */
interface Effects {
  kept: Map<string, Uint8Array[]>;
  replaying: boolean;
  ordinals: Map<string, number>;
  running?: { name: string; ordinal: number };
}

const liveEffects = (): Effects => ({ kept: new Map(), replaying: false, ordinals: new Map() });

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';

/**
 * A harness has no controller, so a spawn answers at once as a new instance:
 * under the id it gave, or under one shaped like a generated id, counted
 * rather than drawn so a test reads the same ids every run.
 */
function resolvedSpawns(): Pick<
  CoreBridge,
  'spawnRequest' | 'spawnTryResult' | 'spawnWait' | 'part'
> {
  const answers: string[] = [];
  const read = (handle: bigint): string | null => answers[Number(handle) - 1] ?? null;
  return {
    spawnRequest: (_token, factoryId, id) => {
      const suffix = (answers.length + 1)
        .toString(32)
        .padStart(16, '0')
        .replace(/./g, (digit) => BASE32[parseInt(digit, 32)] as string);
      const answer = { id: id || `${factoryId}-${suffix}`, outcome: 'spawned' };
      return BigInt(answers.push(JSON.stringify(answer)));
    },
    spawnTryResult: read,
    spawnWait: async (handle) => read(handle),
    // No controller, so no listener: a part is encoded and dropped.
    part: () => {},
  };
}

export interface HarnessOptions<TState> {
  cellKey?: string;
  /** Override the declared starting state. */
  state?: TState;
  /** The value `ctx.now()` serves until an input or an advance moves it. */
  now?: bigint;
  relay?: string;
}

/** A restore payload, as the core would hand one across. */
export interface RestorePayload {
  snapshot?: Uint8Array;
  changes?: Uint8Array[];
  /** Stamp every body as written at this version, so a migration is exercised. */
  fromVersion?: number;
}

export interface FeedOptions {
  /** Replay the next live input, in order: its `ctx.effect` calls are served what it kept. */
  replay?: boolean;
}

/** One invocation's recorded effects. */
export interface Outcome<TOut, TChange> {
  emits: TOut[];
  changes: TChange[];
  timers: Array<{ kind: TimerKind; scheduledAt: bigint; register: boolean }>;
  checkpoint: boolean;
}

/**
 * A `ctx.effect` keeps its result on a live `feed`; the n-th
 * `feed(value, { replay: true })` replays the n-th live input and is served
 * what it kept, and throws `EffectReplayMismatch` when a function runs there
 * or a kept result is never asked for.
 */
export class TaskletTestHarness<TIn, TOut, TState, TChange> {
  private cellState: TState;
  private time: bigint;
  private readonly log: TChange[] = [];
  private readonly emitted: TOut[] = [];
  private readonly spec: ClassSpec;
  private readonly node: TaskNode;
  private readonly outgoing: ChannelSpec[];
  private readonly cell: InvocationInput['cell'];
  private readonly bridge: CoreBridge = {
    ...resolvedSpawns(),
    effectBegin: (_token, _item, name) => this.effectBegin(name),
    effectEnd: async (_token, _item, payload) => this.effectEnd(payload),
    effectRelease: () => {
      this.effects.running = undefined;
    },
  };
  /** Each live input's effect results, in order, and how many replays ran. */
  private readonly keptEffects: Array<Map<string, Uint8Array[]>> = [];
  private replays = 0;
  private effects = liveEffects();

  private constructor(
    private readonly tasklet: Tasklet<TIn, TOut, TState, TChange>,
    private readonly options: HarnessOptions<TState>,
    state: TState,
  ) {
    this.cellState = state;
    this.time = options.now ?? 0n;
    // Wired by the production builder rather than by a hand-written topology
    // literal: one entry channel in, one plugin channel out. A field the
    // declaration gains later reaches the harness for free. The output names the
    // MessagePack codec, so the emits the harness reads back are each value
    // exactly as the handler emitted it.
    this.spec = defineRelay({
      factoryId: `harness_${tasklet.name}`,
      channels: (c) =>
        c
          .input('harness_in')
          .into(tasklet as never)
          .output({ codec: 'msgpack' }),
    });
    this.node = this.spec.tasklets[0] as TaskNode;
    this.outgoing = this.spec.channels.filter((channel) => channel.producer !== undefined);
    // No core runs here, so the harness names the cell's identity itself.
    const identity = Buffer.from(`${options.relay}/${tasklet.name}/${options.cellKey}`);
    this.cell = { identities: new Map([['', identity]]), key: '' };
  }

  /** A harness over the tasklet's declared starting state. */
  static of<TIn, TOut, TState, TChange>(
    tasklet: Tasklet<TIn, TOut, TState, TChange>,
    options: HarnessOptions<TState> = {},
  ): TaskletTestHarness<TIn, TOut, TState, TChange> {
    const initial = options.state ?? (tasklet.definition.initState?.(options.cellKey) as TState);
    return new TaskletTestHarness(tasklet, options, initial);
  }

  /**
   * A harness restored from a durable payload — the shape a warm restart
   * hands across, including one stamped at an older schema version, so a
   * migration is provable without a controller.
   */
  static withPayload<TIn, TOut, TState, TChange>(
    tasklet: Tasklet<TIn, TOut, TState, TChange>,
    payload: RestorePayload,
    options: HarnessOptions<TState> = {},
  ): TaskletTestHarness<TIn, TOut, TState, TChange> {
    const body = (bytes: Uint8Array): RestoreBody => ({
      bytes,
      fromVersion: payload.fromVersion,
    });
    // The same fold the dispatcher runs, so what a test constructs from is
    // what a warm restart would have produced rather than an agreement with it.
    const state = foldRestore(
      tasklet.definition as never,
      {
        snapshot: payload.snapshot ? body(payload.snapshot) : undefined,
        changes: (payload.changes ?? []).map(body),
      },
      options.cellKey,
      tasklet.name,
    ) as TState;
    return new TaskletTestHarness(tasklet, options, state);
  }

  private context(now: bigint, replay = false): Invocation {
    return new Invocation({
      spec: this.spec,
      node: this.node,
      outgoing: this.outgoing,
      cell: this.cell,
      cellKey: this.options.cellKey,
      changeSeq: BigInt(this.log.length),
      now,
      replay,
      state: this.cellState,
      token: 0n,
      item: 0,
      bridge: this.bridge,
    });
  }

  private collect(ctx: Invocation): Outcome<TOut, TChange> {
    this.cellState = rawState(ctx.currentState) as TState;

    const emits = ctx.outputs.map((output) => decodeValue<TOut>(output.payload));
    const changes = ctx.changes.map((bytes) =>
      this.tasklet.definition.changeCodec
        ? this.tasklet.definition.changeCodec.decode(bytes)
        : decodeValue<TChange>(bytes),
    );
    this.emitted.push(...emits);
    this.log.push(...changes);
    return {
      emits,
      changes,
      timers: ctx.timers.map((timer) => ({ ...timer })),
      checkpoint: ctx.checkpointed,
    };
  }

  /**
   * Apply one input at the harness clock.
   *
   * Run inside the same async context the dispatcher establishes, so a handler
   * that reads an ambient clock is reported here rather than only in
   * production.
   */
  async feed(value: TIn, options: FeedOptions = {}): Promise<Outcome<TOut, TChange>> {
    if (options.replay) {
      const kept = this.keptEffects[this.replays++] ?? new Map();
      this.effects = { ...liveEffects(), kept, replaying: true };
    } else {
      this.effects = liveEffects();
      this.keptEffects.push(this.effects.kept);
    }
    const ctx = this.context(this.time, options.replay);
    await invocation.run({ tasklet: this.tasklet.name }, () => ctx.handle(value));
    if (options.replay) {
      for (const [name, results] of this.effects.kept) {
        if (results.length > (this.effects.ordinals.get(name) ?? 0)) {
          throw new EffectReplayMismatch(this.tasklet.name, name, 'was never asked for');
        }
      }
    }
    return this.collect(ctx);
  }

  /** The core's answer to a begun `ctx.effect`, over this input's results. */
  private effectBegin(name: string): NativeEffectStart {
    if (!name || Buffer.byteLength(name, 'utf8') > MAX_EFFECT_NAME) {
      throw flezhaError('effect.bad_name', { name: JSON.stringify(name) });
    }
    const effects = this.effects;
    if (effects.running) throw flezhaError('effect.nested', { name });
    const ordinal = effects.ordinals.get(name) ?? 0;
    effects.ordinals.set(name, ordinal + 1);
    const result = effects.kept.get(name)?.[ordinal];
    if (result) return { status: 'kept', result: Buffer.from(result) };
    if (effects.replaying) {
      throw new EffectReplayMismatch(this.tasklet.name, name, 'ran its function');
    }
    effects.running = { name, ordinal };
    return { status: 'run' };
  }

  /** Keep a live input's result, as the controller does. */
  private effectEnd(payload: Uint8Array): null {
    const { kept, running } = this.effects;
    if (!running) throw new Error('an end follows a begin that runs');
    const results = kept.get(running.name) ?? [];
    results[running.ordinal] = Uint8Array.from(payload);
    kept.set(running.name, results);
    this.effects.running = undefined;
    return null;
  }

  /** Fire a timer. `ctx.now()` inside the callback is the scheduled time. */
  async fireTimer(kind: TimerKind, scheduledAt: bigint): Promise<Outcome<TOut, TChange>> {
    const onTimer = this.tasklet.definition.onTimer;
    if (!onTimer) {
      throw flezhaError('runtime.timer_without_on_timer', { tasklet: this.tasklet.name });
    }
    const firing: TimerFiring = { kind, scheduledAt };
    const ctx = this.context(scheduledAt);
    await invocation.run({ tasklet: this.tasklet.name }, () => onTimer(firing, ctx as never));
    return this.collect(ctx);
  }

  /** Move the clock the next input is served at. */
  advanceTo(now: bigint): void {
    this.time = now;
  }

  /** Move the clock forward. */
  advanceBy(delta: bigint): void {
    this.time += delta;
  }

  get now(): bigint {
    return this.time;
  }

  /** The cell's state as the handler left it. */
  get state(): TState {
    return this.cellState;
  }

  /** Every change recorded so far, in order. */
  get recordedChanges(): TChange[] {
    return [...this.log];
  }

  /** Every value emitted so far, in order. */
  get emits(): TOut[] {
    return [...this.emitted];
  }

  /**
   * The determinism gate: fold the recorded change log against a freshly
   * constructed initial state and require the result to equal the live one.
   *
   * A handler that mutates its state outside a recorded change passes every
   * live assertion and loses the difference at the next restart. This is the
   * only check that sees it.
   */
  replayEquals(): void {
    const definition = this.tasklet.definition;
    const replayed = definition.initState?.(this.options.cellKey) as TState;
    for (const change of this.log) definition.applyChange?.(replayed, change);
    if (!isDeepStrictEqual(replayed, this.cellState)) {
      throw new ReplayMismatch(this.tasklet.name, this.cellState, replayed);
    }
  }
}
