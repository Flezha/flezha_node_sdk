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

import { defineUnbounded, type ChannelSpec, type ClassSpec, type TaskNode } from './authoring.js';
import { decodeValue } from './codec.js';
import { Invocation, type SpawnBridge } from './ctx.js';
import { invocation } from './determinism.js';
import { foldRestore, rawState, type RestoreBody } from './state.js';
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

function replacer(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? `${value}n` : value;
}

const BASE32 = 'abcdefghijklmnopqrstuvwxyz234567';

/**
 * A harness has no controller, so a spawn answers at once as a new instance:
 * under the id it gave, or under one shaped like a generated id, counted
 * rather than drawn so a test reads the same ids every run.
 */
function resolvedSpawns(): SpawnBridge {
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
  };
}

export interface HarnessOptions<TState> {
  cellKey?: string;
  /** Override the declared starting state. */
  state?: TState;
  /** The value `ctx.now()` serves until an input or an advance moves it. */
  now?: bigint;
  operator?: string;
}

/** A restore payload, as the core would hand one across. */
export interface RestorePayload {
  snapshot?: Uint8Array;
  changes?: Uint8Array[];
  /** Stamp every body as written at this version, so a migration is exercised. */
  fromVersion?: number;
}

/** One invocation's recorded effects. */
export interface Invoked<TOut, TChange> {
  emits: TOut[];
  changes: TChange[];
  timers: Array<{ kind: TimerKind; scheduledAt: bigint; register: boolean }>;
  checkpoint: boolean;
}

export class TaskletTestHarness<TIn, TOut, TState, TChange> {
  private state: TState;
  private clock: bigint;
  private readonly log: TChange[] = [];
  private readonly emitted: TOut[] = [];
  private readonly spec: ClassSpec;
  private readonly node: TaskNode;
  private readonly outgoing: ChannelSpec[];
  private readonly spawns = resolvedSpawns();

  private constructor(
    private readonly tasklet: Tasklet<TIn, TOut, TState, TChange>,
    private readonly options: HarnessOptions<TState>,
    state: TState,
  ) {
    this.state = state;
    this.clock = options.now ?? 0n;
    // Wired by the production builder rather than by a hand-written topology
    // literal: one entry channel in, one plugin channel out. A field the
    // declaration gains later reaches the harness for free. The sink names the
    // MessagePack codec, so the emits the harness reads back are each value
    // exactly as the handler emitted it.
    this.spec = defineUnbounded({
      factoryId: `harness_${tasklet.name}`,
      channels: (c) =>
        c
          .input('harness_in')
          .into(tasklet as never)
          .sink({ codec: 'msgpack' }),
    });
    this.node = this.spec.tasklets[0] as TaskNode;
    this.outgoing = this.spec.channels.filter((channel) => channel.producer !== undefined);
  }

  /** A harness over the tasklet's declared starting state. */
  static of<TIn, TOut, TState, TChange>(
    tasklet: Tasklet<TIn, TOut, TState, TChange>,
    options: HarnessOptions<TState> = {},
  ): TaskletTestHarness<TIn, TOut, TState, TChange> {
    const initial = options.state ?? (tasklet.definition.initialState?.(options.cellKey) as TState);
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

  private context(now: bigint): Invocation {
    return new Invocation({
      spec: this.spec,
      node: this.node,
      outgoing: this.outgoing,
      operator: this.options.operator ?? `harness_${this.tasklet.name}`,
      cellKey: this.options.cellKey,
      changeSeq: BigInt(this.log.length),
      now,
      replay: false,
      state: this.state,
      token: 0n,
      bridge: this.spawns,
    });
  }

  private collect(ctx: Invocation): Invoked<TOut, TChange> {
    this.state = rawState(ctx.currentState) as TState;

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
  async input(value: TIn): Promise<Invoked<TOut, TChange>> {
    const ctx = this.context(this.clock);
    await invocation.run({ tasklet: this.tasklet.name }, () => ctx.handle(value));
    return this.collect(ctx);
  }

  /** Fire a timer. `ctx.now()` inside the callback is the scheduled time. */
  async fireTimer(kind: TimerKind, scheduledAt: bigint): Promise<Invoked<TOut, TChange>> {
    const onTimer = this.tasklet.definition.onTimer;
    if (!onTimer) {
      throw new Error(
        `${this.tasklet.name} declares no onTimer; a tasklet that registers a timer ` +
          'handles its firing',
      );
    }
    const firing: TimerFiring = { kind, scheduledAt };
    const ctx = this.context(scheduledAt);
    await invocation.run({ tasklet: this.tasklet.name }, () => onTimer(firing, ctx as never));
    return this.collect(ctx);
  }

  /** Move the clock the next input is served at. */
  advanceTo(now: bigint): void {
    this.clock = now;
  }

  /** Move the clock forward. */
  advanceBy(delta: bigint): void {
    this.clock += delta;
  }

  get now(): bigint {
    return this.clock;
  }

  /** The cell's state as the handler left it. */
  get currentState(): TState {
    return this.state;
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
    const replayed = definition.initialState?.(this.options.cellKey) as TState;
    for (const change of this.log) definition.applyChange?.(replayed, change);
    if (!isDeepStrictEqual(replayed, this.state)) {
      throw new ReplayMismatch(this.tasklet.name, this.state, replayed);
    }
  }
}
