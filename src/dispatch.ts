/**
 * Turning one activation into one answer, exactly once.
 *
 * Per-cell exclusivity IS the handout: the core holds a cell's next
 * activation until this one is answered, so an activation that returns
 * unanswered retires that cell for the life of the process. Every path out
 * of `dispatch` therefore ends in a completion or an abandon, through a
 * `finally` rather than a rule applied at each return site — a thrown handler,
 * a rejected promise and an encoding failure all reach the same guard.
 */

import { applyTransforms } from './authoring.js';
import type { ChannelSpec, ClassSpec, TaskNode } from './authoring.js';
import { decodeValue, encodeValue, fromStateJson, toStateJson } from './codec.js';
import { Invocation, NO_ITEM, type CoreBridge } from './ctx.js';
import { invocation } from './determinism.js';
import { flezhaError } from './errors.js';
import { log } from './logger.js';
import type {
  Native,
  NativeActivation,
  NativeActivationResult,
  NativeItemEffect,
  NativeKeyRequest,
  NativePrepareAnswer,
  NativePreparedValue,
  NativeWorkItem,
} from './native.js';
import { foldRestore, rawState } from './state.js';

/**
 * Where one cell's state is kept: its relay's table, and its key there. The
 * relay is part of it because one class serves every instance and
 * partition of its factory on this runner, each with cells of its own under
 * the same tasklet and key.
 */
export interface CellAddress {
  table: Map<string, unknown>;
  /** The relay's identity bytes, as the core hands them with a cell's first activation. */
  identities: Map<string, Uint8Array>;
  key: string;
}

function tableOf<V>(tables: Map<string, Map<string, V>>, relay: string): Map<string, V> {
  let table = tables.get(relay);
  if (!table) tables.set(relay, (table = new Map()));
  return table;
}

/** A declared class, with the lookups dispatch needs on the hot path. */
export class RegisteredClass {
  /** Channels each tasklet produces on, by tasklet index. */
  readonly outgoing: ChannelSpec[][];
  /** Live cell state: one table per relay, keyed by tasklet index and cell key. */
  private readonly relays = new Map<string, Map<string, unknown>>();
  private readonly identities = new Map<string, Map<string, Uint8Array>>();

  constructor(readonly spec: ClassSpec) {
    this.outgoing = spec.tasklets.map((node) =>
      spec.channels.filter((channel) => channel.producer === node.index),
    );
  }

  /** The address of the cell an activation names, keeping the identity bytes it carries. */
  address(activation: NativeActivation, node: TaskNode): CellAddress {
    const key = `${node.index}\0${activation.cellKey ?? ''}`;
    const identities = tableOf(this.identities, activation.relay);
    if (activation.cellIdentity) identities.set(key, activation.cellIdentity);
    return { table: tableOf(this.relays, activation.relay), identities, key };
  }

  /** Drop a cell that left this runner, or with `wholeRelay` every cell of its relay. */
  forget(activation: NativeActivation, node: TaskNode): void {
    for (const tables of [this.relays, this.identities]) {
      if (activation.wholeRelay) tables.delete(activation.relay);
      else tables.get(activation.relay)?.delete(`${node.index}\0${activation.cellKey ?? ''}`);
    }
  }

  /** The cell's live state, created from the declaration on first touch. */
  stateOf(node: TaskNode, address: CellAddress, cellKey: string | undefined): unknown {
    const { table, key } = address;
    if (!table.has(key)) {
      const initial = node.tasklet.definition.initState;
      table.set(key, initial ? initial(cellKey) : undefined);
    }
    return table.get(key);
  }

  setState(address: CellAddress, state: unknown): void {
    address.table.set(address.key, state);
  }
}

export class Registry {
  private readonly classes = new Map<string, RegisteredClass>();

  add(spec: ClassSpec): RegisteredClass {
    if (this.classes.has(spec.factoryId)) {
      throw flezhaError('topology.duplicate_factory_id', { factory_id: spec.factoryId });
    }
    const registered = new RegisteredClass(spec);
    this.classes.set(spec.factoryId, registered);
    return registered;
  }

  get(factoryId: string): RegisteredClass | undefined {
    return this.classes.get(factoryId);
  }

  remove(factoryId: string): void {
    this.classes.delete(factoryId);
  }

  all(): RegisteredClass[] {
    return [...this.classes.values()];
  }
}

function failed(token: bigint, message: string, nonRetryable = false): NativeActivationResult {
  return { token, outcome: 'failed', error: message, nonRetryable };
}

/**
 * What a failure says on the wire. `name: message`, in family with what the
 * other SDKs send — this string is stored in a dead-letter entry and read by
 * a user, and a multi-line stack there is noise the retry ladder
 * multiplies. The stack is logged host-side instead, where it is a diagnostic
 * rather than a record.
 */
function errorMessage(error: unknown): string {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  return String(error);
}

/** A handler declares an error non-retryable by setting the flag on it. */
function isNonRetryable(error: unknown): boolean {
  return Boolean(
    error && typeof error === 'object' && (error as { nonRetryable?: boolean }).nonRetryable,
  );
}

export interface DispatchDeps {
  registry: Registry;
  bridge: CoreBridge;
}

/**
 * What a dispatch loop needs to answer an activation, over one addon.
 *
 * Both loops build it the same way — that is what "one dispatch path" means at
 * this level — so a third kind of loop gets it by calling this rather than by
 * copying it.
 */
export function dispatchDeps(native: Native, registry: Registry): DispatchDeps {
  return { registry, bridge: native };
}

/**
 * Produce the answer for one activation. Throwing is not an option here: every
 * failure becomes a result the core can act on, because the alternative is an
 * cell that never receives work again.
 */
export async function answer(
  activation: NativeActivation,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const registered = deps.registry.get(activation.factory);
  if (!registered) {
    return failed(
      activation.token,
      `no class registered for factory \`${activation.factory}\``,
      true,
    );
  }

  try {
    const node = registered.spec.tasklets[activation.tasklet];
    if (!node) {
      return failed(
        activation.token,
        `class \`${activation.factory}\` has no tasklet at index ${activation.tasklet}`,
        true,
      );
    }

    const address = registered.address(activation, node);
    switch (activation.kind) {
      case 'work':
        return await runWork(activation, registered, node, address, deps);
      case 'restore_fold':
        return restoreFold(activation, registered, node, address);
      case 'snapshot_request':
        return snapshot(activation, node, registered.stateOf(node, address, activation.cellKey));
      case 'live_state_read':
        return {
          token: activation.token,
          outcome: 'state_json',
          json: toStateJson(registered.stateOf(node, address, activation.cellKey)),
        };
      case 'apply_state':
        registered.setState(address, fromStateJson(activation.stateJson ?? Buffer.alloc(0)));
        return { token: activation.token, outcome: 'state_applied' };
      case 'key_request':
        return prepare(activation, registered);
      case 'compensate_run':
        return await compensate(activation, registered, node, address, deps);
      case 'leave':
        registered.forget(activation, node);
        return { token: activation.token, outcome: 'state_applied' };
      default:
        return failed(activation.token, `unknown activation kind \`${activation.kind}\``, true);
    }
  } catch (error) {
    return failed(activation.token, errorMessage(error), isNonRetryable(error));
  }
}

/**
 * Run user code against one cell, and keep the state it produced.
 *
 * Every activation kind that reaches a handler goes through here — an input,
 * a timer firing, a compensation — so the context they receive and the state
 * they leave behind cannot differ between them. The state is written back
 * whether or not the body threw: a change recorded before the throw is
 * already journaled, and dropping it here would make the live cell
 * disagree with its own log.
 */
async function invoke(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: TaskNode,
  address: CellAddress,
  deps: DispatchDeps,
  at: { now: bigint; replay: boolean; offset?: bigint; item: number },
  body: (ctx: Invocation) => Promise<unknown>,
): Promise<Invocation> {
  const ctx = new Invocation({
    spec: registered.spec,
    node,
    outgoing: registered.outgoing[node.index] as ChannelSpec[],
    cell: address,
    cellKey: activation.cellKey,
    changeSeq: activation.changeSeq,
    now: at.now,
    replay: at.replay,
    state: registered.stateOf(node, address, activation.cellKey),
    token: activation.token,
    item: at.item,
    offset: at.offset,
    bridge: deps.bridge,
    compensating: activation.kind === 'compensate_run',
  });
  try {
    await invocation.run({ tasklet: node.name }, () => body(ctx));
    return ctx;
  } finally {
    registered.setState(address, rawState(ctx.currentState));
  }
}

async function runWork(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: TaskNode,
  address: CellAddress,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const definition = node.tasklet.definition;
  const effects: NativeItemEffect[] = [];

  for (const [index, item] of (activation.items ?? []).entries()) {
    const now = item.now ?? 0n;
    try {
      const ctx = await invoke(
        activation,
        registered,
        node,
        address,
        deps,
        { now, replay: item.replay ?? false, offset: item.consumedOffset, item: index },
        async (ctx) => {
          if (item.kind === 'timer') {
            if (!definition.onTimer) {
              // Registration is a runtime call, so this cannot be refused at declaration time.
              throw flezhaError('runtime.timer_without_on_timer', { tasklet: node.name });
            }
            await definition.onTimer(
              { kind: item.timerKind ?? 'processing_time', scheduledAt: item.scheduledAt ?? now },
              ctx as never,
            );
            return;
          }
          // The prepare crossing decoded a keyed payload and ran this node's
          // edge already; each value comes encoded as a value between tasklets.
          const values =
            item.form === 'prepared'
              ? (item.values ?? []).map((bytes) => decodeValue(bytes))
              : entryValues(registered, node, item);
          // A filtered value is consumed and reaches no handler, and a fanned
          // one reaches it once per value. Either way the input produces one
          // effect, so its offset advances once and nothing is retried.
          for (const input of values) {
            await ctx.handle(input);
          }
        },
      );

      effects.push(ctx.effectRecord(item.consumedOffset));
    } catch (error) {
      // The stack is a host diagnostic, not a record: the verdict below
      // travels to the dead-letter entry and the controller's log.
      log().debug(`${node.name} failed an item: ${(error as Error)?.stack ?? error}`);
      // Items after this one are re-handed by the core, so the batch stops
      // here rather than reporting effects the cell never saw.
      effects.push({
        ...failureOf(error),
        eventTime: now,
        consumedOffset: item.consumedOffset,
      });
      break;
    }
  }

  return { token: activation.token, outcome: 'work', effects };
}

/**
 * An entry payload the channel's decoder or its edge's transforms rejected:
 * the one failure the core is told apart from a handler's, because it parks
 * the payload rather than counting it as a tasklet failure. Only the entry
 * seam raises it.
 */
class UnreadableEntry extends Error {
  constructor(cause: unknown) {
    super(errorMessage(cause));
    this.name = 'UnreadableEntry';
  }
}

/**
 * A failed item's verdict and the message the wire carries: unreadable at the
 * entrance, reported as its cause, else the handler's.
 */
function failureOf(error: unknown): Pick<NativeItemEffect, 'verdict' | 'message'> {
  if (error instanceof UnreadableEntry) return { verdict: 'unreadable', message: error.message };
  return {
    verdict: isNonRetryable(error) ? 'non_retryable' : 'retryable',
    message: errorMessage(error),
  };
}

/**
 * A delivered payload as the handler sees it: decoded, then through this
 * node's own edge off the channel when the core marks it untransformed. A flow
 * producer already ran that edge, and running it again would compose the
 * chain with itself. A failure on an entry channel (the wire's bytes, not a
 * producer's) is the entrance's, and is reported as such.
 */
function entryValues(registered: RegisteredClass, node: TaskNode, item: NativeWorkItem): unknown[] {
  const channel = registered.spec.channels[item.channel ?? 0] as ChannelSpec | undefined;
  const edge =
    item.form === 'untransformed'
      ? channel?.consumers.find((each) => each.tasklet === node.index)
      : undefined;
  try {
    return applyTransforms(
      edge?.transforms ?? [],
      decoded(channel, item.payload ?? Buffer.alloc(0)),
    );
  } catch (error) {
    if (channel === undefined || channel.kind === 'entry') throw new UnreadableEntry(error);
    throw error;
  }
}

/** A payload as its channel reads it: the crossing and the delivery both start here. */
function decoded(channel: ChannelSpec | undefined, payload: Uint8Array): unknown {
  return channel?.decode ? channel.decode(payload) : decodeValue(payload);
}

function restoreFold(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: TaskNode,
  address: CellAddress,
): NativeActivationResult {
  registered.setState(
    address,
    foldRestore(
      node.tasklet.definition as never,
      { snapshot: activation.snapshot, changes: activation.changes },
      activation.cellKey,
      node.name,
    ),
  );
  return { token: activation.token, outcome: 'restored' };
}

function snapshot(
  activation: NativeActivation,
  node: TaskNode,
  state: unknown,
): NativeActivationResult {
  const codec = node.tasklet.definition.stateCodec;
  return {
    token: activation.token,
    outcome: 'snapshot',
    state: codec ? Buffer.from(codec.encode(state as never)) : encodeValue(state),
  };
}

function prepare(
  activation: NativeActivation,
  registered: RegisteredClass,
): NativeActivationResult {
  const request = activation.keyRequest;
  if (!request) {
    return failed(activation.token, 'a key request carried no payloads', true);
  }
  const channel = registered.spec.channels[request.channel] as ChannelSpec | undefined;
  const answers: NativePrepareAnswer[] = request.payloads.map((payload) => {
    try {
      return prepared(registered, channel, request, decoded(channel, payload));
    } catch (error) {
      // A per-payload refusal enters the input channel's retry ladder; it is
      // not a failure of the crossing, whose other payloads still have answers.
      return { error: errorMessage(error), nonRetryable: isNonRetryable(error) };
    }
  });

  return { token: activation.token, outcome: 'prepared', prepared: answers };
}

/**
 * One decoded payload's answer: every value each keyed consumer's own edge
 * yields, with the Cells it names, and the event time the first timed consumer
 * reads off its first value. A payload its chains filter answers no value, so
 * the core mints no Cell for it.
 */
function prepared(
  registered: RegisteredClass,
  channel: ChannelSpec | undefined,
  request: NativeKeyRequest,
  value: unknown,
): NativePrepareAnswer {
  const values: NativePreparedValue[] = [];
  let eventTime: bigint | undefined;
  for (const edge of channel?.consumers ?? []) {
    const definition = registered.spec.tasklets[edge.tasklet]!.tasklet.definition;
    const cellsFor = definition.cellsFor;
    const keyed = request.wantKeys && (edge.keyBy !== undefined || cellsFor !== undefined);
    const timed = request.wantEventTime && eventTime === undefined && definition.eventTime;
    if (!keyed && !timed) continue;
    const yielded = applyTransforms(edge.transforms, value);
    if (timed && yielded.length > 0) eventTime = BigInt(definition.eventTime!(yielded[0] as never));
    if (!keyed) continue;
    for (const each of yielded) {
      const keys = edge.keyBy ? [edge.keyBy(each)] : cellsFor!(each);
      values.push({ consumer: edge.tasklet, keys, bytes: encodeValue(each) });
    }
  }
  return { values, eventTime };
}

async function compensate(
  activation: NativeActivation,
  registered: RegisteredClass,
  node: TaskNode,
  address: CellAddress,
  deps: DispatchDeps,
): Promise<NativeActivationResult> {
  const definition = node.tasklet.definition;
  if (!definition.compensate) {
    return failed(activation.token, `${node.name} declares no compensate hook`, true);
  }
  const outputs = (activation.outputs ?? []).map((bytes) => decodeValue(bytes));
  const ctx = await invoke(
    activation,
    registered,
    node,
    address,
    deps,
    { now: 0n, replay: false, item: NO_ITEM },
    async (ctx) => (definition.compensate as (o: unknown[], c: unknown) => unknown)(outputs, ctx),
  );
  return { token: activation.token, outcome: 'work', effects: [ctx.effectRecord(undefined)] };
}

/**
 * Dispatch one activation and answer it exactly once.
 *
 * The guard is the contract, not politeness. An unanswered activation retires
 * its cell for the life of the process, so a throw from the dispatcher, from
 * encoding the result, or from `complete` itself abandons rather than logging
 * and returning.
 */
export async function dispatch(
  activation: NativeActivation,
  native: Native,
  deps: DispatchDeps,
): Promise<void> {
  let answered = false;
  try {
    const result = await answer(activation, deps);
    await native.complete(result);
    answered = true;
  } catch (error) {
    log().error(`activation processing failed: ${errorMessage(error)}`);
  } finally {
    if (!answered) {
      try {
        native.abandon(activation.token);
      } catch (error) {
        // The core refuses an unknown token: `complete` had already landed and
        // only the bookkeeping after it threw. Nothing is wedged.
        log().warn(`abandoning activation ${activation.token} failed: ${errorMessage(error)}`);
      }
    }
  }
}
