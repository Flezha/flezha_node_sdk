// The builders the dispatch-shaped suites share: what one relay's dispatch
// depends on, the activation the core hands out, and a declaration's refusal.
import assert from 'node:assert/strict';

import { FlezhaError, defineRelay, defineTasklet, validate } from '../dist/esm/index.js';
import { Registry, answer } from '../dist/esm/dispatch.js';

/**
 * The dispatch dependencies over `specs`, registered in the order given.
 *
 * Submitted first, exactly as a runner registers: the consumer edges a
 * dispatch routes on are what the core derived, so a spec the core has not
 * judged has none.
 */
export function depsFor(...specs) {
  const registry = new Registry();
  for (const spec of specs) {
    validate(spec);
    registry.add(spec);
  }
  return { registry, bridge: spawnBridge() };
}

/**
 * The addon's three spawn entries, answering the JSON the core does.
 * `answerFor(request)` is what a read of that request's handle answers, `null`
 * while pending; by default a spawn under the id it named, or a generated one.
 * As in the core, a policy without an id is refused at the call, a handle's
 * first answer is what every later read answers, and no read waits. Every
 * request and every read (`try` or `wait`) is recorded.
 */
export function spawnBridge(
  answerFor = (request) => ({ id: request.id || 'worker-k7m2qzxw4pd3fn2a', outcome: 'spawned' }),
) {
  const answers = [];
  const bridge = {
    requests: [],
    reads: [],
    spawnRequest: (token, factoryId, id, payload, ifRunning, replay, offset) => {
      if (!id && ifRunning) {
        const refusal = {
          code: 'spawn.if_running_without_id',
          message: 'needs an id',
          details: {},
        };
        throw FlezhaError.fromDocument(JSON.stringify({ error: refusal }));
      }
      return BigInt(
        bridge.requests.push({ token, factoryId, id, payload, ifRunning, replay, offset }),
      );
    },
    spawnTryResult: (handle) => read('try', handle),
    spawnWait: async (handle) => read('wait', handle),
  };
  const read = (kind, handle) => {
    bridge.reads.push(kind);
    const at = Number(handle) - 1;
    answers[at] ??= answerFor(bridge.requests[at]);
    // A failed spawn is thrown, as the addon throws the core's error document.
    if (answers[at]?.error) throw FlezhaError.fromDocument(JSON.stringify(answers[at]));
    return answers[at] === null ? null : JSON.stringify(answers[at]);
  };
  return bridge;
}

/**
 * Run `body(input, ctx)` as a tasklet's `run` on one input, `'p'`, under activation
 * token 7 with `bridge` for its spawns; answers that input's effect.
 */
export async function handleOnce(bridge, body, item = {}) {
  const spec = defineRelay({
    factoryId: 'once',
    channels: (c) =>
      c
        .input('in')
        .into(defineTasklet({ name: 'once', run: body }))
        .output('out'),
  });
  const work = activationFor('once')({ token: 7n, items: [inputItem('p', item)] });
  const result = await answer(work, { ...depsFor(spec), bridge });
  return result.effects[0];
}

/** An activation builder bound to one factory; `overrides` names what varies. */
export const activationFor =
  (factory) =>
  (overrides = {}) => ({
    token: 1n,
    relay: 'op-1',
    tasklet: 0,
    cellKey: 'k',
    changeSeq: 0n,
    factory,
    kind: 'work',
    ...overrides,
  });

/** One input item of a work batch, as the JSON an entry naming no codec reads. */
export const inputItem = (value, overrides = {}) => ({
  kind: 'input',
  channel: 0,
  payload: Buffer.from(JSON.stringify(value)),
  metadata: [],
  replay: false,
  now: 1_000n,
  ...overrides,
});

/** A plugin frame read back as the JSON an output naming no codec sends. */
export const jsonOf = (bytes) => JSON.parse(Buffer.from(bytes).toString('utf8'));

/** The FlezhaError that building and submitting `build()` throws. */
export function refusal(build) {
  try {
    validate(build());
  } catch (error) {
    assert.ok(error instanceof FlezhaError, `expected a FlezhaError, got ${error}`);
    return error;
  }
  return assert.fail('expected a refusal');
}

/** Every code a refusal names: each issue the core listed, else its own. */
export function codesOf(error) {
  const { issues } = error.details;
  return Array.isArray(issues) ? issues.map((issue) => issue.code) : [error.code];
}

/** An `assert.throws` / `assert.rejects` matcher for a FlezhaError with `code`. */
export const coded = (code) => (error) => error instanceof FlezhaError && error.code === code;

/** A keyed running total, one cell per value. */
export const Counter = defineTasklet({
  name: 'counter',
  initState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.by;
  },
  run: (input, ctx) => {
    ctx.change({ by: input });
    ctx.emit(ctx.state.total);
  },
});

export const Counters = defineRelay({
  factoryId: 'counters',
  channels: (c) => {
    c.input('in')
      .keyBy((value) => String(value))
      .into(Counter)
      .keyed()
      .output('out');
  },
});
