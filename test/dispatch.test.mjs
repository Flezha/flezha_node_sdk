// One activation, one answer — and the guard that makes it exactly one.
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  decodeValue,
  defineRelay,
  defineTasklet,
  encodeValue,
  loadNative,
} from '../dist/esm/index.js';
import { Registry, answer, dispatch } from '../dist/esm/dispatch.js';
import { stableId } from '../dist/esm/identity.js';
import { createRunner, importModule, loadRegistry, specsFrom } from '../dist/esm/runner.js';

import {
  Counter,
  Counters as Spec,
  activationFor,
  depsFor,
  inputItem,
  jsonOf,
} from './dispatch-fixtures.mjs';
import { testAppModule } from './test-app.mjs';

// The handout-contract tests below answer against the core itself, so the
// refusal they assert is the core's own. Nothing starts a runner, so no
// controller connection is attempted.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

const tempDirs = [];
function tempDir(prefix) {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}
after(() => tempDirs.forEach((dir) => rmSync(dir, { recursive: true, force: true })));

const fixtures = await testAppModule('fixtures.mjs');

/**
 * What the SDK says while `run` executes.
 *
 * Read off the console rather than through `setLogger`, so what is asserted is
 * what a host with no logger of its own would actually see.
 */
async function said(run) {
  const lines = [];
  const console_ = { error: console.error, warn: console.warn, log: console.log };
  console.error = (line) => lines.push(line);
  console.warn = (line) => lines.push(line);
  console.log = (line) => lines.push(line);
  try {
    await run();
  } finally {
    Object.assign(console, console_);
  }
  return lines.join('\n');
}

const fixture = () => depsFor(Spec);
const activation = activationFor('counters');

test('a work batch answers one effect per item, in order', async () => {
  // One cell's batch: every item carries the key that routed it here.
  const result = await answer(
    activation({ cellKey: '2', items: [inputItem(2), inputItem(2)] }),
    fixture(),
  );
  assert.equal(result.outcome, 'work');
  assert.equal(result.effects.length, 2);
  assert.deepEqual(
    result.effects.map((effect) => effect.verdict),
    ['ok', 'ok'],
  );
  assert.deepEqual(
    result.effects.flatMap((effect) => effect.outputs.map((o) => jsonOf(o.payload))),
    [2, 4],
  );
  assert.equal(result.effects[0].outputs[0].target, 'plugin');
  assert.equal(result.effects[0].outputs[0].pluginChannel, 'out');
  assert.equal(result.effects[0].outputs[0].key, '2');
  assert.equal(result.effects[0].eventTime, 1_000n);
});

test('a throwing handler is a retryable verdict and stops the batch', async () => {
  const Throwing = defineTasklet({
    name: 'throwing',
    run: (input, ctx) => {
      if (input === 'boom') throw new Error('handler exploded');
      ctx.emit(input);
    },
  });
  const spec = defineRelay({
    factoryId: 'throwing_op',
    channels: (c) => c.input('in').into(Throwing).output(),
  });
  const result = await answer(
    activation({
      factory: 'throwing_op',
      cellKey: undefined,
      items: [inputItem('ok'), inputItem('boom'), inputItem('never')],
    }),
    depsFor(spec),
  );
  assert.equal(result.effects.length, 2, 'items after the failure are re-handed by the core');
  assert.equal(result.effects[1].verdict, 'retryable');
  assert.match(result.effects[1].message, /handler exploded/);
});

test('a rejected promise is answered the same way a throw is', async () => {
  const Rejecting = defineTasklet({
    name: 'rejecting',
    run: async () => {
      throw Object.assign(new Error('gave up'), { nonRetryable: true });
    },
  });
  const spec = defineRelay({
    factoryId: 'rejecting_op',
    channels: (c) => c.input('in').into(Rejecting).output(),
  });
  const result = await answer(
    activation({ factory: 'rejecting_op', cellKey: undefined, items: [inputItem(1)] }),
    depsFor(spec),
  );
  assert.equal(result.effects[0].verdict, 'non_retryable');
  assert.match(result.effects[0].message, /gave up/);
});

test('a restore fold rebuilds the state, and a snapshot encodes it back', async () => {
  const deps = fixture();
  const restored = await answer(
    activation({
      kind: 'restore_fold',
      snapshot: { bytes: Buffer.from(encodeValue({ total: 10 })) },
      changes: [
        { bytes: Buffer.from(encodeValue({ by: 5 })) },
        { bytes: Buffer.from(encodeValue({ by: 2 })) },
      ],
    }),
    deps,
  );
  assert.equal(restored.outcome, 'restored');

  const snapshot = await answer(activation({ kind: 'snapshot_request' }), deps);
  assert.deepEqual(decodeValue(snapshot.state), { total: 17 });

  const read = await answer(activation({ kind: 'live_state_read' }), deps);
  assert.deepEqual(JSON.parse(read.json.toString()), { total: 17 });
});

test('a migration handover round-trips through the JSON form', async () => {
  const deps = fixture();
  await answer(
    activation({
      kind: 'restore_fold',
      changes: [{ bytes: Buffer.from(encodeValue({ by: 4 })) }],
    }),
    deps,
  );
  const serialized = await answer(activation({ kind: 'live_state_read' }), deps);

  const receiving = fixture();
  const applied = await answer(
    activation({ kind: 'apply_state', stateJson: serialized.json }),
    receiving,
  );
  assert.equal(applied.outcome, 'state_applied');
  const snapshot = await answer(activation({ kind: 'snapshot_request' }), receiving);
  assert.deepEqual(decodeValue(snapshot.state), { total: 4 });
});

test('the JSON handover carries a bigint and a byte array without narrowing', async () => {
  const Wide = defineTasklet({
    name: 'wide',
    initState: () => ({ offset: 0n, blob: new Uint8Array() }),
    applyChange: (state, change) => {
      state.offset = change.offset;
      state.blob = change.blob;
    },
    run: () => undefined,
  });
  const spec = defineRelay({
    factoryId: 'wide_op',
    channels: (c) => c.input('in').into(Wide).output(),
  });
  const source = depsFor(spec);
  const huge = (1n << 63n) + 7n;
  await answer(
    activation({
      factory: 'wide_op',
      cellKey: undefined,
      kind: 'restore_fold',
      changes: [{ bytes: Buffer.from(encodeValue({ offset: huge, blob: new Uint8Array([9]) })) }],
    }),
    source,
  );
  const serialized = await answer(
    activation({ factory: 'wide_op', cellKey: undefined, kind: 'live_state_read' }),
    source,
  );

  const target = depsFor(spec);
  await answer(
    activation({
      factory: 'wide_op',
      cellKey: undefined,
      kind: 'apply_state',
      stateJson: serialized.json,
    }),
    target,
  );
  const snapshot = await answer(
    activation({ factory: 'wide_op', cellKey: undefined, kind: 'snapshot_request' }),
    target,
  );
  const state = decodeValue(snapshot.state);
  assert.equal(state.offset, huge);
  assert.deepEqual([...state.blob], [9]);
});

/** The keys a prepare crossing on `channel` answers, one set per text payload. */
async function keysAt(spec, channel, ...payloads) {
  const result = await answer(
    activationFor(spec.factoryId)({
      kind: 'key_request',
      cellKey: undefined,
      keyRequest: {
        channel,
        payloads: payloads.map((text) => Buffer.from(text)),
        wantKeys: true,
        wantEventTime: false,
      },
    }),
    depsFor(spec),
  );
  assert.equal(result.outcome, 'prepared');
  return result.prepared.map((answerFor) => answerFor.keys);
}

test('a prepare crossing answers one key per payload, in order', async () => {
  assert.deepEqual(await keysAt(Spec, 0, '7', '8'), [['7'], ['8']]);
});

test('a prepare crossing answers the keyed consumer behind an unkeyed one', async () => {
  const Watch = defineTasklet({ name: 'watch', run: (input, ctx) => ctx.emit(input) });
  const spec = defineRelay({
    factoryId: 'split_keyed',
    channels: (c) => {
      const entry = c.input('in');
      entry
        .filter((value) => value === 'drop')
        .into(Watch)
        .output('dropped');
      entry
        .filter((value) => value !== 'drop')
        .keyBy((value) => value.split(':')[0])
        .into(Counter)
        .keyed()
        .output('out');
    },
  });
  assert.deepEqual(await keysAt(spec, 0, '"a:1"', '"drop"'), [['a'], []]);
});

test('one payload failing its prepare does not fail the others', async () => {
  const Windowed = defineTasklet({
    name: 'windowed',
    eventTime: (input) => {
      if (input.at === undefined) throw new Error('no event time on this payload');
      return BigInt(input.at);
    },
    run: (input, ctx) => ctx.emit(input),
  });
  const spec = defineRelay({
    factoryId: 'windowed_op',
    channels: (c) =>
      c
        .input('in')
        .keyBy((v) => v.k)
        .into(Windowed)
        .keyed()
        .output(),
  });
  const result = await answer(
    activation({
      factory: 'windowed_op',
      cellKey: undefined,
      kind: 'key_request',
      keyRequest: {
        channel: 0,
        payloads: [{ k: 'a', at: 5 }, { k: 'b' }].map((v) => Buffer.from(JSON.stringify(v))),
        wantKeys: true,
        wantEventTime: true,
      },
    }),
    depsFor(spec),
  );
  assert.deepEqual(result.prepared[0], { keys: ['a'], eventTime: 5n });
  assert.match(result.prepared[1].error, /no event time/);
});

test('an unanswerable activation abandons rather than retiring its cell', async () => {
  // A token the core never handed out: it refuses the completion, and refuses
  // the abandon the guard falls back to. Neither refusal escapes the guard.
  const reported = await said(() =>
    dispatch(activation({ token: 42n, items: [inputItem(1)] }), native, fixture()),
  );
  assert.match(reported, /runtime\.unknown_activation_token: activation token 42 /);
  assert.match(reported, /abandoning activation 42 failed/);
});

test('a completed activation is not also abandoned', async () => {
  // Only `complete` is scripted, and only to reach the path a real handout
  // would reach: the abandon is the core's own, so a guard that abandoned an
  // answered activation would earn its refusal and report it.
  const peer = { complete: async () => {}, abandon: (token) => native.abandon(token) };
  const reported = await said(() =>
    dispatch(activation({ token: 7n, items: [inputItem(1)] }), peer, fixture()),
  );
  assert.equal(reported, '');
});

test('an activation for an unknown factory is answered, not dropped', async () => {
  // The failure reaches the core: it refuses the token the answer carries,
  // which is what tells the completion apart from a dropped activation.
  const completed = [];
  const peer = {
    complete: async (result) => {
      completed.push(result);
      await native.complete(result);
    },
    abandon: (token) => native.abandon(token),
  };
  const reported = await said(() =>
    dispatch(activation({ token: 13n, factory: 'never_declared' }), peer, fixture()),
  );
  assert.equal(completed[0].outcome, 'failed');
  assert.match(completed[0].error, /no class registered/);
  assert.equal(completed[0].nonRetryable, true);
  assert.match(reported, /runtime\.unknown_activation_token: activation token 13 /);
});

test('a module specifier is imported and its relays registered', async () => {
  const dir = tempDir('flezha-specifier-');
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineRelay, defineTasklet } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const Echo = defineTasklet({ name: 'echo', run: (i) => i });\n` +
      `export default defineRelay({ factoryId: 'from_specifier', channels: (c) => c.input('in').into(Echo).output() });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.ok(registry.get('from_specifier'), "the module's relay was registered");
  assert.deepEqual(
    specsFrom(module, await importModule(module)).map((s) => s.factoryId),
    ['from_specifier'],
  );
});

// Each build is its own compile of `importModule`, so each loads a handler by a
// relative path, the spelling an author passes to `registerFactory`.
for (const [build, load, file, bind, exporting] of [
  [
    'esm',
    loadRegistry,
    'handlers.mjs',
    (sdk) => `import { defineRelay, defineTasklet } from '${sdk}';`,
    'export default',
  ],
  [
    'cjs',
    createRequire(import.meta.url)('../dist/cjs/runner.js').loadRegistry,
    'handlers.cjs',
    (sdk) => `const { defineRelay, defineTasklet } = require('${sdk}');`,
    'module.exports =',
  ],
]) {
  test(`the ${build} build imports a handler module by relative path`, async () => {
    const module = join(tempDir('flezha-specifier-'), file);
    writeFileSync(
      module,
      `${bind(new URL(`../dist/${build}/index.js`, import.meta.url).pathname)}\n` +
        `const Echo = defineTasklet({ name: 'echo', run: (i) => i });\n` +
        `${exporting} defineRelay({ factoryId: 'by_relative_path', channels: (c) => c.input('in').into(Echo).output() });\n`,
    );
    const registry = await load([`./${relative(process.cwd(), module)}`]);
    assert.ok(registry.get('by_relative_path'), "the module's relay was registered");
  });
}

test('an unimportable specifier is named, with the original error chained', async () => {
  await assert.rejects(
    () => importModule('./definitely-not-here.mjs'),
    (error) =>
      error.message.includes('./definitely-not-here.mjs') &&
      error.message.includes('module specifier') &&
      error.cause instanceof Error,
  );
});

// A graph is built at module scope, so a builder refusal is thrown while the
// module is imported, and it reaches the author as itself.
test('a refusal raised while a handler module is imported keeps its own name', async () => {
  const dir = tempDir('flezha-module-scope-');
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineRelay, defineTasklet } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const A = defineTasklet({ name: 'twin', run: (i) => i });\n` +
      `const B = defineTasklet({ name: 'twin', run: (i) => i });\n` +
      `export default defineRelay({ factoryId: 'module_scope', channels: (c) => {\n` +
      `  const s = c.input('in'); s.into(A); s.into(B);\n` +
      `} });\n`,
  );
  await assert.rejects(
    () => loadRegistry([module]),
    (error) => error instanceof FlezhaError && error.code === 'topology.node_identity_collision',
  );
});

test('a module that throws anything but a refusal is named, with the original error chained', async () => {
  const dir = tempDir('flezha-broken-');
  const module = join(dir, 'broken.mjs');
  writeFileSync(module, 'export default { x: , };\n');
  await assert.rejects(
    () => loadRegistry([module]),
    (error) =>
      error.message.includes(module) &&
      error.message.includes('module specifier') &&
      error.cause instanceof SyntaxError,
  );
});

test('a module exporting no relay is refused naming the specifier', async () => {
  const dir = tempDir('flezha-empty-');
  const module = join(dir, 'empty.mjs');
  writeFileSync(module, 'export const nothing = 1;\n');
  await assert.rejects(
    () => loadRegistry([module]),
    (error) => error.code === 'topology.no_relay_exported' && error.details.module === module,
  );
});

test('a duplicate factory id fails at the second declaration, naming it', () => {
  const registry = new Registry();
  registry.add(Spec);
  assert.throws(
    () => registry.add(Spec),
    (error) =>
      error.message.startsWith('topology.duplicate_factory_id') &&
      error.details.factory_id === 'counters',
  );
});

// --- a keyed plugin output ---------------------------------------------------
//
// The core stamps a keyed output's identity at settle; the host hands it the
// key and the user's metadata only.

test('a keyed plugin output carries its key and the user metadata, and cannot spoof a stamp', async () => {
  const UserMeta = defineTasklet({
    name: 'stamp_user_meta',
    run: (input, ctx) => {
      if (input === 'spoof') {
        ctx.emit('v', { metadata: { 'flz.seq': '999.0' } });
        return;
      }
      ctx.emit('v', { metadata: { user: 'meta' } });
    },
  });
  const spec = defineRelay({
    factoryId: 'stamps_user',
    channels: (c) => {
      c.input('in')
        .keyBy(() => 'ek')
        .into(UserMeta)
        .keyed()
        .output();
    },
  });
  const activation = activationFor('stamps_user');

  const ok = await answer(
    activation({ cellKey: 'ek', changeSeq: 2n, items: [inputItem('plain')] }),
    depsFor(spec),
  );
  const [output] = ok.effects[0].outputs;
  assert.equal(output.key, 'ek');
  assert.deepEqual(output.metadata, [{ key: 'user', value: 'meta' }]);

  const spoofed = await answer(
    activation({ cellKey: 'ek', changeSeq: 2n, items: [inputItem('spoof')] }),
    depsFor(spec),
  );
  assert.equal(spoofed.effects[0].verdict, 'retryable');
});

// --- the structured-steps emit arm ---------------------------------------
//
// One released value is ONE edge output on a steps class, untransformed and
// unkeyed, naming the producing channel and its first consumer: the driver
// fans it out from the journaled handoff. On a flow class the same topology
// releases one output per consumer, transformed and keyed. The plugin output
// rides alongside either way.

const StepsProducer = defineTasklet({
  name: 'steps_producer',
  run: (input, ctx) => {
    ctx.emit(input);
  },
});

/** What a consumer of the `handoff` channel was handed, in delivery order. */
const DELIVERED = [];

/** A tasklet that records what it is handed; `cells` declares how it is keyed. */
const record = (name, cells = {}) =>
  defineTasklet({
    name,
    ...cells,
    run: (input) => {
      DELIVERED.push(input);
    },
  });

/** Answer one work activation on `spec`: its effects, and what its handlers were handed. */
async function ran(spec, overrides) {
  DELIVERED.length = 0;
  const { effects } = await answer(activationFor(spec.factoryId)(overrides), depsFor(spec));
  return { effects, handed: [...DELIVERED] };
}

const stepsGraph = (c) => {
  const edge = c.input('in').into(StepsProducer).named('handoff');
  edge.map((value) => value + 1).into(record('steps_b'));
  edge.into(record('steps_c'));
  edge.output();
};

/**
 * An edge payload is the class codec's MessagePack; the `handoff` output names no
 * codec, so its frame for a number is JSON.
 */
const payloadOf = (output) =>
  output.target === 'plugin' ? jsonOf(output.payload) : decodeValue(output.payload);

const StepsSpec = defineRelay({ factoryId: 'steps_emit', steps: {}, channels: stepsGraph });
const FlowTwinSpec = defineRelay({
  factoryId: 'steps_emit_stream',
  channels: stepsGraph,
});

test('a structured steps releases one untransformed edge output per emit', async () => {
  const result = await answer(
    activationFor('steps_emit')({ items: [inputItem(10)] }),
    depsFor(StepsSpec),
  );
  assert.equal(result.outcome, 'work');
  const outputs = result.effects[0].outputs;
  assert.deepEqual(
    outputs.map((o) => [o.target, o.consumer, o.pluginChannel, payloadOf(o)]),
    [
      ['edge', 1, undefined, 10],
      ['plugin', undefined, 'handoff', 10],
    ],
  );
  assert.equal(outputs[0].key, undefined);
  assert.equal(outputs[0].eventTime, undefined);
});

const CarrySpec = defineRelay({
  factoryId: 'steps_carry',
  steps: { each: 'chained' },
  channels: (c) => {
    c.input('in').into(StepsProducer).named('carry');
  },
});

test("a steps terminal nothing consumes hands its value to the core as the pass's carry", async () => {
  const result = await answer(
    activationFor('steps_carry')({ items: [inputItem(3)] }),
    depsFor(CarrySpec),
  );
  assert.deepEqual(
    result.effects[0].outputs.map((o) => [o.target, o.channel, o.consumer, payloadOf(o)]),
    [['edge', 1, 0, 3]],
  );
});

test("an edge payload is the encoder's own bytes, with nothing layered on top", async () => {
  const result = await answer(
    activationFor('steps_emit_stream')({ items: [inputItem(10)] }),
    depsFor(FlowTwinSpec),
  );
  const outputs = result.effects[0].outputs;
  for (const output of outputs.filter((o) => o.target === 'edge')) {
    assert.ok(output.payload instanceof Uint8Array);
    assert.equal(
      Buffer.isBuffer(output.payload),
      false,
      "msgpack's own bytes were copied into a Buffer on the way to the core",
    );
  }
  assert.deepEqual(outputs[1].payload, encodeValue(10));
});

test('every timer call inside a steps tasklet reaches the item effect as a timer op', async () => {
  const calls = {
    timerAfter: [(ctx) => ctx.timerAfter(1_000), 'processing_time', 2_000n, true],
    timerAtEventTime: [(ctx) => ctx.timerAtEventTime(5_000n), 'event_time', 5_000n, true],
    cancelTimer: [
      (ctx) => ctx.cancelTimer('processing_time', 2_000n),
      'processing_time',
      2_000n,
      false,
    ],
  };
  for (const [call, [arm, kind, scheduledAt, register]] of Object.entries(calls)) {
    const Arms = defineTasklet({
      name: 'arms',
      run: (_, ctx) => {
        arm(ctx);
      },
      onTimer: () => {},
    });
    const spec = defineRelay({
      factoryId: `steps_timer_${call}`,
      steps: {},
      channels: (c) => c.input('in').into(Arms),
    });
    const { effects } = await answer(
      activationFor(spec.factoryId)({ items: [inputItem(1)] }),
      depsFor(spec),
    );
    assert.deepEqual(effects[0].timers, [{ kind, scheduledAt, register }], call);
  }
});

test('a timer call inside a compensate hook fails naming runtime.timers_unavailable', async () => {
  const Undo = defineTasklet({
    name: 'undo',
    run: () => {},
    compensate: (_outputs, ctx) => {
      ctx.timerAfter(1_000);
    },
  });
  const spec = defineRelay({
    factoryId: 'steps_timer_compensate',
    steps: {},
    channels: (c) => c.input('in').into(Undo),
  });
  const result = await answer(
    activationFor(spec.factoryId)({ kind: 'compensate_run', outputs: [] }),
    depsFor(spec),
  );
  assert.equal(result.outcome, 'failed');
  assert.match(result.error, /runtime\.timers_unavailable: /);
});

test('the same graph on a flow class releases one output per consumer', async () => {
  const result = await answer(
    activationFor('steps_emit_stream')({ items: [inputItem(10)] }),
    depsFor(FlowTwinSpec),
  );
  const outputs = result.effects[0].outputs;
  assert.deepEqual(
    outputs.map((o) => [o.target, o.consumer, payloadOf(o)]),
    [
      ['edge', 1, 11],
      ['edge', 2, 10],
      ['plugin', undefined, 10],
    ],
  );
});

// The other end of the same two arms: what the CONSUMER is handed. A stream
// producer already applied the edge, so delivery applies nothing; a steps relay
// stage released the value raw, so delivery is the one application.

/** Hand `payload` to `tasklet` on the `handoff` channel and answer what it saw. */
async function delivered(spec, tasklet, payload) {
  const { effects, handed } = await ran(spec, {
    tasklet,
    items: [inputItem(0, { channel: 1, payload })],
  });
  assert.equal(effects[0].verdict, 'ok');
  return handed;
}

test('a flow inter-task edge applies its map exactly once', async () => {
  const produced = await answer(
    activationFor('steps_emit_stream')({ items: [inputItem(10)] }),
    depsFor(FlowTwinSpec),
  );
  const mapped = produced.effects[0].outputs[0];
  assert.deepEqual(await delivered(FlowTwinSpec, 1, mapped.payload), [11]);
});

test('a steps consumer applies its own edge exactly once, at delivery', async () => {
  const stage = await answer(
    activationFor('steps_emit')({ items: [inputItem(10)] }),
    depsFor(StepsSpec),
  );
  const released = stage.effects[0].outputs[0].payload;
  // One released value reaches BOTH children — the driver's fan-out — and each
  // evaluates its own edge here.
  assert.deepEqual(await delivered(StepsSpec, 1, released), [11]);
  assert.deepEqual(await delivered(StepsSpec, 2, released), [10]);
});

// `ctx.complete` rides the item's effect on either kind: which kind runs the
// tasklet is the core's to judge, so the effect is built the same way.

/** Completes with `first` and then with its input, except on `stay`; echoes every input. */
const Completing = defineTasklet({
  name: 'completing',
  run: (input, ctx) => {
    ctx.emit(input);
    if (input === 'stay') return;
    ctx.complete('first');
    ctx.complete(input);
  },
});

const completingGraph = (c) => c.input('in').into(Completing).output('out');

for (const [run, settings] of [
  ['steps', { steps: {} }],
  ['flow', {}],
]) {
  test(`a ${run} tasklet's complete rides its effect, the last call winning`, async () => {
    const spec = defineRelay({
      factoryId: `complete_${run}`,
      ...settings,
      channels: completingGraph,
    });
    const { effects } = await answer(
      activationFor(spec.factoryId)({ items: [inputItem('bye'), inputItem('stay')] }),
      depsFor(spec),
    );
    assert.deepEqual(
      effects.map((effect) => effect.verdict),
      ['ok', 'ok'],
    );
    assert.deepEqual(effects[0].complete, Buffer.from('bye'), 'text rides as UTF-8');
    assert.equal(effects[1].complete, undefined, 'an item that never completes carries none');
  });
}

// An entry naming no codec reads JSON, so plain text fails its decode: the
// item is reported unreadable (the entrance's failure, not the handler's), so
// the core parks it under the entry channel, and the handler is never handed it.
test('plain text into an entry naming no codec is unreadable and never reaches the handler', async () => {
  const spec = defineRelay({
    factoryId: 'json_entry',
    channels: (c) => {
      c.input('in').into(record('json_entry_tasklet'));
    },
  });
  const { effects, handed } = await ran(spec, {
    cellKey: undefined,
    items: [inputItem({ a: 1 }), inputItem(0, { payload: Buffer.from('hello') })],
  });
  assert.deepEqual(
    effects.map((effect) => effect.verdict),
    ['ok', 'unreadable'],
  );
  assert.match(effects[1].message, /not valid JSON/);
  assert.deepEqual(handed, [{ a: 1 }]);
});

test('bytes that are not UTF-8 into a text entry are unreadable', async () => {
  const spec = defineRelay({
    factoryId: 'text_entry',
    channels: (c) => {
      c.input('in', { codec: 'text' }).into(record('text_entry_tasklet'));
    },
  });
  const { effects, handed } = await ran(spec, {
    cellKey: undefined,
    items: [inputItem(0, { payload: Buffer.from([0xff, 0xfe]) })],
  });
  assert.equal(effects[0].verdict, 'unreadable');
  assert.match(effects[0].message, /not valid UTF-8/);
  assert.deepEqual(handed, []);
});

// An entry edge's transform runs at the entrance too, so its failure is the
// entrance's; a handler throwing on a value that did decode stays a handler
// failure, which is what the retry ladder is for.
test('an entry edge transform that throws is unreadable, a handler that throws is not', async () => {
  const spec = defineRelay({
    factoryId: 'entry_edge',
    channels: (c) => {
      c.input('in', { codec: 'text' })
        .map((text) => {
          if (text === 'poison') throw new Error('the mapping rejected it');
          return text;
        })
        .into(
          defineTasklet({
            name: 'entry_edge_tasklet',
            run: (input) => {
              if (input === 'boom') throw new Error('the handler rejected it');
            },
          }),
        );
    },
  });
  const { effects: mapped } = await ran(spec, {
    cellKey: undefined,
    items: [inputItem(0, { payload: Buffer.from('poison') })],
  });
  assert.equal(mapped[0].verdict, 'unreadable');
  assert.equal(mapped[0].message, 'Error: the mapping rejected it');
  const { effects: thrown } = await ran(spec, {
    cellKey: undefined,
    items: [inputItem(0, { payload: Buffer.from('boom') })],
  });
  assert.equal(thrown[0].verdict, 'retryable');
  assert.match(thrown[0].message, /the handler rejected it/);
});

// --- merged sources ---------------------------------------------------------
//
// Where the test app's own `merged_input` and `transform_chain` carry the shape,
// they are what runs. Both output, as text, what their tasklet returned for
// each value it ran.

const textItem = (channel, text) => inputItem(0, { channel, payload: Buffer.from(text) });

/** One text payload on channel 0 handed to `cellKey`; `item` overrides the item's fields. */
const ranText = (spec, cellKey, text, item = {}) =>
  ran(spec, { cellKey, items: [{ ...textItem(0, text), ...item }] });

/** The text each output of `effects` carried, in order. */
const sunk = (effects) =>
  effects.flatMap((effect) =>
    effect.outputs.map((output) => Buffer.from(output.payload).toString()),
  );

test('each merged source reaches the handler under its own mapping, in the order handed', async () => {
  const { effects } = await ran(fixtures.mergedInput, {
    cellKey: 'AAPL',
    items: [textItem(0, 'AAPL:1'), textItem(1, 'AAPL:2'), textItem(0, 'AAPL:3')],
  });
  assert.equal(effects.length, 3);
  assert.deepEqual(sunk(effects), ['ticks:AAPL:1', 'quotes:AAPL:2', 'ticks:AAPL:3']);
});

test('every merged source is keyed on its mapped value at the crossing', async () => {
  for (const channel of [0, 1]) {
    assert.deepEqual(await keysAt(fixtures.mergedInput, channel, 'MSFT:9'), [['MSFT']]);
  }
});

test('a tasklet merged with an entry releases its mapped value to the merged node', async () => {
  const spec = defineRelay({
    factoryId: 'merged_hop',
    channels: (c) => {
      const produced = c.input('raw').into(StepsProducer);
      c.merge()
        .from(produced, (value) => ({ produced: value }))
        .from(c.input('direct'), (value) => ({ direct: value }))
        .into(record('merged_hop_record'));
    },
  });
  const { effects } = await ran(spec, { cellKey: undefined, items: [inputItem(7)] });
  assert.deepEqual(
    effects[0].outputs.map((output) => [output.consumer, decodeValue(output.payload)]),
    [[1, { produced: 7 }]],
  );
});

// --- flatMap ----------------------------------------------------------------
//
// One input, several values: the handler runs once per value under the one
// input, the crossing answers every value's key, and a producer releases one
// frame per value.

const FanSpec = defineRelay({
  factoryId: 'fan',
  channels: (c) => {
    c.input('lines', { codec: 'text' })
      .flatMap((line) => line.split(',').filter(Boolean))
      .into(record('fan_record'));
  },
});

test('a fan of three runs the handler three times under one effect', async () => {
  const { effects, handed } = await ranText(FanSpec, undefined, 'a,b,c', { consumedOffset: 4n });
  assert.deepEqual(handed, ['a', 'b', 'c']);
  assert.deepEqual(
    effects.map((effect) => [effect.verdict, effect.consumedOffset]),
    [['ok', 4n]],
  );
});

test('a fan of none is a drop: the input is consumed and reaches no handler', async () => {
  const { effects, handed } = await ranText(FanSpec, undefined, '', { consumedOffset: 5n });
  assert.deepEqual(handed, []);
  assert.deepEqual(
    effects.map((effect) => [effect.verdict, effect.consumedOffset]),
    [['ok', 5n]],
  );
});

test('a fan followed by keyBy answers every key at the crossing, once each', async () => {
  assert.deepEqual(await keysAt(fixtures.transformChain, 0, 'a:1,b:2,c:3,a:4'), [['A', 'B', 'C']]);
});

// The core hands the whole payload to every key the crossing answered.
test('a cell handed a fanned payload runs only the values keyed to it', async () => {
  const { effects } = await ranText(fixtures.transformChain, 'A', 'a:1,b:2,c:3,a:4');
  assert.deepEqual(sunk(effects), ['A:1', 'A:4']);
});

test('a fan into a static consumer runs each value on the cells it names, and [] on none', async () => {
  const keys = ['a', 'b'];
  const cellsFor = (trade) => keys.filter((key) => trade.startsWith(`${key}:`));
  const spec = defineRelay({
    factoryId: 'static_fan',
    channels: (c) => {
      c.input('trades', { codec: 'text' })
        .flatMap((line) => line.split(','))
        .into(record('static_fan_record', { cells: { keys, cellsFor } }));
    },
  });
  const payload = 'a:1,b:2,z:3,a:4';
  assert.deepEqual((await ranText(spec, 'a', payload)).handed, ['a:1', 'a:4']);
  assert.deepEqual((await ranText(spec, 'b', payload)).handed, ['b:2']);
  assert.deepEqual(await keysAt(spec, 0, payload), [['a', 'b']]);
});

test('a producer releases one frame per fanned value, and an array from map is one value', async () => {
  const released = async (factoryId, reshape) => {
    const spec = defineRelay({
      factoryId,
      channels: (c) =>
        reshape(c.input('in').into(StepsProducer)).into(record(`${factoryId}_record`)),
    });
    const { effects } = await ran(spec, { cellKey: undefined, items: [inputItem(10)] });
    return effects[0].outputs.map((output) => decodeValue(output.payload));
  };
  assert.deepEqual(
    await released('fanned', (edge) => edge.flatMap((value) => [value, value + 1])),
    [10, 11],
  );
  assert.deepEqual(await released('mapped', (edge) => edge.map((value) => [value, value + 1])), [
    [10, 11],
  ]);
});

// --- registering a factory --------------------------------------------------

test('a module exporting short forms registers each, with the run it declares', async () => {
  const dir = tempDir('flezha-short-');
  const module = join(dir, 'short.mjs');
  writeFileSync(
    module,
    `import { defineRelay } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const upcase = defineRelay({ factoryId: 'short_form', output: 'out', run: (i) => i });\n` +
      `export const place = defineRelay({ factoryId: 'short_steps', steps: {}, output: 'placed', input: 'order', run: (i) => i });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.equal(registry.get('short_form')?.spec.run, undefined);
  assert.deepEqual(registry.get('short_steps')?.spec.run, { steps: {} });
});

test('registerFactory rejects at the line that names a module which cannot load', async () => {
  const runner = createRunner('register_factory_ns');
  await assert.rejects(
    () => runner.registerFactory('./no_such_module_anywhere.mjs'),
    (error) => error.message.includes('no_such_module_anywhere'),
  );
});

test('registerFactory rejects a module exporting no relay, before run()', async () => {
  const dir = tempDir('flezha-empty-register-');
  const module = join(dir, 'empty.mjs');
  writeFileSync(module, 'export const nothing = 1;\n');
  const runner = createRunner('register_factory_empty_ns');
  await assert.rejects(
    () => runner.registerFactory(module),
    (error) => error.code === 'topology.no_relay_exported' && error.details.module === module,
  );
});

test('unregisterFactory undoes a pre-run registration and names an unknown one', async () => {
  const dir = tempDir('flezha-unregister-');
  const module = join(dir, 'one.mjs');
  writeFileSync(
    module,
    `import { defineRelay } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const one = defineRelay({ factoryId: 'unregister_me', output: 'out', run: (i) => i });\n`,
  );
  const runner = createRunner('unregister_ns');
  await runner.registerFactory(module);
  await runner.unregisterFactory(module);
  await assert.rejects(
    () => runner.unregisterFactory(module),
    (error) => error.code === 'runtime.class_not_registered' && error.details.factory === module,
  );
});

test('a factory changed after start is served, then refused as unregistered', async () => {
  const dir = tempDir('flezha-late-');
  const module = join(dir, 'late.mjs');
  writeFileSync(
    module,
    `import { defineRelay } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const late = defineRelay({ factoryId: 'late_form', output: 'out', run: (i) => i });\n`,
  );

  // A started runner is one that holds a core and a registry. Standing those in
  // is what lets the post-start branch run with no controller anywhere.
  const declared = [];
  const undeclared = [];
  const runner = createRunner('late_ns');
  runner.registry = new Registry();
  runner.native = {
    declareClass(bytes) {
      declared.push(JSON.parse(Buffer.from(bytes).toString()).factory_id);
    },
    async undeclareClass(factoryId) {
      undeclared.push(factoryId);
    },
  };
  const deps = { registry: runner.registry, bridge: fixture().bridge };
  const late = activationFor('late_form');

  await runner.registerFactory(module);
  assert.deepEqual(declared, ['late_form'], 'the class reached the core');
  const served = await answer(late({ cellKey: undefined, items: [inputItem('hi')] }), deps);
  assert.deepEqual(
    served.effects.map((effect) => effect.verdict),
    ['ok'],
  );

  await runner.unregisterFactory(module);
  assert.deepEqual(undeclared, ['late_form'], 'the removal reached the core');
  const refused = await answer(
    late({ token: 2n, cellKey: undefined, items: [inputItem('hi')] }),
    deps,
  );
  assert.equal(refused.outcome, 'failed');
  assert.equal(refused.error, 'no class registered for factory `late_form`');
  assert.equal(refused.nonRetryable, true);
});

test('an unregister the core refuses for one relay leaves the whole module registered', async () => {
  const dir = tempDir('flezha-late-refused-');
  const module = join(dir, 'pair.mjs');
  writeFileSync(
    module,
    `import { defineRelay } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const idle = defineRelay({ factoryId: 'pair_idle', output: 'out', run: (i) => i });\n` +
      `export const live = defineRelay({ factoryId: 'pair_live', output: 'out', run: (i) => i });\n`,
  );
  const declared = [];
  const runner = createRunner('late_refused_ns');
  runner.registry = new Registry();
  runner.native = {
    declareClass(bytes) {
      declared.push(JSON.parse(Buffer.from(bytes).toString()).factory_id);
    },
    async undeclareClass(factoryId) {
      if (factoryId === 'pair_live') {
        throw Object.assign(new Error('in use'), { code: 'runtime.relay_type_in_use' });
      }
      declared.splice(declared.indexOf(factoryId), 1);
    },
  };
  await runner.registerFactory(module);

  await assert.rejects(
    () => runner.unregisterFactory(module),
    (error) => error.code === 'runtime.relay_type_in_use',
  );
  assert.deepEqual(declared.sort(), ['pair_idle', 'pair_live']);
  assert.ok(runner.registry.get('pair_idle'), 'the idle relay is still served');
  assert.deepEqual(runner.specifiers, [module]);
});

test('stableId matches the cross-language vector', () => {
  const identity = Buffer.from('instance/op/key');
  assert.equal(stableId(identity, 42n, 0), 'bbf81fd4955afd1724203700a240e822');
  assert.equal(stableId(identity, 42n, 1), 'bbf81fd4965afd1724203700a240e95d');
  assert.equal(stableId(Buffer.alloc(0), 0n, 0), '15915375576d2f389a938dc3a9940c9d');
});
