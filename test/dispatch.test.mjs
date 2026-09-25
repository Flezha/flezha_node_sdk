// One activation, one answer — and the guard that makes it exactly one.
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  DeclarationError,
  decodeValue,
  defineUnbounded,
  defineTasklet,
  defineBounded,
  encodeValue,
  loadNative,
} from '../dist/esm/index.js';
import { Registry, answer, dispatch } from '../dist/esm/dispatch.js';
import { createRunner, importModule, loadRegistry, specsFrom } from '../dist/esm/runner.js';

import { activationFor, depsFor, inputItem, jsonOf } from './dispatch-fixtures.mjs';
import { testAppModule } from './test-app.mjs';

// The handout-contract tests below answer against the core itself, so the
// refusal they assert is the core's own. Nothing starts a runner, so no
// controller connection is attempted.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

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

const Counter = defineTasklet({
  name: 'counter',
  dynamicCells: {},
  initialState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.by;
  },
  run: (input, ctx) => {
    ctx.change({ by: input });
    return ctx.state.total;
  },
});

const Spec = defineUnbounded({
  factoryId: 'counters',
  channels: (c) => {
    c.input('in')
      .keyBy((value) => String(value))
      .into(Counter)
      .keyed()
      .sink('out');
  },
});

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
    run: (input) => {
      if (input === 'boom') throw new Error('handler exploded');
      return input;
    },
  });
  const spec = defineUnbounded({
    factoryId: 'throwing_op',
    channels: (c) => c.input('in').into(Throwing).sink(),
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
  const spec = defineUnbounded({
    factoryId: 'rejecting_op',
    channels: (c) => c.input('in').into(Rejecting).sink(),
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
  const serialized = await answer(activation({ kind: 'migration_serialize' }), deps);

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
    initialState: () => ({ offset: 0n, blob: new Uint8Array() }),
    applyChange: (state, change) => {
      state.offset = change.offset;
      state.blob = change.blob;
    },
    run: () => undefined,
  });
  const spec = defineUnbounded({
    factoryId: 'wide_op',
    channels: (c) => c.input('in').into(Wide).sink(),
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
    activation({ factory: 'wide_op', cellKey: undefined, kind: 'migration_serialize' }),
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

test('one payload failing its prepare does not fail the others', async () => {
  const Windowed = defineTasklet({
    name: 'windowed',
    dynamicCells: {},
    clock: 'event_time',
    eventTime: (input) => {
      if (input.at === undefined) throw new Error('no event time on this payload');
      return BigInt(input.at);
    },
    run: (input) => input,
  });
  const spec = defineUnbounded({
    factoryId: 'windowed_op',
    channels: (c) =>
      c
        .input('in')
        .keyBy((v) => v.k)
        .into(Windowed)
        .keyed()
        .sink(),
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
  assert.match(reported, /unknown activation token 42/);
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
  assert.match(reported, /unknown activation token 13/);
});

test('a module specifier is imported and its operators registered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-specifier-'));
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineUnbounded, defineTasklet } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const Echo = defineTasklet({ name: 'echo', run: (i) => i });\n` +
      `export default defineUnbounded({ factoryId: 'from_specifier', channels: (c) => c.input('in').into(Echo).sink() });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.ok(registry.get('from_specifier'), "the module's operator was registered");
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
    (sdk) => `import { defineUnbounded, defineTasklet } from '${sdk}';`,
    'export default',
  ],
  [
    'cjs',
    createRequire(import.meta.url)('../dist/cjs/runner.js').loadRegistry,
    'handlers.cjs',
    (sdk) => `const { defineUnbounded, defineTasklet } = require('${sdk}');`,
    'module.exports =',
  ],
]) {
  test(`the ${build} build imports a handler module by relative path`, async () => {
    const module = join(mkdtempSync(join(tmpdir(), 'flezha-specifier-')), file);
    writeFileSync(
      module,
      `${bind(new URL(`../dist/${build}/index.js`, import.meta.url).pathname)}\n` +
        `const Echo = defineTasklet({ name: 'echo', run: (i) => i });\n` +
        `${exporting} defineUnbounded({ factoryId: 'by_relative_path', channels: (c) => c.input('in').into(Echo).sink() });\n`,
    );
    const registry = await load([`./${relative(process.cwd(), module)}`]);
    assert.ok(registry.get('by_relative_path'), "the module's operator was registered");
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
  const dir = mkdtempSync(join(tmpdir(), 'flezha-module-scope-'));
  const module = join(dir, 'handlers.mjs');
  writeFileSync(
    module,
    `import { defineUnbounded, defineTasklet } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `const A = defineTasklet({ name: 'twin', run: (i) => i });\n` +
      `const B = defineTasklet({ name: 'twin', run: (i) => i });\n` +
      `export default defineUnbounded({ factoryId: 'module_scope', channels: (c) => {\n` +
      `  const s = c.input('in'); s.into(A); s.into(B);\n` +
      `} });\n`,
  );
  await assert.rejects(
    () => loadRegistry([module]),
    (error) =>
      error instanceof DeclarationError && error.refusals.includes('NodeIdentityCollision'),
  );
});

test('a module that throws anything but a refusal is named, with the original error chained', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-broken-'));
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

test('a module exporting no operator is refused naming the specifier', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-empty-'));
  const module = join(dir, 'empty.mjs');
  writeFileSync(module, 'export const nothing = 1;\n');
  await assert.rejects(
    () => loadRegistry([module]),
    (error) => error.message.includes(module) && error.message.includes('exports no operator'),
  );
});

test('a duplicate factory id fails at the second declaration, naming it', () => {
  const registry = new Registry();
  registry.add(Spec);
  assert.throws(
    () => registry.add(Spec),
    (error) => error.message.startsWith('DuplicateFactoryId') && error.message.includes('counters'),
  );
});

// --- output identity stamps -------------------------------------------------
//
// One invocation, several emits: the per-emit ordinal is the half a single-emit
// assertion cannot see, and the e2e golden checks stamp NAMES only.

const StampEmitter = defineTasklet({
  name: 'stamp_emitter',
  run: (input, ctx) => {
    if (input === 'twice') {
      ctx.emitTo('keyed_out', 'a');
      ctx.emitTo('keyed_out', 'b');
    } else {
      ctx.emitTo('plain_out', 'p');
      ctx.emitTo('keyed_out', 'k');
    }
  },
});

// One keyed plugin channel: the topology every SDK can express, and where the
// per-emit ordinal has to be right.
const StampSpec = defineUnbounded({
  factoryId: 'stamps',
  channels: (c) => {
    c.input('in')
      .keyBy(() => 'ek')
      .into(StampEmitter)
      .keyed()
      .sink('keyed_out');
  },
});

// A tasklet owns ONE output channel, so a keyed and a non-keyed plugin
// channel are two tasklets. The unkeyed one carries no stamps at all.
const PlainEmitter = defineTasklet({
  name: 'plain_emitter',
  run: (_input, ctx) => {
    ctx.emitTo('plain_out', 'p');
  },
});

const MixedSpec = defineUnbounded({
  factoryId: 'stamps_mixed',
  channels: (c) => {
    c.input('in')
      .keyBy(() => 'ek')
      .branch([
        (edge) => edge.into(PlainEmitter).sink('plain_out'),
        (edge) => edge.into(StampEmitter).keyed().sink('keyed_out'),
      ]);
  },
});

const stampActivation = activationFor('stamps');
const mixedActivation = activationFor('stamps_mixed');
const metaOf = (output) => Object.fromEntries(output.metadata.map((m) => [m.key, m.value]));

test('a keyed plugin emit carries the identity stamps, and the ordinal advances per emit', async () => {
  const result = await answer(
    stampActivation({ cellKey: 'ek', changeSeq: 5n, items: [inputItem('twice')] }),
    depsFor(StampSpec),
  );
  assert.equal(result.outcome, 'work');
  const outputs = result.effects[0].outputs;
  assert.equal(outputs.length, 2);
  assert.deepEqual(metaOf(outputs[0]), {
    'flz.cell': 'op-1/stamp_emitter/ek',
    'flz.seq': '5.0',
    'flz.key': 'ek',
  });
  assert.deepEqual(metaOf(outputs[1]), {
    'flz.cell': 'op-1/stamp_emitter/ek',
    'flz.seq': '5.1',
    'flz.key': 'ek',
  });
});

test('a non-keyed plugin emit carries no stamps', async () => {
  const result = await answer(
    mixedActivation({ cellKey: 'ek', changeSeq: 5n, items: [inputItem('mixed')] }),
    depsFor(MixedSpec),
  );
  const [plain] = result.effects[0].outputs;
  assert.equal(plain.pluginChannel, 'plain_out');
  assert.deepEqual(plain.metadata, []);
});

test('a fresh work item restarts the emit ordinal', async () => {
  const result = await answer(
    stampActivation({
      cellKey: 'ek',
      changeSeq: 5n,
      items: [inputItem('twice'), inputItem('twice')],
    }),
    depsFor(StampSpec),
  );
  assert.deepEqual(
    result.effects.map((effect) => effect.outputs.map((o) => metaOf(o)['flz.seq'])),
    [
      ['5.0', '5.1'],
      ['5.0', '5.1'],
    ],
  );
});

test('user metadata survives beside the stamps and cannot spoof one', async () => {
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
  const spec = defineUnbounded({
    factoryId: 'stamps_user',
    channels: (c) => {
      c.input('in')
        .keyBy(() => 'ek')
        .into(UserMeta)
        .keyed()
        .sink();
    },
  });
  const activation = activationFor('stamps_user');

  const ok = await answer(
    activation({ cellKey: 'ek', changeSeq: 2n, items: [inputItem('plain')] }),
    depsFor(spec),
  );
  assert.deepEqual(metaOf(ok.effects[0].outputs[0]), {
    user: 'meta',
    'flz.cell': 'op-1/stamp_user_meta/ek',
    'flz.seq': '2.0',
    'flz.key': 'ek',
  });

  const spoofed = await answer(
    activation({ cellKey: 'ek', changeSeq: 2n, items: [inputItem('spoof')] }),
    depsFor(spec),
  );
  assert.equal(spoofed.effects[0].verdict, 'retryable');
});

// --- the structured-bounded emit arm ---------------------------------------
//
// One released value is ONE edge output on a bounded class, untransformed and
// unkeyed, naming the producing channel and its first consumer: the driver
// fans it out from the journaled handoff. On an unbounded class the same topology
// releases one output per consumer, transformed and keyed. The plugin output
// rides alongside either way.

const BoundedProducer = defineTasklet({
  name: 'bounded_producer',
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

const boundedGraph = (c) => {
  const edge = c.input('in').into(BoundedProducer).named('handoff');
  edge.map((value) => value + 1).into(record('bounded_b'));
  edge.into(record('bounded_c'));
  edge.sink();
};

/**
 * An edge payload is the class codec's MessagePack; the `handoff` sink names no
 * codec, so its frame for a number is JSON.
 */
const payloadOf = (output) =>
  output.target === 'plugin' ? jsonOf(output.payload) : decodeValue(output.payload);

const BoundedSpec = defineBounded({ factoryId: 'bounded_emit', channels: boundedGraph });
const UnboundedTwinSpec = defineUnbounded({ factoryId: 'bounded_emit_stream', channels: boundedGraph });

test('a structured bounded releases one untransformed edge output per emit', async () => {
  const result = await answer(
    activationFor('bounded_emit')({ items: [inputItem(10)] }),
    depsFor(BoundedSpec),
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

test("an edge payload is the encoder's own bytes, with nothing layered on top", async () => {
  const result = await answer(
    activationFor('bounded_emit_stream')({ items: [inputItem(10)] }),
    depsFor(UnboundedTwinSpec),
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

test('every timer call inside a bounded tasklet reaches the item effect as a timer op', async () => {
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
    const spec = defineBounded({
      factoryId: `bounded_timer_${call}`,
      channels: (c) => c.input('in').into(Arms),
    });
    const { effects } = await answer(
      activationFor(spec.factoryId)({ items: [inputItem(1)] }),
      depsFor(spec),
    );
    assert.deepEqual(effects[0].timers, [{ kind, scheduledAt, register }], call);
  }
});

test('a timer call inside a compensate hook fails naming TimersUnavailable', async () => {
  const Undo = defineTasklet({
    name: 'undo',
    run: () => {},
    compensate: (_outputs, ctx) => ctx.timerAfter(1_000),
  });
  const spec = defineBounded({
    factoryId: 'bounded_timer_compensate',
    channels: (c) => c.input('in').into(Undo),
  });
  const result = await answer(
    activationFor(spec.factoryId)({ kind: 'compensate_run', outputs: [] }),
    depsFor(spec),
  );
  assert.equal(result.outcome, 'failed');
  assert.match(result.error, /TimersUnavailable: undo: timerAfter inside compensate/);
});

test('the same graph on an unbounded class releases one output per consumer', async () => {
  const result = await answer(
    activationFor('bounded_emit_stream')({ items: [inputItem(10)] }),
    depsFor(UnboundedTwinSpec),
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
// producer already applied the edge, so delivery applies nothing; a bounded operator
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

test('an unbounded inter-task edge applies its map exactly once', async () => {
  const produced = await answer(
    activationFor('bounded_emit_stream')({ items: [inputItem(10)] }),
    depsFor(UnboundedTwinSpec),
  );
  const mapped = produced.effects[0].outputs[0];
  assert.deepEqual(await delivered(UnboundedTwinSpec, 1, mapped.payload), [11]);
});

test('a bounded consumer applies its own edge exactly once, at delivery', async () => {
  const stage = await answer(
    activationFor('bounded_emit')({ items: [inputItem(10)] }),
    depsFor(BoundedSpec),
  );
  const released = stage.effects[0].outputs[0].payload;
  // One released value reaches BOTH children — the driver's fan-out — and each
  // evaluates its own edge here.
  assert.deepEqual(await delivered(BoundedSpec, 1, released), [11]);
  assert.deepEqual(await delivered(BoundedSpec, 2, released), [10]);
});

// `ctx.complete` rides the item's effect on either kind: which kind runs the
// tasklet is the core's to judge, so the effect is built the same way.

/** Completes with `first` and then with its input, except on `stay`; echoes every input. */
const Completing = defineTasklet({
  name: 'completing',
  run: (input, ctx) => {
    if (input === 'stay') return input;
    ctx.complete('first');
    ctx.complete(input);
    return input;
  },
});

const completingGraph = (c) => c.input('in').into(Completing).sink('out');

for (const [kind, define] of [
  ['bounded', defineBounded],
  ['unbounded', defineUnbounded],
]) {
  test(`a ${kind} tasklet's complete rides its effect, the last call winning`, async () => {
    const spec = define({ factoryId: `complete_${kind}`, channels: completingGraph });
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
  const spec = defineUnbounded({
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
  const spec = defineUnbounded({
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
  const spec = defineUnbounded({
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
// they are what runs. Both sink, as text, what their tasklet returned for
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
  const spec = defineUnbounded({
    factoryId: 'merged_hop',
    channels: (c) => {
      const produced = c.input('raw').into(BoundedProducer);
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

const FanSpec = defineUnbounded({
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
  const spec = defineUnbounded({
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
    const spec = defineUnbounded({
      factoryId,
      channels: (c) =>
        reshape(c.input('in').into(BoundedProducer)).into(record(`${factoryId}_record`)),
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

test('a module exporting short forms registers each, of the kind its function names', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-short-'));
  const module = join(dir, 'short.mjs');
  writeFileSync(
    module,
    `import { defineUnbounded, defineBounded } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const upcase = defineUnbounded({ factoryId: 'short_form', sink: 'out', run: (i) => i });\n` +
      `export const place = defineBounded({ factoryId: 'short_bounded', sink: 'placed', input: 'order', run: (i) => i });\n`,
  );
  const registry = await loadRegistry([module]);
  assert.equal(registry.get('short_form')?.spec.kind, 'unbounded');
  assert.equal(registry.get('short_bounded')?.spec.kind, 'bounded');
});

test('registerFactory rejects at the line that names a module which cannot load', async () => {
  const runner = createRunner('register_factory_ns');
  await assert.rejects(
    () => runner.registerFactory('./no_such_module_anywhere.mjs'),
    (error) => error.message.includes('no_such_module_anywhere'),
  );
});

test('registerFactory rejects a module exporting no operator, before run()', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-empty-register-'));
  const module = join(dir, 'empty.mjs');
  writeFileSync(module, 'export const nothing = 1;\n');
  const runner = createRunner('register_factory_empty_ns');
  await assert.rejects(
    () => runner.registerFactory(module),
    (error) => error.message.includes(module) && error.message.includes('exports no operator'),
  );
});

test('unregisterFactory undoes a pre-run registration and names an unknown one', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-unregister-'));
  const module = join(dir, 'one.mjs');
  writeFileSync(
    module,
    `import { defineUnbounded } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const one = defineUnbounded({ factoryId: 'unregister_me', sink: 'out', run: (i) => i });\n`,
  );
  const runner = createRunner('unregister_ns');
  await runner.registerFactory(module);
  await runner.unregisterFactory(module);
  await assert.rejects(
    () => runner.unregisterFactory(module),
    (error) => error.message.includes(module) && error.message.includes('not registered'),
  );
});

test('a post-start registerFactory declares against the core and the workers', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'flezha-late-'));
  const module = join(dir, 'late.mjs');
  writeFileSync(
    module,
    `import { defineUnbounded } from '${new URL('../dist/esm/index.js', import.meta.url).pathname}';\n` +
      `export const late = defineUnbounded({ factoryId: 'late_form', sink: 'out', run: (i) => i });\n`,
  );

  // A started runner is one that holds a core and a registry. Standing those in
  // is what lets the post-start branch run with no controller anywhere.
  const declared = [];
  const runner = createRunner('late_ns');
  runner.registry = new Registry();
  runner.native = {
    declareClass(bytes) {
      declared.push(JSON.parse(Buffer.from(bytes).toString()).factory_id);
    },
  };

  await runner.registerFactory(module);

  assert.deepEqual(declared, ['late_form'], 'the class reached the core');
  assert.ok(runner.registry.get('late_form'), 'and the registry the dispatch loops serve from');

  await assert.rejects(
    () => runner.unregisterFactory(module),
    (error) => error.message.includes('cannot stop serving a started declaration'),
  );
});
