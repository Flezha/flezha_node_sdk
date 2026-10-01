// The one tasklet form: output only through the context, the tasklet's own
// settings as fields of its definition, the cell mode read from the wiring, and
// the short form as a tasklet's fields.
import assert from 'node:assert/strict';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  decodeValue,
  defineTasklet,
  defineOperator,
  encodeValue,
  loadNative,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import { answer } from '../dist/esm/dispatch.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

import {
  activationFor,
  codesOf,
  depsFor,
  inputItem,
  jsonOf,
  refusal,
} from './dispatch-fixtures.mjs';

const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');

/** One operator wiring `tasklet` from `in` to the sink `out`. */
const piped = (factoryId, tasklet, settings = {}) =>
  defineOperator({
    factoryId,
    ...settings,
    channels: (c) => c.input('in').into(tasklet).sink('out'),
  });

/** The effects `spec` answers for `items`, as its first tasklet's single cell. */
async function effectsOf(spec, items, overrides = {}) {
  const work = activationFor(spec.factoryId)({ cellKey: undefined, items, ...overrides });
  const result = await answer(work, depsFor(spec));
  assert.equal(result.outcome, 'work', result.error);
  for (const effect of result.effects) assert.equal(effect.verdict, 'ok', effect.message);
  return result.effects;
}

const declared = (spec) => toHostClassDecl(spec).tasklets;

// --- a value leaves only through the context -------------------------------

const TwiceThenNothing = defineTasklet({
  name: 'twice_then_nothing',
  run: (input, ctx) => {
    if (input !== 'first') return;
    ctx.emit('a');
    ctx.emit('b', { key: 'k' });
  },
});

test('a value leaves a tasklet only through the context, under the dispatcher', async () => {
  const [first, second] = await effectsOf(piped('twice', TwiceThenNothing), [
    inputItem('first'),
    inputItem('second'),
  ]);
  assert.deepEqual(
    first.outputs.map((output) => [utf8(output.payload), output.key]),
    [
      ['a', undefined],
      ['b', 'k'],
    ],
  );
  assert.deepEqual(second.outputs, []);
});

test('a value leaves a tasklet only through the context, under the harness', async () => {
  const harness = TaskletTestHarness.of(TwiceThenNothing);
  assert.deepEqual((await harness.input('first')).emits, ['a', 'b']);
  assert.deepEqual((await harness.input('second')).emits, []);
});

// Plain JavaScript runs no type checker, so each of these loads and runs.
const Returning = defineTasklet({
  name: 'returning',
  run: (input) => input.toUpperCase(),
  onTimer: (firing) => firing.scheduledAt,
  compensate: () => 'undone',
});

test('a value run, onTimer or compensate returns is ignored under the dispatcher', async () => {
  const spec = piped('returning_op', Returning, { steps: {} });
  const [input, timer] = await effectsOf(spec, [
    inputItem('hello'),
    { kind: 'timer', timerKind: 'processing_time', scheduledAt: 5n, now: 5n },
  ]);
  assert.deepEqual([input.outputs, timer.outputs], [[], []]);
  const undo = await answer(
    activationFor(spec.factoryId)({ cellKey: undefined, kind: 'compensate_run', outputs: [] }),
    depsFor(spec),
  );
  assert.deepEqual(undo.effects[0].outputs, []);
});

test('a value run returns is ignored under the harness', async () => {
  assert.deepEqual((await TaskletTestHarness.of(Returning).input('hello')).emits, []);
});

// --- the tasklet's settings are fields of its definition --------------------

const keyedInto = (tasklet) =>
  defineOperator({
    factoryId: `keyed_${tasklet.name}`,
    channels: (c) =>
      c
        .input('in')
        .keyBy((value) => String(value))
        .into(tasklet)
        .sink('out'),
  });

const emitting = { run: (input, ctx) => ctx.emit(input) };

test('the idle TTL is whole seconds, and the cell cap a field of its own', () => {
  const Sessions = defineTasklet({
    name: 'sessions',
    idleTtlSeconds: 3600,
    maxCells: 9,
    ...emitting,
  });
  const [node] = declared(keyedInto(Sessions));
  assert.deepEqual([node.idle_ttl_ms, node.max_cells], [3_600_000, 9]);
  assert.deepEqual(
    Object.keys(declared(piped('no_ttl', defineTasklet({ name: 'plain', ...emitting })))[0]).filter(
      (key) => key === 'idle_ttl_ms' || key === 'max_cells',
    ),
    [],
    'unset never evicts',
  );
});

test('an idle TTL that is not a positive whole number is refused, naming it', () => {
  for (const ttl of [0.2, 0, -5, '60']) {
    const error = refusal(() =>
      defineTasklet({ name: 'sessions', idleTtlSeconds: ttl, ...emitting }),
    );
    assert.equal(error.code, 'topology.invalid_setting', String(ttl));
    assert.equal(error.details.declaration, "tasklet 'sessions'");
    assert.equal(error.details.setting, 'idleTtlSeconds');
  }
});

test('the clock follows from what the definition writes', () => {
  const fold = { initState: () => ({ n: 0 }), applyChange: () => {} };
  const clocks = [
    defineTasklet({ name: 'event', eventTime: (trade) => trade.at, ...emitting }),
    defineTasklet({ name: 'logical', logicalClock: true, ...fold, ...emitting }),
    defineTasklet({ name: 'wall', ...emitting }),
  ].map((tasklet) => {
    const spec = piped(`clock_${tasklet.name}`, tasklet);
    validate(spec);
    return declared(spec)[0].clock;
  });
  assert.deepEqual(clocks, ['event_time', 'logical', 'wall']);
});

test('the event time an input carries is what the crossing answers', async () => {
  const Trades = defineTasklet({ name: 'trades', eventTime: (trade) => trade.at, ...emitting });
  const spec = keyedInto(Trades);
  const result = await answer(
    activationFor(spec.factoryId)({
      kind: 'key_request',
      keyRequest: {
        channel: 0,
        payloads: [Buffer.from(JSON.stringify({ at: 1_720_000_000_000 }))],
        wantKeys: false,
        wantEventTime: true,
      },
    }),
    depsFor(spec),
  );
  assert.equal(result.prepared[0].eventTime, 1_720_000_000_000n);
});

test('event time and the logical clock together are refused as two clocks', () => {
  const error = refusal(() =>
    defineTasklet({ name: 'both', eventTime: () => 0, logicalClock: true, ...emitting }),
  );
  assert.equal(error.code, 'topology.two_clocks');
  assert.deepEqual(error.details, { tasklet: 'both' });
});

test('the initial state comes from initState, with the cell key a keyed cell has', async () => {
  const Totals = defineTasklet({
    name: 'totals',
    initState: (cellKey) => ({ key: cellKey, total: 0 }),
    applyChange: () => {},
    ...emitting,
  });
  assert.deepEqual(TaskletTestHarness.of(Totals, { cellKey: 'A' }).currentState, {
    key: 'A',
    total: 0,
  });
  const read = await answer(
    activationFor('keyed_totals')({ cellKey: 'A', kind: 'live_state_read' }),
    depsFor(keyedInto(Totals)),
  );
  assert.deepEqual(jsonOf(read.json), { key: 'A', total: 0 });
  assert.deepEqual(TaskletTestHarness.of(Totals).currentState, { key: undefined, total: 0 });
});

test('a fold with no initial state is refused, naming the tasklet', () => {
  const error = refusal(() =>
    defineTasklet({ name: 'nowhere', applyChange: () => {}, ...emitting }),
  );
  assert.equal(error.code, 'topology.change_without_state');
  assert.deepEqual(error.details, { tasklet: 'nowhere' });
});

test('a change is plain data, folded live and folded again on restore', async () => {
  const Events = defineTasklet({
    name: 'events',
    initState: () => ({ last: 0, count: 0 }),
    applyChange: (state, change) => {
      state.last = change.at;
      state.count += 1;
    },
    run: (at, ctx) => ctx.change({ kind: 'event', at }),
  });
  const live = TaskletTestHarness.of(Events);
  const { changes } = await live.input(5);
  assert.deepEqual(changes, [{ kind: 'event', at: 5 }]);
  live.replayEquals();

  const restored = TaskletTestHarness.withPayload(Events, { changes: changes.map(encodeValue) });
  assert.deepEqual(restored.currentState, live.currentState);
});

test('a retired or unknown tasklet field is refused, naming it and the tasklet', () => {
  for (const [field, value, now] of [
    ['dynamicCells', {}, 'keyBy'],
    ['clock', 'logical', 'logicalClock'],
    ['initialState', () => ({}), 'initState'],
    ['idleTtlMs', 1_000, undefined],
    ['applyChanges', () => {}, undefined],
  ]) {
    const error = refusal(() => defineTasklet({ name: 'old_form', [field]: value, ...emitting }));
    assert.equal(error.code, 'topology.unknown_field');
    assert.equal(error.details.declaration, "tasklet 'old_form'");
    assert.ok(error.details.fields.includes(`\`${field}\``), error.message);
    if (now) assert.ok(error.details.fields.includes(now), error.message);
  }
});

test('a retired hook a definition inherits is refused as well', () => {
  class Legacy {
    name = 'legacy';
    run(input, ctx) {
      ctx.emit(input);
    }
    initialState() {
      return { n: 0 };
    }
  }
  const error = refusal(() => defineTasklet(new Legacy()));
  assert.equal(error.code, 'topology.unknown_field');
  assert.ok(error.details.fields.includes('`initialState`'), error.message);
});

test('a retired field on a short form is refused, naming it', () => {
  const error = refusal(() =>
    defineOperator({ factoryId: 'old_short', sink: 'out', dynamicCells: {}, ...emitting }),
  );
  assert.equal(error.code, 'topology.unknown_field');
  assert.ok(error.details.fields.includes('`dynamicCells`'), error.message);
});

// --- the cell mode is read from the wiring -----------------------------------

const Customers = defineTasklet({
  name: 'totals',
  initState: () => ({ total: 0 }),
  applyChange: (state, change) => {
    state.total += change.by;
  },
  run: (order, ctx) => ctx.change({ by: order.amount }),
});

test('keyBy alone makes a tasklet dynamic, with a cell per key', async () => {
  const spec = defineOperator({
    factoryId: 'orders',
    channels: (c) =>
      c
        .input('orders')
        .keyBy((order) => order.customer)
        .into(Customers),
  });
  assert.equal(declared(spec)[0].cell_mode, 'dynamic');

  const deps = depsFor(spec);
  const cellOf = activationFor('orders');
  for (const [customer, amount] of [
    ['ann', 3],
    ['bob', 4],
    ['ann', 5],
  ]) {
    await answer(cellOf({ cellKey: customer, items: [inputItem({ customer, amount })] }), deps);
  }
  const totals = [];
  for (const customer of ['ann', 'bob']) {
    const read = await answer(cellOf({ cellKey: customer, kind: 'live_state_read' }), deps);
    totals.push(jsonOf(read.json).total);
  }
  assert.deepEqual(totals, [8, 4]);
});

test('a keyed merge makes its tasklet dynamic', () => {
  const spec = defineOperator({
    factoryId: 'detector',
    channels: (c) =>
      c
        .merge()
        .from(c.input('a'), (value) => value)
        .from(c.input('b'), (value) => value)
        .keyBy((value) => value.symbol)
        .into(Customers),
  });
  assert.equal(declared(spec)[0].cell_mode, 'dynamic');
});

test('one definition takes its mode from each wiring', () => {
  const keyed = defineOperator({
    factoryId: 'keyed_orders',
    channels: (c) =>
      c
        .input('orders')
        .keyBy((order) => order.customer)
        .into(Customers),
  });
  const single = defineOperator({
    factoryId: 'all_orders',
    channels: (c) => c.input('orders').into(Customers),
  });
  assert.deepEqual(
    [declared(keyed)[0].cell_mode, declared(single)[0].cell_mode],
    ['dynamic', 'single'],
  );
});

test('a static key set fed by keyBy is refused as topology.key_by_on_unkeyable_target', () => {
  const error = refusal(() =>
    keyedInto(
      defineTasklet({
        name: 'fixed',
        cells: { keys: ['a', 'b'], cellsFor: () => ['a'] },
        ...emitting,
      }),
    ),
  );
  assert.ok(codesOf(error).includes('topology.key_by_on_unkeyable_target'), error.message);
  assert.match(error.message, /fixed/);
});

// --- the short form is a tasklet's fields --------------------------------------

test("a steps short form's emitted value reaches its sink", async () => {
  const spec = defineOperator({
    factoryId: 'place',
    steps: {},
    sink: 'placed',
    input: 'order',
    codec: 'text',
    run: (order, ctx) => ctx.emit(`placed ${order}`),
  });
  const [effect] = await effectsOf(spec, [{ ...inputItem(''), payload: Buffer.from('o-1') }]);
  assert.deepEqual(
    effect.outputs.map((output) => [output.pluginChannel, utf8(output.payload)]),
    [['placed', 'placed o-1']],
  );
});

test('a short form emits only through its context', async () => {
  const shortForm = (factoryId, run) =>
    defineOperator({ factoryId, sink: 'out', codec: 'text', run });
  const hello = [{ ...inputItem(''), payload: Buffer.from('hello') }];
  const [emitted] = await effectsOf(
    shortForm('upcase', (line, ctx) => ctx.emit(line.toUpperCase())),
    hello,
  );
  const [returned] = await effectsOf(
    shortForm('upcase_returning', (line) => line.toUpperCase()),
    hello,
  );
  assert.deepEqual(
    emitted.outputs.map((output) => utf8(output.payload)),
    ['HELLO'],
  );
  assert.deepEqual(returned.outputs, []);
});

test('a short form grown into its full form declares the same record and keeps its cells', async () => {
  const fields = {
    initState: () => ({ seen: [] }),
    applyChange: (state, change) => {
      state.seen.push(change);
    },
    run: (line, ctx) => ctx.change(line),
  };
  const short = defineOperator({ factoryId: 'counter', input: 'ticks', codec: 'text', ...fields });
  const full = defineOperator({
    factoryId: 'counter',
    channels: (c) =>
      c.input('ticks', { codec: 'text' }).into(defineTasklet({ name: 'counter', ...fields })),
  });
  assert.deepEqual(toHostClassDecl(full), toHostClassDecl(short));

  const cell = activationFor('counter');
  const before = depsFor(short);
  const { effects } = await answer(
    cell({ cellKey: undefined, items: [{ ...inputItem(''), payload: Buffer.from('tick') }] }),
    before,
  );
  const after = depsFor(full);
  await answer(
    cell({
      cellKey: undefined,
      kind: 'restore_fold',
      changes: effects[0].changes.map((bytes) => ({ bytes })),
    }),
    after,
  );
  const read = await answer(cell({ cellKey: undefined, kind: 'live_state_read' }), after);
  assert.deepEqual(jsonOf(read.json), { seen: ['tick'] });
  assert.deepEqual(decodeValue(effects[0].changes[0]), 'tick');
});

// The tasklet's declared name is where its state is filed: a short form's is
// its factory id, verbatim.
test("a tasklet's durable identity is its declared name", () => {
  const short = defineOperator({
    factoryId: 'order_executor',
    steps: {},
    sink: 'out',
    ...emitting,
  });
  assert.deepEqual(
    declared(short).map((node) => node.name),
    ['order_executor'],
  );
  assert.deepEqual(
    declared(piped('named', defineTasklet({ name: 'track_prices', ...emitting }))).map(
      (node) => node.name,
    ),
    ['track_prices'],
  );
});

// A short form's fields are read as a tasklet's are, along the prototype chain.
class InheritedShortForm {
  factoryId = 'inherited';
  sink = 'out';
  run(value, ctx) {
    ctx.emit({ value, state: ctx.state });
  }
  initState() {
    return { n: 1 };
  }
}

class InheritedHook {
  factoryId = 'inherited_hook';
  sink = 'out';
  run = (value, ctx) => ctx.emit(ctx.state);
  initState() {
    return { n: 1 };
  }
}

test('a short form keeps a hook it inherits, as it keeps its own', () => {
  const { definition } = defineOperator(new InheritedHook()).tasklets[0].tasklet;
  assert.equal(typeof definition.initState, 'function', 'initState reached the tasklet');
  assert.deepEqual(definition.initState(), { n: 1 });
});

test('a short form keeps a run it inherits', async () => {
  const { tasklet } = defineOperator(new InheritedShortForm()).tasklets[0];
  assert.deepEqual((await TaskletTestHarness.of(tasklet).input('v')).emits, [
    { value: 'v', state: { n: 1 } },
  ]);
});
