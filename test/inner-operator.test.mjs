// An operator inside an operator, run locally: a flow class whose keyed edge
// ends in a steps class runs one stepped instance of it per key, each with its
// own state, and the instance's exit output continues on the outer graph; a
// flow class is inlined under prefixed names.

import assert from 'node:assert/strict';
import test from 'node:test';
import { LocalOperator, defineOperator, defineTasklet } from '../dist/esm/index.js';

const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');

/** One turn of a session: `key:text` answered as `key#turns:text`. */
const Turn = defineTasklet({
  name: 'turn',
  initState: () => ({ turns: 0 }),
  applyChange: (state, change) => {
    state.turns += change.by;
  },
  run: (line, ctx) => {
    const [key, text] = line.split(':');
    ctx.change({ by: 1 });
    ctx.emit(`${key}#${ctx.state.turns}:${text}`);
  },
});

const Reply = defineTasklet({ name: 'reply', run: (line, ctx) => ctx.emit(line) });
const Trim = defineTasklet({ name: 'trim', run: (line, ctx) => ctx.emit(line.trim()) });
const Upper = defineTasklet({ name: 'upper', run: (line, ctx) => ctx.emit(line.toUpperCase()) });

const Session = defineOperator({
  factoryId: 'inner_session_node',
  steps: { each: 'per_input' },
  durable: false,
  channels: (c) => {
    c.input('turns', { codec: 'text' }).into(Turn).named('answer');
  },
});

const Gateway = defineOperator({
  factoryId: 'inner_gateway_node',
  channels: (c) => {
    c.input('requests', { codec: 'text' })
      .keyBy((line) => line.split(':')[0])
      .into(Session, { maxCells: 2 })
      .into(Reply)
      .sink('out', { codec: 'text' });
  },
});

const Cleanup = defineOperator({
  factoryId: 'inner_cleanup_node',
  channels: (c) => {
    c.input('raw', { codec: 'text' }).into(Trim).into(Upper);
  },
});

const Inlining = defineOperator({
  factoryId: 'inner_inlining_node',
  channels: (c) => {
    c.input('requests', { codec: 'text' }).into(Cleanup).into(Reply).sink('out', { codec: 'text' });
  },
});

const TwoEntries = defineOperator({
  factoryId: 'inner_two_entries_node',
  channels: (c) => {
    c.input('a').into(Trim);
    c.input('b').into(Upper);
  },
});

const StepsOuter = defineOperator({
  factoryId: 'inner_steps_outer_node',
  steps: { each: 'per_input' },
  durable: false,
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(Session).into(Reply);
  },
});

async function run(spec, lines, count) {
  const released = [];
  const local = LocalOperator.start(spec);
  try {
    local.onSink('out', (output) => released.push(utf8(output.payload)));
    for (const line of lines) await local.feed('requests', line);
    await local.runUntilIdle(5_000);
  } finally {
    await local.close();
  }
  assert.equal(released.length, count, JSON.stringify(released));
  return released.sort();
}

test('every key runs its own scope and the exit continues the outer graph', async () => {
  assert.deepEqual(await run(Gateway, ['alice:hi', 'bob:yo', 'alice:again'], 3), [
    'alice#1:hi',
    'alice#2:again',
    'bob#1:yo',
  ]);
});

test('a third key past maxCells is dropped', async () => {
  assert.deepEqual(await run(Gateway, ['alice:hi', 'bob:yo', 'carol:hey'], 2), [
    'alice#1:hi',
    'bob#1:yo',
  ]);
});

test('a flow class is inlined under prefixed names', async () => {
  assert.deepEqual(await run(Inlining, ['  hi  '], 1), ['HI']);
  assert.deepEqual(Inlining.tasklets.map((node) => node.name).sort(), [
    'inner_cleanup_node.trim',
    'inner_cleanup_node.upper',
    'reply',
  ]);
  assert.ok(Inlining.tasklets.every((node) => node.embeds === undefined));
});

test('an inlined class with two entries and no input is refused naming both', () => {
  assert.throws(
    () =>
      defineOperator({
        factoryId: 'inner_ambiguous_entry_node',
        channels: (c) => {
          c.input('in').into(TwoEntries).into(Reply);
        },
      }),
    (error) =>
      error.code === 'topology.embedded_entry_ambiguous' &&
      error.details.inner === 'inner_two_entries_node' &&
      error.details.channels === 'a, b',
  );
});

test('a steps class embedding a steps class is refused naming both', () => {
  assert.throws(
    () => LocalOperator.start(StepsOuter),
    (error) =>
      error.code === 'topology.nested_steps' &&
      String(error.message).includes('inner_steps_outer_node') &&
      String(error.message).includes('inner_session_node'),
  );
});

test('a value a tasklet emits enters the scope in its entry codec', async () => {
  const AfterATasklet = defineOperator({
    factoryId: 'inner_after_a_tasklet_node',
    channels: (c) => {
      c.input('requests', { codec: 'text' })
        .into(Trim)
        .keyBy((line) => line.split(':')[0])
        .into(Session)
        .into(Reply)
        .sink('out', { codec: 'text' });
    },
  });
  assert.deepEqual(await run(AfterATasklet, ['  alice:hi  '], 1), ['alice#1:hi']);
});

test('a sink right after an embedded class is refused', () => {
  const SinkAfter = defineOperator({
    factoryId: 'inner_sink_after_node',
    channels: (c) => {
      c.input('requests', { codec: 'text' }).into(Session).sink('out', { codec: 'text' });
    },
  });
  assert.throws(
    () => LocalOperator.start(SinkAfter),
    (error) => error.code === 'topology.invalid_setting' && error.details.value === 'out',
  );
});
