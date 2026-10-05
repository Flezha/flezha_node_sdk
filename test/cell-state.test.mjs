// A cell belongs to one relay.
//
// One registered class serves every relay of its factory on a runner, such
// as each instance of a steps relay or each partition of a stream, and
// each has cells of its own under the same tasklet and cell key. Every
// dispatch path that reads or writes a cell's state is driven here with two
// relays side by side. The harness keeps one cell of its own, so its
// restore is driven for one relay only, to show it fills the cell it reads.
import assert from 'node:assert/strict';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  decodeValue,
  defineTasklet,
  defineRelay,
  encodeValue,
  loadNative,
} from '../dist/esm/index.js';
import { answer } from '../dist/esm/dispatch.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

import { activationFor, depsFor, inputItem, jsonOf } from './dispatch-fixtures.mjs';

// The relays are judged by the core, as a runner's are, before any answer.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

/** Keeps what it was sent, and undoes by emitting what its own cell holds. */
const Kept = defineTasklet({
  name: 'kept',
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change);
  },
  run: (value, ctx) => ctx.change(value),
  compensate: (_outputs, ctx) => ctx.emit([...ctx.state.seen]),
});

const KeptRelay = defineRelay({
  factoryId: 'kept_relay',
  channels: (c) => c.input('input').into(Kept).output('out', { codec: 'msgpack' }),
});

const activation = activationFor('kept_relay');

function relayOf(deps) {
  const ask = (kind, relay, extra = {}) => answer(activation({ kind, relay, ...extra }), deps);
  return {
    ask,
    async work(relay, value) {
      const result = await ask('work', relay, { items: [inputItem(value)] });
      assert.equal(result.effects[0].verdict, 'ok', result.effects[0].message);
    },
    async read(relay) {
      return jsonOf((await ask('live_state_read', relay)).json);
    },
  };
}

test('two relays of one factory keep their own cell', async () => {
  const cells = relayOf(depsFor(KeptRelay));
  await cells.work('order_a', 'a');
  await cells.work('order_b', 'b');
  await cells.work('order_a', 'c');

  assert.deepEqual(await cells.read('order_a'), { seen: ['a', 'c'] });
  assert.deepEqual(await cells.read('order_b'), { seen: ['b'] });
});

test('a restore and an applied state reach only their own relay', async () => {
  const cells = relayOf(depsFor(KeptRelay));
  const restored = await cells.ask('restore_fold', 'order_a', {
    changes: [{ bytes: encodeValue('r') }],
  });
  const applied = await cells.ask('apply_state', 'order_b', {
    stateJson: Buffer.from('{"seen":["m"]}'),
  });

  assert.equal(restored.outcome, 'restored');
  assert.equal(applied.outcome, 'state_applied');
  assert.deepEqual(await cells.read('order_a'), { seen: ['r'] });
  assert.deepEqual(await cells.read('order_b'), { seen: ['m'] });
  assert.deepEqual(await cells.read('order_c'), { seen: [] });
});

test("a snapshot and a migration serialize read their own relay's cell", async () => {
  const cells = relayOf(depsFor(KeptRelay));
  await cells.work('order_a', 'a');
  await cells.work('order_b', 'b');

  const snapshot = await cells.ask('snapshot_request', 'order_b');
  const migrated = await cells.ask('migration_serialize', 'order_b');

  assert.deepEqual(decodeValue(snapshot.state), { seen: ['b'] });
  assert.deepEqual(jsonOf(migrated.json), { seen: ['b'] });
});

test("a compensation reads its own relay's cell", async () => {
  const cells = relayOf(depsFor(KeptRelay));
  await cells.work('order_a', 'a');
  await cells.work('order_b', 'b');

  const result = await cells.ask('compensate_run', 'order_b', { outputs: [] });

  assert.deepEqual(decodeValue(result.effects[0].outputs[0].payload), ['b']);
});

test('a harness restore fills the cell it reads', async () => {
  const harness = TaskletTestHarness.withPayload(
    Kept,
    { changes: [encodeValue('r')] },
    { relay: 'order_a' },
  );

  assert.deepEqual(harness.currentState, { seen: ['r'] });
});
