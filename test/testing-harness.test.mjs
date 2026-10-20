// The tasklet test harness, and the replay-equality gate it exists for.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineTasklet, encodeValue } from '../dist/esm/index.js';
import { TaskletTestHarness, ReplayMismatch } from '../dist/esm/testing.js';

const Window = defineTasklet({
  name: 'rolling_window',
  initState: () => ({ samples: [] }),
  applyChange: (state, change) => {
    if (change.push !== undefined) state.samples.push(change.push);
    if (change.popFront !== undefined) state.samples.splice(0, change.popFront);
  },
  run: (value, ctx) => {
    ctx.change({ push: value });
    const over = ctx.state.samples.length - 3;
    if (over > 0) ctx.change({ popFront: over });
    if (ctx.state.samples.length < 3) return;
    ctx.emit(ctx.state.samples.reduce((a, b) => a + b, 0) / 3);
  },
});

test('a defined handler runs with no runner and no controller', async () => {
  // Nothing here starts a runner or loads the native binary: a handler is an
  // ordinary function, and this is the proof of it.
  const emitted = [];
  await Window.definition.run(3, {
    state: { samples: [1, 2, 3] },
    change() {},
    now: () => 0n,
    emit: (value) => emitted.push(value),
  });
  assert.deepEqual(emitted, [2]);
});

test('inputs, emits and the recorded change log are inspectable', async () => {
  const harness = TaskletTestHarness.of(Window, { cellKey: 'AAPL' });
  assert.deepEqual((await harness.feed(3)).emits, []);
  assert.deepEqual((await harness.feed(6)).emits, []);
  assert.deepEqual((await harness.feed(9)).emits, [6]);
  assert.deepEqual((await harness.feed(12)).emits, [9]);
  assert.deepEqual(harness.state.samples, [6, 9, 12]);
  assert.deepEqual(harness.recordedChanges, [
    { push: 3 },
    { push: 6 },
    { push: 9 },
    { push: 12 },
    { popFront: 1 },
  ]);
  harness.replayEquals();
});

test('the replay gate reports both states when the fold does not rebuild one', async () => {
  // A handler cannot drift by mutating state directly — the guard refuses
  // that. What is left, and what this gate is for here, is an `applyChange`
  // that is not a pure function of (state, change): the live fold and the
  // replay fold then disagree, and only a restart would otherwise show it.
  let folds = 0;
  const Impure = defineTasklet({
    name: 'impure_fold',
    initState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      folds += 1;
      state.total += change.by * folds;
    },
    run: (value, ctx) => {
      ctx.change({ by: value });
      return undefined;
    },
  });
  const harness = TaskletTestHarness.of(Impure);
  await harness.feed(2);
  await harness.feed(2);
  assert.throws(
    () => harness.replayEquals(),
    (error) =>
      error instanceof ReplayMismatch &&
      error.message.includes('live:') &&
      error.message.includes('replayed:') &&
      error.message.includes('impure_fold'),
  );
});

test('a timer fires with ctx.now() at its scheduled time', async () => {
  const Session = defineTasklet({
    name: 'session',
    initState: () => ({ closedAt: null }),
    applyChange: (state, change) => {
      state.closedAt = change.closedAt;
    },
    run: (_input, ctx) => {
      ctx.timerAfter(5_000n);
    },
    onTimer: (firing, ctx) => {
      assert.equal(ctx.now(), firing.scheduledAt);
      ctx.change({ closedAt: ctx.now() });
    },
  });
  const harness = TaskletTestHarness.of(Session, { now: 1_000n });
  const applied = await harness.feed('hello');
  assert.deepEqual(applied.timers, [
    { kind: 'processing_time', scheduledAt: 6_000n, register: true },
  ]);
  await harness.fireTimer('processing_time', 6_000n);
  assert.equal(harness.state.closedAt, 6_000n);
  harness.replayEquals();
});

test('the clock the next input is served at can be advanced', async () => {
  const Clocked = defineTasklet({ name: 'clocked', run: (_i, ctx) => ctx.emit(ctx.now()) });
  const harness = TaskletTestHarness.of(Clocked, { now: 10n });
  assert.deepEqual((await harness.feed(1)).emits, [10n]);
  harness.advanceBy(5n);
  assert.deepEqual((await harness.feed(1)).emits, [15n]);
  harness.advanceTo(100n);
  assert.equal(harness.now, 100n);
});

test('a harness can be built from a restore payload', async () => {
  const harness = TaskletTestHarness.withPayload(Window, {
    snapshot: encodeValue({ samples: [1, 2] }),
    changes: [encodeValue({ push: 3 })],
  });
  assert.deepEqual(harness.state.samples, [1, 2, 3]);
  assert.deepEqual((await harness.feed(4)).emits, [3]);
});

test('a payload stamped at an older version reaches the declared migration', () => {
  const Migrating = defineTasklet({
    name: 'migrating',
    schemaVersion: 2,
    minReadableSchemaVersion: 1,
    migrateState: (from, bytes) => {
      assert.equal(from, 1);
      return { items: bytes.length, currency: 'EUR' };
    },
    initState: () => ({ items: 0, currency: 'EUR' }),
    applyChange: (state, change) => {
      state.items += change.by;
    },
    run: () => undefined,
  });
  const harness = TaskletTestHarness.withPayload(Migrating, {
    snapshot: new Uint8Array([1, 2, 3]),
    fromVersion: 1,
  });
  assert.deepEqual(harness.state, { items: 3, currency: 'EUR' });
});

test('stableId is distinct within an invocation and stable across a replay', async () => {
  const seen = [];
  const Ids = defineTasklet({
    name: 'ids',
    run: (_input, ctx) => {
      seen.push(ctx.stableId(), ctx.stableId());
    },
  });
  await TaskletTestHarness.of(Ids, { cellKey: 'k' }).feed(1);
  await TaskletTestHarness.of(Ids, { cellKey: 'k' }).feed(1);
  assert.notEqual(seen[0], seen[1], 'two calls in one invocation differ');
  assert.equal(seen[0], seen[2], 'the first call of a replayed invocation matches');
  assert.equal(seen[1], seen[3]);
  assert.match(seen[0], /^[0-9a-f]{32}$/);
});

test('a reserved metadata key is refused naming the key', async () => {
  const Meta = defineTasklet({
    name: 'meta',
    run: (_input, ctx) => {
      ctx.emit('v', { metadata: { 'flz.key': 'mine' } });
    },
  });
  await assert.rejects(
    () => TaskletTestHarness.of(Meta).feed(1),
    (error) => error.code === 'runtime.reserved_metadata_key' && error.details.key === 'flz.key',
  );
});

test('a string and an object output are reported exactly as emitted', async () => {
  // The harness reads its output back as MessagePack, so a string output is the
  // string rather than the bytes a plugin would receive.
  const Emitter = defineTasklet({
    name: 'emitter',
    run: (input, ctx) => {
      ctx.emit({ said: input });
      ctx.emit(`echo: ${input}`);
    },
  });
  const harness = TaskletTestHarness.of(Emitter);
  assert.deepEqual((await harness.feed('hi')).emits, [{ said: 'hi' }, 'echo: hi']);
});

test('a spawn answers a handle already resolved to a new instance', async () => {
  const Spawner = defineTasklet({
    name: 'spawner',
    run: async (input, ctx) =>
      ctx.emit([
        ctx.spawnSteps('worker', input).tryResult(),
        ctx.spawnSteps('worker', input).tryResult(),
        await ctx.spawnSteps('worker', input, { id: 'w1' }).wait(),
      ]),
  });
  const { emits } = await TaskletTestHarness.of(Spawner).feed('p');
  assert.deepEqual(emits, [
    [
      { id: 'worker-aaaaaaaaaaaaaaab', outcome: 'spawned' },
      { id: 'worker-aaaaaaaaaaaaaaac', outcome: 'spawned' },
      { id: 'w1', outcome: 'spawned' },
    ],
  ]);
});
