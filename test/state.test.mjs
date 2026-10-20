// Declared state is mutated only through declared changes.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineTasklet, FlezhaError } from '../dist/esm/index.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

const Counter = defineTasklet({
  name: 'counter',
  initState: () => ({ total: 0, tags: { last: '' } }),
  applyChange: (state, change) => {
    if (change.kind === 'add') state.total += change.by;
    if (change.kind === 'tag') state.tags.last = change.value;
  },
  run: (input, ctx) => {
    ctx.change({ kind: 'add', by: input });
    ctx.emit(ctx.state.total);
  },
});

test('a change folds live and the handler reads the folded state', async () => {
  const harness = TaskletTestHarness.of(Counter);
  assert.deepEqual((await harness.feed(3)).emits, [3]);
  assert.deepEqual((await harness.feed(4)).emits, [7]);
  assert.equal(harness.state.total, 7);
});

test('a direct assignment is refused naming the field and the change path', async () => {
  const Bad = defineTasklet({
    name: 'bad',
    initState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      state.total += change.by;
    },
    run: (_input, ctx) => {
      ctx.state.total = 99;
    },
  });
  const harness = TaskletTestHarness.of(Bad);
  await assert.rejects(
    () => harness.feed(1),
    (error) =>
      error instanceof FlezhaError &&
      error.code === 'runtime.state_mutated' &&
      error.details.tasklet === 'bad' &&
      error.details.path === 'total',
  );
});

test('the guard reaches a nested field, where the loss is the same', async () => {
  const Nested = defineTasklet({
    name: 'nested',
    initState: () => ({ tags: { last: '' } }),
    applyChange: (state, change) => {
      state.tags.last = change.value;
    },
    run: (_input, ctx) => {
      ctx.state.tags.last = 'x';
    },
  });
  const harness = TaskletTestHarness.of(Nested);
  await assert.rejects(
    () => harness.feed(1),
    (error) => error.code === 'runtime.state_mutated' && error.details.path === 'tags.last',
  );
});

test('applyChange retains its ability to mutate', async () => {
  const harness = TaskletTestHarness.of(Counter);
  await harness.feed(5);
  harness.replayEquals();
  assert.equal(harness.state.total, 5);
});

test('deleting a field is refused too', async () => {
  const Deleter = defineTasklet({
    name: 'deleter',
    initState: () => ({ total: 0 }),
    applyChange: (state, change) => {
      state.total += change.by;
    },
    run: (_input, ctx) => {
      delete ctx.state.total;
    },
  });
  await assert.rejects(
    () => TaskletTestHarness.of(Deleter).feed(1),
    (error) => error instanceof FlezhaError && error.code === 'runtime.state_mutated',
  );
});
