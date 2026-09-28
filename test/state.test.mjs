// Declared state is mutated only through declared changes.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineTasklet, StateMutationError } from '../dist/esm/index.js';
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
  assert.deepEqual((await harness.input(3)).emits, [3]);
  assert.deepEqual((await harness.input(4)).emits, [7]);
  assert.equal(harness.currentState.total, 7);
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
    () => harness.input(1),
    (error) =>
      error instanceof StateMutationError &&
      error.message.includes('state.total') &&
      error.message.includes('ctx.change'),
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
    () => harness.input(1),
    (error) => error.message.includes('state.tags.last'),
  );
});

test('applyChange retains its ability to mutate', async () => {
  const harness = TaskletTestHarness.of(Counter);
  await harness.input(5);
  harness.replayEquals();
  assert.equal(harness.currentState.total, 5);
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
    () => TaskletTestHarness.of(Deleter).input(1),
    (error) => error instanceof StateMutationError,
  );
});
