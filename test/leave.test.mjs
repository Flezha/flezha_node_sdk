// A leave: the core tells the host a cell, or a whole relay, left this runner.
import assert from 'node:assert/strict';
import test, { after } from 'node:test';

import { ACTIVATION_SCHEMA_VERSION, encodeValue } from '../dist/esm/index.js';
import { loadNative } from '../dist/esm/native.js';
import { answer } from '../dist/esm/dispatch.js';

import { Counters, activationFor, depsFor } from './dispatch-fixtures.mjs';

// The declarations are judged by the core, so a context is open; nothing starts a runner.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

const activation = activationFor('counters');

test('a cell that left starts again at its initial state, as does every cell of a relay that left', async () => {
  const deps = depsFor(Counters);
  const fold = { kind: 'restore_fold', changes: [{ bytes: Buffer.from(encodeValue({ by: 4 })) }] };
  const total = async (overrides) => {
    const read = await answer(activation({ kind: 'live_state_read', ...overrides }), deps);
    return JSON.parse(read.json.toString()).total;
  };
  await answer(activation(fold), deps);
  await answer(activation({ ...fold, cellKey: 'other' }), deps);

  const left = await answer(activation({ kind: 'leave', wholeRelay: false }), deps);

  assert.deepEqual(left, { token: 1n, outcome: 'state_applied' });
  assert.equal(await total({}), 0);
  assert.equal(await total({ cellKey: 'other' }), 4);
  await answer(activation({ kind: 'leave', wholeRelay: true }), deps);
  assert.equal(await total({ cellKey: 'other' }), 0);
});
