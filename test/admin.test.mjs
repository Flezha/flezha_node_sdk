// Starting an operative from application code: the request a start sends, and
// the answer it reads back.
import assert from 'node:assert/strict';
import test from 'node:test';

import { Admin } from '../dist/esm/index.js';

/** An addon whose one admin call is recorded and answered with `answer`. */
function answering(answer) {
  const calls = [];
  const native = {
    adminCall: async (verb, args) => {
      calls.push([verb, JSON.parse(args)]);
      if (answer instanceof Error) throw answer;
      return JSON.stringify(answer);
    },
  };
  return { admin: new Admin(native), calls };
}

test('a start without an id sends none and answers the generated id', async () => {
  const { admin, calls } = answering({ id: 'echo-k7m2qzxw4pd3fn2a', outcome: 'started' });
  const started = await admin.startOperative('echo', { kind: 'stream' });
  assert.deepEqual(calls, [['Operative.start', [{ factory_id: 'echo', kind: 'stream' }]]]);
  assert.deepEqual(started, { id: 'echo-k7m2qzxw4pd3fn2a', outcome: 'started' });
});

test('a start with an id sends it and answers only the id and the outcome', async () => {
  const { admin, calls } = answering({ id: 'my_echo', outcome: 'already_running', node: 'n1' });
  const started = await admin.startOperative('echo', { id: 'my_echo' });
  assert.deepEqual(calls, [['Operative.start', [{ factory_id: 'echo', id: 'my_echo' }]]]);
  assert.deepEqual(started, { id: 'my_echo', outcome: 'already_running' });
});

test('a refused start rejects the way every refused call does', async () => {
  const refusal = new Error('quota exceeded: max_operatives');
  const { admin } = answering(refusal);
  await assert.rejects(admin.startOperative('echo'), refusal);
});
