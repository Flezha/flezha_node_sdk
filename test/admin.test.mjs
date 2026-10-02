// Starting an operator from application code: the request a start sends, and
// the answer it reads back.
import assert from 'node:assert/strict';
import test from 'node:test';

import { Admin, FlezhaError, loadNative } from '../dist/esm/index.js';

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
  const started = await admin.startOperator('echo');
  assert.deepEqual(calls, [['Operator.start', [{ factory_id: 'echo' }]]]);
  assert.deepEqual(started, { id: 'echo-k7m2qzxw4pd3fn2a', outcome: 'started' });
});

test('a start with an id sends it and answers only the id and the outcome', async () => {
  const { admin, calls } = answering({ id: 'my_echo', outcome: 'already_running', node: 'n1' });
  const started = await admin.startOperator('echo', { id: 'my_echo' });
  assert.deepEqual(calls, [['Operator.start', [{ factory_id: 'echo', id: 'my_echo' }]]]);
  assert.deepEqual(started, { id: 'my_echo', outcome: 'already_running' });
});

test('a plugin is enabled by name, and bound and unbound by operator', async () => {
  const { admin, calls } = answering(null);
  await admin.enablePlugin('writer', 'operator_file_sink', { opts: { path: 'out.txt' } });
  await admin.enablePlugin('feed', 'operator_websocket', {
    single: true,
    operator_id: 'my_echo',
    hooks: [['on_output', {}]],
  });
  await admin.bindPlugin('writer', 'my_echo');
  await admin.bindPlugin('writer', 'my_other', { hooks: ['on_output'], channel: 'in' });
  await admin.unbindPlugin('writer', 'my_echo');
  await admin.disablePlugin('writer');
  await admin.listPlugins();
  assert.deepEqual(calls, [
    [
      'PluginManager.enable',
      ['writer', { opts: { path: 'out.txt' }, provider: 'operator_file_sink' }],
    ],
    [
      'PluginManager.enable',
      [
        'feed',
        {
          opts: {},
          single: true,
          operator_id: 'my_echo',
          hooks: [['on_output', {}]],
          provider: 'operator_websocket',
        },
      ],
    ],
    ['PluginManager.bind', ['writer', { operator_id: 'my_echo' }]],
    [
      'PluginManager.bind',
      ['writer', { hooks: ['on_output'], channel: 'in', operator_id: 'my_other' }],
    ],
    ['PluginManager.unbind', ['writer', { operator_id: 'my_echo' }]],
    ['PluginManager.disable', ['writer', {}]],
    ['PluginManager.list', []],
  ]);
});

test('a refused start rejects the way every refused call does', async () => {
  const refusal = FlezhaError.fromDocument(
    JSON.stringify({
      error: {
        code: 'quota.exceeded',
        class: 'conflict',
        message: "the namespace's operators quota is reached (3 of 3)",
        details: { kind: 'operators', current: 3, limit: 3 },
        hint: 'flz namespace show',
      },
    }),
  );
  const { admin } = answering(refusal);
  await assert.rejects(admin.startOperator('echo'), refusal);
});

// The addon's own failure reaches the caller as the one error, carrying the
// core's code: here the call made before the runner has a context.
test('an administration call the core refuses rejects with its code', async () => {
  await assert.rejects(
    new Admin(loadNative()).listOperators(),
    (error) => error instanceof FlezhaError && error.code === 'runtime.runner_not_started',
  );
});
