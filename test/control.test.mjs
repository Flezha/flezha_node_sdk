// Starting a relay from application code: the request a start sends, and
// the answer it reads back.
import assert from 'node:assert/strict';
import test from 'node:test';

import { Control, FlezhaError, Runner, loadNative } from '../dist/esm/index.js';

/** An addon whose one control call is recorded and answered with `answer`. */
function answering(answer) {
  const calls = [];
  const native = {
    adminCall: async (verb, args) => {
      calls.push([verb, JSON.parse(args)]);
      if (answer instanceof Error) throw answer;
      return JSON.stringify(answer);
    },
  };
  return { control: new Control(native), native, calls };
}

test('a start without an id sends none and answers the generated id', async () => {
  const { control, calls } = answering({ id: 'echo-k7m2qzxw4pd3fn2a', outcome: 'started' });
  const started = await control.startRelay('echo');
  assert.deepEqual(calls, [['Relay.start', [{ factory_id: 'echo' }]]]);
  assert.deepEqual(started, { id: 'echo-k7m2qzxw4pd3fn2a', outcome: 'started' });
});

test('a start with an id sends it and answers only the id and the outcome', async () => {
  const { control, calls } = answering({ id: 'my_echo', outcome: 'already_running', node: 'n1' });
  const started = await control.startRelay('echo', { id: 'my_echo' });
  assert.deepEqual(calls, [['Relay.start', [{ factory_id: 'echo', id: 'my_echo' }]]]);
  assert.deepEqual(started, { id: 'my_echo', outcome: 'already_running' });
});

test('a plugin is enabled by name, and bound and unbound by relay', async () => {
  const { control, calls } = answering(null);
  await control.enablePlugin('writer', 'relay_file_output', { opts: { path: 'out.txt' } });
  await control.enablePlugin('feed', 'relay_websocket', {
    single: true,
    relay_id: 'my_echo',
    hooks: [['on_output', {}]],
  });
  await control.bindPlugin('writer', 'my_echo');
  await control.bindPlugin('writer', 'my_other', { hooks: ['on_output'], channel: 'in' });
  await control.unbindPlugin('writer', 'my_echo');
  await control.disablePlugin('writer');
  await control.listPlugins();
  assert.deepEqual(calls, [
    [
      'PluginManager.enable',
      ['writer', { opts: { path: 'out.txt' }, provider: 'relay_file_output' }],
    ],
    [
      'PluginManager.enable',
      [
        'feed',
        {
          opts: {},
          single: true,
          relay_id: 'my_echo',
          hooks: [['on_output', {}]],
          provider: 'relay_websocket',
        },
      ],
    ],
    ['PluginManager.bind', ['writer', { relay_id: 'my_echo' }]],
    [
      'PluginManager.bind',
      ['writer', { hooks: ['on_output'], channel: 'in', relay_id: 'my_other' }],
    ],
    ['PluginManager.unbind', ['writer', { relay_id: 'my_echo' }]],
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
        message: "the namespace's relays quota is reached (3 of 3)",
        details: { kind: 'relays', current: 3, limit: 3 },
        hint: 'flz namespace show',
      },
    }),
  );
  const { control } = answering(refusal);
  await assert.rejects(control.startRelay('echo'), refusal);
});

// The addon's own failure reaches the caller as the one error, carrying the
// core's code: here the call made before the runner has a context.
test('an administration call the core refuses rejects with its code', async () => {
  await assert.rejects(
    new Control(loadNative()).listRelays(),
    (error) => error instanceof FlezhaError && error.code === 'runtime.runner_not_started',
  );
});

/** A runner over `native`, as one is after `run()` loaded the addon. */
function runnerOver(native) {
  const runner = new Runner('default');
  runner.native = native;
  return runner;
}

test('a send hands its options to the core and reads the answer', async () => {
  const calls = [];
  const native = {
    sendMessage: async (relay, channel, message, msgId, key, local) => {
      calls.push([relay, channel, Buffer.from(message).toString(), msgId, key, local]);
      return local ? 'null' : JSON.stringify({ to: 'prices#p1', verdict: 'absorbed' });
    },
  };
  const runner = runnerOver(native);
  await runner.send('my_echo', 'hello', 'in');
  const verdict = await runner.send('prices', 'x', 'in', { msgId: 'fx/0/7', key: 'EURUSD' });
  assert.equal(await runner.send('orders', 'hé', 'in', { local: true, msgId: null }), null);
  assert.deepEqual(calls, [
    ['my_echo', 'in', 'hello', null, null, false],
    ['prices', 'in', 'x', 'fx/0/7', 'EURUSD', false],
    ['orders', 'in', 'hé', null, null, true],
  ]);
  assert.deepEqual(verdict, { to: 'prices#p1', verdict: 'absorbed' });
});

// Through the real addon: no runner holds `elsewhere`, so the core refuses it.
test('the core refuses a local send to a relay held elsewhere, or with an id', async () => {
  const native = loadNative();
  native.contextNew(native.schemaVersion());
  try {
    await assert.rejects(
      runnerOver(native).send('elsewhere', 'x', 'in', { local: true }),
      (error) => error instanceof FlezhaError && error.code === 'runtime.relay_not_local',
    );
    await assert.rejects(
      runnerOver(native).send('orders', 'x', 'in', { local: true, msgId: 'm-1' }),
      (error) =>
        error instanceof FlezhaError &&
        error.code === 'usage.options_conflict' &&
        error.details.other === 'msg_id',
    );
    assert.deepEqual(runnerOver(native).relays(), []);
  } finally {
    native.contextFree();
  }
});
