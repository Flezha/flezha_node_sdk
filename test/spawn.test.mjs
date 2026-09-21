// A tasklet's spawn: the handle it returns at once, and what the handle's
// reads answer. The core's own answers are stood in for by `spawnBridge`; the
// same reads against the real addon with no controller are in
// `local-mode.test.mjs`.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  SpawnAlreadyRunningError,
  SpawnError,
  SpawnFactoryUnknownError,
  SpawnRefusedError,
  SpawnTimeoutError,
  SpawnUnavailableError,
  decodeValue,
  loadNative,
} from '../dist/esm/index.js';

import { handleOnce, spawnBridge } from './dispatch-fixtures.mjs';

// `handleOnce` submits its class, so this file needs the process context.
loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const caught = (read) => {
  try {
    return read();
  } catch (error) {
    return error;
  }
};

/** Run `body` on one input and require the input to have been handled cleanly. */
async function handled(bridge, body, item) {
  const effect = await handleOnce(bridge, body, item);
  assert.equal(effect.verdict, 'ok', effect.message);
  return effect;
}

test('a spawn returns its handle at once, under the activation it runs in', async () => {
  const bridge = spawnBridge();
  let handle;
  await handled(bridge, (input, ctx) => {
    handle = ctx.spawnWorkflow('worker', { order: input });
  });
  assert.equal(typeof handle.then, 'undefined', 'the handle is not a promise');
  assert.deepEqual(bridge.reads, [], 'nothing was read that the tasklet did not ask for');
  const [{ payload, ...request }] = bridge.requests;
  assert.deepEqual(request, {
    token: 7n,
    factoryId: 'worker',
    id: '',
    ifRunning: '',
    replay: false,
  });
  assert.deepEqual(decodeValue(payload), { order: 'p' });
});

test('a spawn resolves to the id it resolved to, generated or given', async () => {
  const bridge = spawnBridge((request) => ({
    id: request.id || 'worker-k7m2qzxw4pd3fn2a',
    outcome: request.id ? 'delivered' : 'spawned',
  }));
  const seen = [];
  await handled(bridge, async (input, ctx) => {
    seen.push(await ctx.spawnWorkflow('worker', input).wait());
    seen.push(ctx.spawnWorkflow('worker', input, { id: 'w1' }).tryResult());
  });
  assert.deepEqual(seen, [
    { id: 'worker-k7m2qzxw4pd3fn2a', outcome: 'spawned' },
    { id: 'w1', outcome: 'delivered' },
  ]);
  assert.deepEqual(
    bridge.requests.map((request) => [request.id, request.ifRunning]),
    [
      ['', ''],
      ['w1', ''],
    ],
  );
});

test('the error policy is sent, and a running id is thrown by both reads', async () => {
  const bridge = spawnBridge((request) => ({
    id: request.id,
    error: 'already_running',
    message: 'an instance of `w1` is already running and if_running was Error',
  }));
  const failures = [];
  await handled(bridge, async (input, ctx) => {
    const handle = ctx.spawnWorkflow('worker', input, { id: 'w1', ifRunning: 'error' });
    failures.push(
      await handle.wait().catch((error) => error),
      caught(() => handle.tryResult()),
    );
  });
  assert.equal(bridge.requests[0].ifRunning, 'error');
  const [waited, tried] = failures;
  assert.ok(waited instanceof SpawnAlreadyRunningError && waited instanceof SpawnError);
  assert.equal(waited.name, 'SpawnAlreadyRunningError');
  assert.equal(waited.id, 'w1');
  assert.deepEqual(tried, waited, 'every read answers the resolved failure');
  assert.deepEqual(bridge.reads, ['wait', 'try']);
});

test('ifRunning without an id throws a TypeError naming both, and sends nothing', async () => {
  const bridge = spawnBridge();
  let thrown;
  await handled(bridge, (input, ctx) => {
    thrown = caught(() => ctx.spawnWorkflow('worker', input, { ifRunning: 'deliver' }));
  });
  assert.ok(thrown instanceof TypeError, String(thrown));
  assert.match(thrown.message, /ifRunning without an id/);
  assert.deepEqual(bridge.requests, []);
});

test('each failure the core answers is thrown as its own class', async () => {
  const classes = {
    already_running: SpawnAlreadyRunningError,
    factory_unknown: SpawnFactoryUnknownError,
    refused: SpawnRefusedError,
    timeout: SpawnTimeoutError,
    unavailable: SpawnUnavailableError,
  };
  for (const [kind, Class] of Object.entries(classes)) {
    const message = `the core's sentence for ${kind}`;
    const bridge = spawnBridge(() => ({ id: '', error: kind, message }));
    let failure;
    await handled(bridge, async (input, ctx) => {
      failure = await ctx
        .spawnWorkflow('worker', input)
        .wait()
        .catch((error) => error);
    });
    assert.ok(failure instanceof Class, `${kind} was thrown as ${failure?.name}`);
    assert.equal(failure.message, message);
  }
});

test('a failure nobody reads faults nothing and leaves no promise to reject', async () => {
  const rejections = [];
  const onRejection = (reason) => rejections.push(reason);
  process.on('unhandledRejection', onRejection);
  try {
    const bridge = spawnBridge(() => ({ id: '', error: 'refused', message: 'refused: quota' }));
    const effect = await handled(bridge, (input, ctx) => {
      ctx.spawnWorkflow('worker', input);
      ctx.emit(input);
    });
    assert.equal(effect.outputs.length, 1, 'the emit still ships');
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(rejections, []);
    assert.deepEqual(bridge.reads, []);
  } finally {
    process.off('unhandledRejection', onRejection);
  }
});

test('a replayed spawn is skipped by the core, answering the id it gave or none', async () => {
  const bridge = spawnBridge((request) => ({ id: request.id, outcome: 'skipped_on_replay' }));
  const seen = [];
  await handled(
    bridge,
    (input, ctx) => {
      seen.push(
        ctx.spawnWorkflow('worker', input).tryResult(),
        ctx.spawnWorkflow('worker', input, { id: 'w1' }).tryResult(),
      );
    },
    { replay: true },
  );
  assert.deepEqual(
    bridge.requests.map((request) => request.replay),
    [true, true],
  );
  assert.deepEqual(seen, [
    { id: '', outcome: 'skipped_on_replay' },
    { id: 'w1', outcome: 'skipped_on_replay' },
  ]);
});

test('the non-waiting read answers null until the answer arrives, and every read asks the core', async () => {
  let arrived = false;
  const bridge = spawnBridge(() => (arrived ? { id: 'w1', outcome: 'spawned' } : null));
  const seen = [];
  await handled(bridge, async (input, ctx) => {
    const handle = ctx.spawnWorkflow('worker', input, { id: 'w1' });
    seen.push(handle.tryResult());
    arrived = true;
    seen.push(handle.tryResult(), await handle.wait());
  });
  assert.deepEqual(seen, [
    null,
    { id: 'w1', outcome: 'spawned' },
    { id: 'w1', outcome: 'spawned' },
  ]);
  assert.deepEqual(bridge.reads, ['try', 'try', 'wait']);
});
