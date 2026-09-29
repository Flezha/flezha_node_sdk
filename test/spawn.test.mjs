// A tasklet's spawn: the handle it returns at once, and what the handle's
// reads answer. The core's own answers are stood in for by `spawnBridge`; the
// same reads against the real addon with no controller are in
// `local-mode.test.mjs`.
import assert from 'node:assert/strict';
import test from 'node:test';

import { ACTIVATION_SCHEMA_VERSION, FlezhaError, loadNative } from '../dist/esm/index.js';

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

/** The error document the core answers a failed spawn read with. */
const failed = (code, message, details = {}) => ({
  error: { code, class: 'conflict', message, details, hint: null },
});

/** Run `body` on one input and require the input to have been handled cleanly. */
async function handled(bridge, body, item) {
  const effect = await handleOnce(bridge, body, item);
  assert.equal(effect.verdict, 'ok', effect.message);
  return effect;
}

test('a spawn returns its handle at once, under the activation and input offset it runs in', async () => {
  const bridge = spawnBridge();
  let handle;
  await handled(
    bridge,
    (input, ctx) => {
      handle = ctx.spawnBounded('worker', { order: input });
    },
    { consumedOffset: 4n },
  );
  assert.equal(typeof handle.then, 'undefined', 'the handle is not a promise');
  assert.deepEqual(bridge.reads, [], 'nothing was read that the tasklet did not ask for');
  const [{ payload, ...request }] = bridge.requests;
  assert.deepEqual(request, {
    token: 7n,
    factoryId: 'worker',
    id: '',
    ifRunning: '',
    replay: false,
    offset: 4n,
  });
  assert.deepEqual(JSON.parse(Buffer.from(payload).toString('utf8')), { order: 'p' });
});

// The payload is the spawned instance's first message, encoded as a sink
// naming no codec sends a value, so the entry channel that reads it needs no
// knowledge of this SDK's inter-tasklet codec.
test('a spawn payload is what a text or json entry reads: UTF-8 for a string, JSON otherwise', async () => {
  const bridge = spawnBridge();
  await handled(bridge, (input, ctx) => {
    ctx.spawnBounded('worker', `ticket-${input}`);
    ctx.spawnBounded('worker', { symbol: 'AAPL', quantity: 10 });
    ctx.spawnBounded('worker', Buffer.from([0xff, 0x00, 0x01]));
  });
  const [text, json, bytes] = bridge.requests.map((request) => Buffer.from(request.payload));
  assert.equal(text.toString('utf8'), 'ticket-p', 'a string travels as its UTF-8 bytes');
  assert.deepEqual(JSON.parse(json.toString('utf8')), { symbol: 'AAPL', quantity: 10 });
  assert.deepEqual([...bytes], [0xff, 0x00, 0x01], 'bytes travel as they are');
});

test('a spawn resolves to the id it resolved to, generated or given', async () => {
  const bridge = spawnBridge((request) => ({
    id: request.id || 'worker-k7m2qzxw4pd3fn2a',
    outcome: request.id ? 'delivered' : 'spawned',
  }));
  const seen = [];
  await handled(bridge, async (input, ctx) => {
    seen.push(await ctx.spawnBounded('worker', input).wait());
    seen.push(ctx.spawnBounded('worker', input, { id: 'w1' }).tryResult());
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
  const bridge = spawnBridge(() =>
    failed('operator.already_running', "operator 'w1' is already running", { operator: 'w1' }),
  );
  const failures = [];
  await handled(bridge, async (input, ctx) => {
    const handle = ctx.spawnBounded('worker', input, { id: 'w1', ifRunning: 'error' });
    failures.push(
      await handle.wait().catch((error) => error),
      caught(() => handle.tryResult()),
    );
  });
  assert.equal(bridge.requests[0].ifRunning, 'error');
  const [waited, tried] = failures;
  assert.ok(waited instanceof FlezhaError);
  assert.equal(waited.code, 'operator.already_running');
  assert.deepEqual(waited.details, { operator: 'w1' });
  assert.deepEqual(tried, waited, 'every read answers the resolved failure');
  assert.deepEqual(bridge.reads, ['wait', 'try']);
});

test('ifRunning without an id throws a TypeError naming both, and sends nothing', async () => {
  const bridge = spawnBridge();
  let thrown;
  await handled(bridge, (input, ctx) => {
    thrown = caught(() => ctx.spawnBounded('worker', input, { ifRunning: 'deliver' }));
  });
  assert.ok(thrown instanceof TypeError, String(thrown));
  assert.match(thrown.message, /ifRunning without an id/);
  assert.deepEqual(bridge.requests, []);
});

test('each failure the core answers is thrown as the one error, carrying its code', async () => {
  for (const code of [
    'operator.already_running',
    'operator.factory_unknown',
    'operator.factory_not_bounded',
    'quota.exceeded',
    'spawn.timeout',
    'spawn.unavailable',
  ]) {
    const bridge = spawnBridge(() => failed(code, `the core's sentence for ${code}`));
    let failure;
    await handled(bridge, async (input, ctx) => {
      failure = await ctx
        .spawnBounded('worker', input)
        .wait()
        .catch((error) => error);
    });
    assert.ok(failure instanceof FlezhaError, `${code} was thrown as ${failure?.name}`);
    assert.equal(failure.code, code);
    assert.equal(failure.message, `${code}: the core's sentence for ${code}`);
  }
});

test('a failure nobody reads faults nothing and leaves no promise to reject', async () => {
  const rejections = [];
  const onRejection = (reason) => rejections.push(reason);
  process.on('unhandledRejection', onRejection);
  try {
    const bridge = spawnBridge(() => failed('quota.exceeded', 'the quota is reached'));
    const effect = await handled(bridge, (input, ctx) => {
      ctx.spawnBounded('worker', input);
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
        ctx.spawnBounded('worker', input).tryResult(),
        ctx.spawnBounded('worker', input, { id: 'w1' }).tryResult(),
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
    const handle = ctx.spawnBounded('worker', input, { id: 'w1' });
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
