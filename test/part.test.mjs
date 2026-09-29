// A tasklet's part: what the host hands the core's part entry. The numbering,
// the stamps and the drops are the core's; what is asserted here is the host's
// half, over a stand-in bridge. The real addon's answers with no controller
// are in `local-mode.test.mjs`.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  defineBounded,
  defineTasklet,
  defineUnbounded,
  loadNative,
} from '../dist/esm/index.js';
import { answer } from '../dist/esm/dispatch.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

import { activationFor, depsFor, inputItem, spawnBridge } from './dispatch-fixtures.mjs';

// A refusal is rendered by the addon, so this file needs the process context.
loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');

/** The spawn stand-in, plus a part entry recording `token/item/payload`, or throwing `refuse()`. */
function partBridge(refuse) {
  const bridge = spawnBridge();
  bridge.parts = [];
  bridge.part = (token, item, payload) => {
    if (refuse) throw refuse();
    bridge.parts.push(`${token}/${item}/${utf8(payload)}`);
  };
  return bridge;
}

/** Sends the parts `1..n` for the input `n`, then emits `done:<n>`; a timer sends one part. */
const Parts = (seen) =>
  defineTasklet({
    name: 'parts',
    run: (n, ctx) => {
      for (let part = 1; part <= n; part += 1) ctx.part({ part });
      seen.push(`returned ${n}`);
      ctx.emit(`done:${n}`);
    },
    onTimer: (timer, ctx) => ctx.part({ fired: Number(timer.scheduledAt) }),
  });

async function run(spec, items, bridge) {
  return answer(activationFor(spec.factoryId)({ token: 7n, items }), {
    ...depsFor(spec),
    bridge,
  });
}

test("each item's parts carry the token, the item's index and the sink's encoding, before run returns", async () => {
  const seen = [];
  const bridge = partBridge();
  bridge.part = (token, item, payload) => seen.push(`${token}/${item}/${utf8(payload)}`);
  const spec = defineUnbounded({
    factoryId: 'part_unbounded',
    channels: (c) => c.input('in').into(Parts(seen)).sink('answer'),
  });
  const { effects } = await run(spec, [inputItem(2), inputItem(1)], bridge);

  assert.deepEqual(seen, [
    '7/0/{"part":1}',
    '7/0/{"part":2}',
    'returned 2',
    '7/1/{"part":1}',
    'returned 1',
  ]);
  assert.deepEqual(
    effects.map((effect) => effect.outputs.map((output) => utf8(output.payload))),
    [['done:2'], ['done:1']],
    'a part is not an output',
  );
});

test('a timer handler sends a part under its own item', async () => {
  const bridge = partBridge();
  const spec = defineUnbounded({
    factoryId: 'part_timer',
    channels: (c) => c.input('in').into(Parts([])).sink('answer'),
  });
  await run(
    spec,
    [inputItem(0), { kind: 'timer', timerKind: 'processing_time', scheduledAt: 5n, now: 5n }],
    bridge,
  );
  assert.deepEqual(bridge.parts, ['7/1/{"fired":5}']);
});

test('a per_input bounded stage sends its parts as an unbounded tasklet does', async () => {
  const seen = [];
  const bridge = partBridge();
  const spec = defineBounded({
    factoryId: 'part_bounded',
    loopMode: 'per_input',
    channels: (c) => c.input('in').into(Parts(seen)).sink('answer'),
  });
  const { effects } = await run(spec, [inputItem(2)], bridge);

  assert.equal(effects[0].verdict, 'ok', effects[0].message);
  assert.deepEqual(bridge.parts, ['7/0/{"part":1}', '7/0/{"part":2}']);
});

test('a refused part fails the invocation as any throw does', async () => {
  const details = JSON.stringify({ tasklet: 'parts', operator: 'op-1', channel: 'answer' });
  const refusal = () =>
    FlezhaError.fromDocument(loadNative().errorDocument('part.no_sink', details));
  const spec = defineUnbounded({
    factoryId: 'part_refused',
    channels: (c) => c.input('in').into(Parts([])).sink('answer'),
  });
  const { effects } = await run(spec, [inputItem(1)], partBridge(refusal));

  assert.equal(effects[0].verdict, 'retryable');
  assert.match(effects[0].message, /part\.no_sink/);
  assert.equal(effects[0].outputs, undefined, 'a failed item ships no output');
});

test('the harness encodes a part and drops it', async () => {
  const harness = TaskletTestHarness.of(Parts([]));
  assert.deepEqual((await harness.input(2)).emits, ['done:2']);

  const loop = {};
  loop.self = loop;
  const Unencodable = defineTasklet({ name: 'unencodable', run: (_, ctx) => ctx.part(loop) });
  await assert.rejects(TaskletTestHarness.of(Unencodable).input(1));
});
