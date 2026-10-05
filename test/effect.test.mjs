// `ctx.effect` (`recorded-effects`): a live input keeps an outside call's
// result, a replay is served it without running the function, a replay that
// reaches outside some other way fails naming the effect, and the refusals. The
// same call through the real core with no controller is in
// `local-mode.test.mjs`.
import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  defineTasklet,
  loadNative,
} from '../dist/esm/index.js';
import { EffectReplayMismatch, TaskletTestHarness } from '../dist/esm/testing.js';

import { handleOnce, spawnBridge } from './dispatch-fixtures.mjs';

// A refusal is rendered by the addon, so this file needs the process context.
loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

/** How often each outside call ran, by name. */
const calls = new Map();

function model(name) {
  calls.set(name, (calls.get(name) ?? 0) + 1);
  return { answer: name, call: calls.get(name) };
}

const Asks = defineTasklet({
  name: 'effect_asks',
  run: async (question, ctx) => ctx.emit(await ctx.effect('model', () => model(question))),
});

const AsksAsync = defineTasklet({
  name: 'effect_asks_async',
  run: async (question, ctx) => ctx.emit(await ctx.effect('model', async () => model(question))),
});

/** A codec: the result is kept as upper-case text and read back lower. */
const upper = {
  encode: (value) => Buffer.from(value.toUpperCase(), 'utf8'),
  decode: (bytes) => Buffer.from(bytes).toString('utf8').toLowerCase(),
};

const WithCodec = defineTasklet({
  name: 'effect_codec',
  run: async (word, ctx) => ctx.emit(await ctx.effect('tool', () => word, { codec: upper })),
});

test('a live input runs the function and a replay is served its result', async () => {
  const harness = TaskletTestHarness.of(Asks);
  const live = await harness.input('live-q');
  const replayed = await harness.input('live-q', { replay: true });

  assert.equal(calls.get('live-q'), 1, 'the replay did not run the function');
  assert.deepEqual(live.emits, [{ answer: 'live-q', call: 1 }]);
  assert.deepEqual(replayed.emits, live.emits);
});

test('an async function is awaited', async () => {
  const harness = TaskletTestHarness.of(AsksAsync);
  const live = await harness.input('async-q');
  const replayed = await harness.input('async-q', { replay: true });

  assert.equal(calls.get('async-q'), 1);
  assert.deepEqual(live.emits, [{ answer: 'async-q', call: 1 }]);
  assert.deepEqual(replayed.emits, live.emits);
});

test('a codec encodes and decodes the result', async () => {
  const harness = TaskletTestHarness.of(WithCodec);
  assert.deepEqual((await harness.input('Hammer')).emits, ['hammer']);
});

test('a replay that runs a function fails naming the effect', async () => {
  let runs = 0;
  const OneMore = defineTasklet({
    name: 'effect_one_more',
    run: async (_input, ctx) => {
      runs += 1;
      for (let call = 0; call < runs; call += 1) await ctx.effect('model', () => 'same');
    },
  });
  const harness = TaskletTestHarness.of(OneMore);
  await harness.input('q');
  await assert.rejects(
    harness.input('q', { replay: true }),
    (error) =>
      error instanceof EffectReplayMismatch &&
      /effect 'model' ran its function/.test(error.message),
  );
});

test('a call made around the effect on replay fails naming it', async () => {
  let live = true;
  const Around = defineTasklet({
    name: 'effect_around',
    run: async (_input, ctx) => {
      if (live) {
        live = false;
        ctx.emit(await ctx.effect('model', () => model('around')));
      } else {
        ctx.emit(model('around'));
      }
    },
  });
  const harness = TaskletTestHarness.of(Around);
  await harness.input('q');
  await assert.rejects(
    harness.input('q', { replay: true }),
    (error) =>
      error instanceof EffectReplayMismatch &&
      /effect 'model' was never asked for/.test(error.message),
  );
});

for (const name of ['', 'm'.repeat(256)]) {
  test(`a bad name (${name.length} bytes) is refused without a retry`, async () => {
    const BadName = defineTasklet({
      name: 'effect_bad_name',
      run: (_input, ctx) => ctx.effect(name, () => assert.fail('a refused call runs nothing')),
    });
    await assert.rejects(
      TaskletTestHarness.of(BadName).input('q'),
      (error) =>
        error instanceof FlezhaError &&
        error.code === 'effect.bad_name' &&
        error.nonRetryable === true,
    );
  });
}

test('an effect inside an effect is refused', async () => {
  const Nested = defineTasklet({
    name: 'effect_nested',
    run: (_input, ctx) => ctx.effect('outer', () => ctx.effect('inner', () => 1)),
  });
  await assert.rejects(
    TaskletTestHarness.of(Nested).input('q'),
    (error) => error instanceof FlezhaError && error.code === 'effect.nested',
  );
});

test('a failing function keeps nothing and its error propagates', async () => {
  let attempts = 0;
  const Fails = defineTasklet({
    name: 'effect_fails',
    run: async (_input, ctx) => {
      attempts += 1;
      if (attempts === 1) {
        await ctx.effect('model', async () => {
          throw new Error('down');
        });
      }
      ctx.emit(await ctx.effect('model', () => 'second'));
    },
  });
  const harness = TaskletTestHarness.of(Fails);
  await assert.rejects(harness.input('q'), /down/);
  assert.deepEqual((await harness.input('q')).emits, ['second'], 'the failed call kept nothing');
});

test('a result the controller kept first is what the call returns, and the run is ended once', async () => {
  const bridge = spawnBridge();
  const seen = [];
  bridge.effectBegin = (token, item, name) => {
    seen.push(`begin ${token}/${item}/${name}`);
    return { status: 'run' };
  };
  bridge.effectEnd = async (_token, _item, payload) => {
    seen.push(`end ${Buffer.from(payload).toString('utf8')}`);
    return Buffer.from('{"answer":"first"}');
  };
  bridge.effectRelease = () => seen.push('release');
  let returned;
  const effect = await handleOnce(bridge, async (_input, ctx) => {
    returned = await ctx.effect('model', () => ({ answer: 'second' }));
  });
  assert.equal(effect.verdict, 'ok', effect.message);
  assert.deepEqual(returned, { answer: 'first' });
  assert.deepEqual(seen, ['begin 7/0/model', 'end {"answer":"second"}']);
});
