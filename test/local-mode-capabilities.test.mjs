// Local mode over the five capabilities the LANGUAGE LAYER owns, each driven
// through the fixture class `test/apps/node_runner/conformance.json` names for
// it: the same class the end-to-end scenario drives, so what passes here and
// what passes there cannot be two different graphs.
//
// The other ten capabilities in the vocabulary are the controller's:
// persistence, effectively-once, correction, partitioning, steps relays,
// liveness, restarts. Local mode refuses them by name rather than
// approximating them, and `sdk-local-mode` says a sixth needs its own
// proposal.
//
// What each test can assert is what a run with no controller can see. A
// durable write reaches a null output, a parked payload has nowhere to park, and
// a partition member's key range arrives with `AddRelay`, so the
// assertions here are the output side of each capability, which is the half
// the language layer produces.

import assert from 'node:assert/strict';
import test from 'node:test';

import { LocalRelay } from '../dist/esm/index.js';

import { manifest, testAppModule } from './test-app.mjs';

const fixtures = await testAppModule('fixtures.mjs');
const declared = manifest().fixtures;

const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');

/** Run one class over `inputs` on `channel`, and return what `output` released. */
async function release(relay, channel, sink, inputs, budgetMs = 20_000) {
  const released = [];
  const run = LocalRelay.start(relay);
  try {
    run.onOutput(sink, (output) => released.push(output));
    for (const input of inputs) await run.feed(channel, input);
    await run.runUntilIdle(budgetMs);
  } finally {
    await run.close();
  }
  return released;
}

/** The class under test is the one the manifest names for the capability. */
function declares(capability, factoryId) {
  assert.ok(
    declared[capability].includes(factoryId),
    `conformance.json lists ${JSON.stringify(declared[capability])} for ${capability}, ` +
      `not ${factoryId}; the local-mode run must drive a class the e2e drives`,
  );
}

test('shorthand: a short form is built and run with no controller', async () => {
  declares('shorthand', 'sugar_do_nothing');

  const released = await release(fixtures.sugarDoNothing, 'input', 'sugar_do_nothing_echo', [
    'alpha',
    'beta',
  ]);

  assert.deepEqual(
    released.map((output) => utf8(output.payload)),
    ['alpha', 'beta'],
    'the builder produced a graph the core ran, in order',
  );
});

test('keyed_channels: a keyed output stamps the key the class resolved', async () => {
  // `partition_pass` is the manifest's only keyed-channel class. Partitioning
  // itself is the controller's (a member's key-group range arrives with
  // `AddRelay`), so what is asserted here is the half the language layer
  // owns: the key it resolves and the stamps it puts on the released value.
  declares('keyed_channels', 'partition_pass');

  const released = await release(fixtures.partitionPass, 'in', 'input', ['a:1', 'b:1', 'a:2']);

  assert.deepEqual(
    released.map((output) => utf8(output.payload)),
    ['a:1', 'b:1', 'a:2'],
  );
  assert.deepEqual(
    released.map((output) => output.metadata['flz.key']),
    ['a', 'b', 'a'],
    'each release carries the key its own value resolved to',
  );
  for (const stamp of ['flz.cell', 'flz.seq']) {
    assert.ok(stamp in released[0].metadata, `a keyed sink carries ${stamp}`);
  }
});

test('dynamic_cell: the declared filter runs before keying and nothing else is lost', async () => {
  // The class's output is not keyed, so a released value carries no cell
  // stamp and the per-key minting is not visible on this channel. It is
  // asserted above, on `partition_pass`, whose tasklet is dynamic too and
  // whose output IS keyed. What this class shows that that one does not is the
  // declared filter: a value it rejects answers no key, so the core is never
  // asked to mint a Cell for it.
  declares('dynamic_cell', 'dynamic_cell');

  const released = await release(fixtures.dynamicCell, 'input', 'kafka', [
    'a:1',
    'b:1',
    'drop',
    'a:2',
  ]);

  assert.deepEqual(
    released.map((output) => utf8(output.payload)),
    ['a:1', 'b:1', 'a:2'],
    'the filter dropped `drop` before it was keyed, and every other value reached its Cell',
  );
  assert.deepEqual(
    released.map((output) => output.metadata.channel),
    ['kafka', 'kafka', 'kafka'],
    'an unkeyed sink carries its channel and no identity stamps',
  );
});

test('retry_ladder: a bounded mailbox that drops nothing loses no input', async () => {
  // `poison_probe` is the manifest's other class for this capability and its
  // poison path ends in the dead-letter queue, which is the controller's. The
  // ladder's other arm, a full mailbox under `drop_nothing` where the
  // producer is held rather than the value discarded, is entirely the
  // language layer's, and is what a run with no controller can see.
  declares('retry_ladder', 'bounded_mailbox_drop_nothing');

  const inputs = Array.from({ length: 24 }, (_, at) => `n${at}`);
  const released = await release(
    fixtures.boundedMailboxDropNothing,
    'drop_nothing_input',
    'drop_nothing_echo',
    inputs,
  );

  assert.deepEqual(
    released.map((output) => utf8(output.payload)),
    inputs,
    'the mailbox holds 4 and 24 were fed: under `drop_nothing` the feed waits, in order',
  );
});

test('cell_timers: a declared timer fires locally with its scheduled time', async () => {
  declares('cell_timers', 'session_timeout');

  // The class reads its own deadline from the environment, the way the e2e
  // sets it; 200 ms keeps the run short without changing what is proved.
  const previous = process.env.TIMER_TEST_TIMEOUT_MS;
  process.env.TIMER_TEST_TIMEOUT_MS = '200';
  let released;
  try {
    released = await release(fixtures.sessionTimeout, 'input', 'kafka', ['open']);
  } finally {
    if (previous === undefined) delete process.env.TIMER_TEST_TIMEOUT_MS;
    else process.env.TIMER_TEST_TIMEOUT_MS = previous;
  }

  const closed = released
    .map((output) => utf8(output.payload))
    .filter((p) => p.startsWith('closed@'));
  assert.equal(
    closed.length,
    1,
    `the timer fired exactly once: ${JSON.stringify(released.map((o) => utf8(o.payload)))}`,
  );
  assert.match(closed[0], /^closed@\d+$/, 'the firing carries the timestamp it was scheduled for');
});

test('a controller capability is refused by name, not approximated', async () => {
  // The line `sdk-local-mode` draws: the ten the controller owns stay refused.
  // `tier_store` is effectively-once, which needs a durable store to release
  // from, so a local run says so instead of running the graph.
  const { tierStore } = await testAppModule('fixtures_state.mjs');
  await assert.rejects(
    async () => LocalRelay.start(tierStore),
    (error) => {
      assert.equal(error.code, 'runtime.local_needs_controller');
      assert.match(error.details.capability, /effectively_once/);
      return true;
    },
  );
});
