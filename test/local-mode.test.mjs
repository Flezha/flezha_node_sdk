// The Node SDK's local mode: a user's own operator definition, its declared
// two-tasklet graph, and no controller anywhere.
//
// The graph and its expected sink output are read from
// `test/fixtures/local_mode_word_count.txt`, which the Rust, Java and Elixir
// local-mode suites read too — so every core is compared against one recorded
// expectation instead of by inspection.

import assert from 'node:assert/strict';
import { dirname } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { LocalOperator, defineOperator, defineTasklet } from '../dist/esm/index.js';

import { read } from './contract.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const utf8 = (bytes) => Buffer.from(bytes).toString('utf8');
const bytes = (text) => Buffer.from(text, 'utf8');

/** One line into its words: the hop to `Tally` is the runtime's job. */
const Split = defineTasklet({
  name: 'split',
  run: (line, ctx) => {
    for (const word of line.split(/\s+/).filter(Boolean)) {
      ctx.emit(word.toLowerCase());
    }
  },
});

/** One Cell per word, minted on first sight of it. */
const Tally = defineTasklet({
  name: 'tally',
  idleTtlSeconds: 300,
  initState: () => ({ seen: 0 }),
  applyChange: (state, change) => {
    state.seen = change.seen;
  },
  run: (word, ctx) => {
    const seen = ctx.state.seen + 1;
    ctx.change({ seen });
    ctx.emit(`${word} ${seen}`);
  },
});

const WordCount = defineOperator({
  factoryId: 'local_word_count_node',
  channels: (c) => {
    c.input('lines', { codec: 'text' })
      .into(Split)
      .keyBy((word) => word)
      .into(Tally)
      .keyed()
      .sink('totals');
  },
});

/** The scenario every core runs: the lines to feed, and the sink values. */
function scenario() {
  const text = read('test/fixtures/local_mode_word_count.txt');
  const lines = [];
  const totals = new Map();
  for (const row of text.split('\n')) {
    if (row.startsWith('lines: ')) {
      lines.push(row.slice('lines: '.length));
    } else if (row.startsWith('totals: ')) {
      const total = row.slice('totals: '.length);
      totals.set(total, (totals.get(total) ?? 0) + 1);
    }
  }
  return { lines, expected: totals };
}

test('a declared graph runs locally and releases the recorded output', async () => {
  const { lines, expected } = scenario();
  const released = [];
  const run = LocalOperator.start(WordCount);
  try {
    run.onSink('totals', (output) => released.push(output));
    for (const line of lines) {
      await run.feed('lines', line);
    }
    await run.runUntilIdle(20_000);
  } finally {
    await run.close();
  }

  const counted = new Map();
  for (const output of released) {
    const payload = utf8(output.payload);
    counted.set(payload, (counted.get(payload) ?? 0) + 1);
  }
  assert.deepEqual(
    [...counted].sort(),
    [...expected].sort(),
    'both tasklets ran, the runtime performed the hop, and one Cell per word counted',
  );

  const first = released[0];
  assert.equal(first.channel, 'totals');
  for (const stamp of ['flz.cell', 'flz.seq', 'flz.key']) {
    assert.ok(
      stamp in first.metadata,
      `a keyed sink carries ${stamp}: ${JSON.stringify(first.metadata)}`,
    );
  }
});

test('the core reads a payload view, at its own offset, exactly as it reads a Buffer', async () => {
  // What the host hands the core is no longer copied into a `Buffer` first, so
  // the reader has to honour a view's `byteOffset` and `byteLength` rather than
  // read its backing store from the start. A wrong reader answers the padding.
  const line = 'alpha beta alpha';
  const padded = Buffer.concat([bytes('PAD'), bytes(line), bytes('TAIL')]);
  const view = new Uint8Array(padded.buffer, padded.byteOffset + 3, line.length);
  assert.notEqual(view.byteOffset, 0, 'the fixture is only a test while the view is offset');

  for (const payload of [view, bytes(line), line]) {
    const released = [];
    const run = LocalOperator.start(WordCount);
    try {
      run.onSink('totals', (output) => released.push(utf8(output.payload)));
      await run.feed('lines', payload);
      await run.runUntilIdle(20_000);
    } finally {
      await run.close();
    }
    assert.deepEqual(released.sort(), ['alpha 1', 'alpha 2', 'beta 1']);
  }
});

test('a channel the class does not declare is refused by name', async () => {
  const run = LocalOperator.start(WordCount);
  try {
    await assert.rejects(
      () => run.feed('nope', '1'),
      (refusal) =>
        refusal.code === 'runtime.channel_undeclared' &&
        refusal.details.channel === 'nope' &&
        String(refusal.details.channels).includes('lines'),
      'the refusal names the channel and what is declared',
    );
  } finally {
    await run.close();
  }
});

/** Reads the map an undeclared entry decodes, and emits a string and a map. */
const Defaults = defineTasklet({
  name: 'defaults',
  run: (value, ctx) => {
    ctx.emit(`a is ${value.a}`);
    ctx.emit({ a: value.a });
  },
});

const DefaultCodecs = defineOperator({
  factoryId: 'local_default_codecs_node',
  channels: (c) => {
    c.input('in').into(Defaults).sink('out');
  },
});

test('an undeclared entry reads JSON and an undeclared sink follows the value', async () => {
  const released = [];
  const run = LocalOperator.start(DefaultCodecs);
  try {
    run.onSink('out', (output) => released.push(utf8(output.payload)));
    await run.feed('in', '{"a":1}');
    await run.runUntilIdle(5_000);
  } finally {
    await run.close();
  }
  assert.deepEqual(released, ['a is 1', '{"a":1}']);
});

// Plain JavaScript runs no type checker, so a returning `run` still loads.
test('a short form whose run returns a value runs it once, and nothing reaches its sink', async () => {
  let calls = 0;
  const Returning = defineOperator({
    factoryId: 'local_returning_node',
    input: 'ticks',
    sink: 'out',
    codec: 'text',
    run: (tick) => {
      calls += 1;
      return `got ${tick}`;
    },
  });
  const released = [];
  const run = LocalOperator.start(Returning);
  try {
    run.onSink('out', (output) => released.push(output));
    await run.feed('ticks', 'a');
    await run.runUntilIdle(5_000);
  } finally {
    await run.close();
  }
  assert.equal(calls, 1, 'a returned value is ignored, never retried');
  assert.deepEqual(released, []);
});

test('a spawn with no controller anywhere is unavailable, from either read', async () => {
  const Spawner = defineTasklet({
    name: 'spawner',
    run: async (input, ctx) => {
      let tried;
      try {
        ctx.spawnSteps('worker', input).tryResult();
      } catch (error) {
        tried = error;
      }
      const waited = await ctx
        .spawnSteps('worker', input, { id: 'w1' })
        .wait()
        .catch((error) => error);
      ctx.emit(`${tried?.name} ${tried?.code} ${waited.name} ${waited.code}`);
    },
  });
  const released = [];
  const run = LocalOperator.start(
    defineOperator({
      factoryId: 'local_spawner_node',
      channels: (c) => c.input('in', { codec: 'text' }).into(Spawner).sink('out'),
    }),
  );
  try {
    run.onSink('out', (output) => released.push(utf8(output.payload)));
    await run.feed('in', 'p');
    await run.runUntilIdle(5_000);
  } finally {
    await run.close();
  }
  assert.deepEqual(released, ['FlezhaError spawn.unavailable FlezhaError spawn.unavailable']);
});

test('a local run drops every part and releases the output alone', async () => {
  const Parts = defineTasklet({
    name: 'parts',
    run: (input, ctx) => {
      ctx.part(`${input}-1`);
      ctx.emit(`done:${input}`);
    },
  });
  const released = [];
  const run = LocalOperator.start(
    defineOperator({
      factoryId: 'local_parts_node',
      channels: (c) => c.input('in', { codec: 'text' }).into(Parts).sink('answer'),
    }),
  );
  try {
    run.onSink('answer', (output) => released.push(utf8(output.payload)));
    await run.feed('in', 'p');
    await run.runUntilIdle(5_000);
  } finally {
    await run.close();
  }
  assert.deepEqual(released, ['done:p']);
});

test('the core refuses a part whose channel ends in no sink', async () => {
  const refused = [];
  const Refused = defineTasklet({
    name: 'refused',
    run: (input, ctx) => {
      try {
        ctx.part(input);
      } catch (error) {
        refused.push(error.code);
      }
      ctx.emit(input);
    },
  });
  const Drain = defineTasklet({ name: 'drain', run: () => {} });
  const graphs = {
    local_part_to_tasklet_node: (c) => c.input('in', { codec: 'text' }).into(Refused).into(Drain),
    local_part_to_response_node: (c) => c.input('in', { codec: 'text' }).into(Refused).response(),
  };
  for (const [factoryId, channels] of Object.entries(graphs)) {
    const run = LocalOperator.start(defineOperator({ factoryId, channels }));
    try {
      await run.feed('in', 'p');
      await run.runUntilIdle(5_000);
    } finally {
      await run.close();
    }
  }
  assert.deepEqual(refused, ['part.no_sink', 'part.no_sink']);
});
