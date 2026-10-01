// The graph builder, what it records, and the refusals it still makes itself.
//
// Every topology rule now lives in `flz_core::host::register::check`, and
// `test/fixtures/reference_topologies.txt` is where each one's case lives — one
// row per refusal, read by every SDK's conformance run. What is left here is
// the authoring surface: the shape a declaration builds, the names it derives,
// which closure reaches which edge, and the defect the record cannot carry.
// `validate` is the call that submits, so it needs the process context.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  defineOperator,
  defineTasklet,
  encodeValue,
  loadNative,
  Registry,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import * as sdk from '../dist/esm/index.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

import { member } from './contract.mjs';
import { codesOf, coded, refusal } from './dispatch-fixtures.mjs';
import { testAppModule } from './test-app.mjs';

loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const fixtures = await testAppModule('fixtures.mjs');

const echo = (name, options = {}) =>
  defineTasklet({ name, ...options, run: (input, ctx) => ctx.emit(input) });

/** The channels the core derived, as `[name, kind, producer, consumers]`. */
const derivedChannels = (spec) =>
  validate(spec).channels.map((channel) => [
    channel.name,
    channel.kind,
    channel.producer,
    channel.consumers.map((edge) => edge.tasklet),
  ]);

test('a graph builds and its topology reaches the declaration', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperator({
    factoryId: 'chain',
    channels: (c) => {
      c.input('in').into(first).into(second).sink();
    },
  });
  // The record carries the DECLARED kind; the plugin kind is the wiring rule,
  // which the core applies and hands back.
  assert.deepEqual(
    validate(spec).channels.map((channel) => [channel.name, channel.kind]),
    [
      ['in', 'entry'],
      ['first', 'inter_task'],
      ['second', 'plugin'],
    ],
  );
  const decl = toHostClassDecl(spec);
  assert.deepEqual(
    decl.channels.map((channel) => channel.kind),
    ['entry', 'inter_task', 'inter_task'],
  );
  assert.deepEqual(
    decl.tasklets.map((p) => p.name),
    ['first', 'second'],
  );
  assert.equal(decl.sdk_lang, 4);
  assert.equal(decl.change_format_id, 5);
});

test('keyBy declares a dynamic consumer and the key reaches the channel', () => {
  const counter = defineTasklet({
    name: 'counter',
    idleTtlSeconds: 60,
    maxCells: 100,
    initState: () => ({ seen: 0 }),
    applyChange: (state, change) => {
      state.seen += change.by;
    },
    run: (input, ctx) => {
      ctx.change({ by: 1 });
      ctx.emit(input);
    },
  });
  const spec = defineOperator({
    factoryId: 'keyed',
    channels: (c) => {
      c.input('in')
        .keyBy((value) => value.symbol)
        .into(counter)
        .keyed()
        .sink();
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.tasklets[0].cell_mode, 'dynamic');
  assert.equal(decl.tasklets[0].idle_ttl_ms, 60_000);
  assert.equal(decl.tasklets[0].max_cells, 100);
  assert.equal(decl.channels[1].keyed, true);
});

// The record carries `change_is_nochange` per tasklet: a handler that folds
// no change has the empty Change type, and a logical clock over it never
// advances. The refusal is the core's `topology.unservable_clock`; what is asserted here
// is that this builder writes the fact it reads.
test('a tasklet that folds no change declares it, and a logical clock over it is refused', () => {
  const spec = defineOperator({
    factoryId: 'unservable',
    channels: (c) =>
      c
        .input('in')
        .into(defineTasklet({ name: 'ticker', logicalClock: true, run: (i, ctx) => ctx.emit(i) }))
        .sink(),
  });
  assert.equal(toHostClassDecl(spec).tasklets[0].change_is_nochange, true);
  assert.deepEqual(codesOf(refusal(() => spec)), ['topology.unservable_clock']);
});

// The verdict itself: one exception carrying every code the core answered with.
test('a refusal reaches the author under the core’s own codes', () => {
  const target = echo('target');
  const refused = refusal(() =>
    defineOperator({
      factoryId: 'edge_already_wired',
      channels: (c) => {
        const edge = c.input('in');
        edge.into(target);
        edge.map((value) => `${value}!`).into(target);
      },
    }),
  );
  assert.deepEqual(codesOf(refused), ['topology.edge_already_wired']);
  assert.equal(refused.code, 'topology.edge_already_wired');
  assert.equal(refused.class, 'invalid');
  assert.deepEqual(refused.details.issues[0].details, { channel: 'in', tasklet: 'target' });
  assert.match(refused.message, /^topology\.edge_already_wired: /);
});

// The one refusal this builder raises under a core code reads the way the core's do.
test('a node identity collision is refused under the core’s code, in the message too', () => {
  const refused = refusal(() =>
    defineOperator({
      factoryId: 'collision',
      channels: (c) => {
        const entry = c.input('in');
        entry.into(echo('twin'));
        entry.into(echo('twin'));
      },
    }),
  );
  assert.equal(refused.code, 'topology.node_identity_collision');
  assert.deepEqual(refused.details, { name: 'twin' });
  assert.match(refused.message, /^topology\.node_identity_collision: /);
});

test('a named edge names the channel, not the consuming node', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineOperator({
    factoryId: 'named_edge',
    channels: (c) => {
      c.input('in').into(first).named('hop').into(second).sink();
    },
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.deepEqual(
    decl.channels.map((channel) => channel.name),
    ['in', 'hop', 'second'],
  );
  assert.deepEqual(
    decl.tasklets.map((p) => p.name),
    ['first', 'second'],
  );
});

test('a second mention under its own node name is a second node', () => {
  const step = echo('step');
  const spec = defineOperator({
    factoryId: 'twice_over',
    channels: (c) => {
      c.input('in').into(step).into(step, 'step_again').sink();
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).tasklets.map((p) => p.name),
    ['step', 'step_again'],
  );
});

test('two tasklets claiming one node identity are refused', () => {
  const refused = refusal(() => {
    const first = echo('same');
    const second = echo('same');
    return defineOperator({
      factoryId: 'collide',
      channels: (c) => {
        c.input('in').branch([(edge) => edge.into(first), (edge) => edge.into(second)]);
      },
    });
  });
  assert.equal(refused.code, 'topology.node_identity_collision');
});

test('the checkpoint acknowledgement reaches the declaration as the tree the core reads', () => {
  const spec = defineOperator({
    factoryId: 'compose',
    flow: {
      exactly: {
        effectively_once: {
          ack: { checkpoint: { every: 4, outputs: 'at_checkpoint', maxDelayMs: 250 } },
        },
      },
    },
    channels: (c) => {
      c.input('in').into(echo('p')).keyed().sink();
    },
  });
  assert.equal(validate(spec).ack_granularity, 'checkpoint');
  assert.deepEqual(toHostClassDecl(spec).run, {
    flow: {
      exactly: {
        effectively_once: {
          ack: { checkpoint: { every: 4, outputs: 'at_checkpoint', max_delay_ms: 250 } },
        },
      },
    },
  });
});

test('two tasklets each terminate on their own sink, named by their node', () => {
  const a = echo('a');
  const b = echo('b');
  const spec = defineOperator({
    factoryId: 'one_sink_twice',
    channels: (c) => {
      c.input('in').branch([(edge) => edge.into(a).sink(), (edge) => edge.into(b).sink()]);
    },
  });
  assert.deepEqual(
    validate(spec).channels.map((channel) => [channel.name, channel.kind, channel.sink]),
    [
      ['in', 'entry', null],
      ['a', 'plugin', 'a'],
      ['b', 'plugin', 'b'],
    ],
  );
});

test('the sink names the channel, and the no-argument form leaves it derived', () => {
  const named = defineOperator({
    factoryId: 'sink_named',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink('items_output');
    },
  });
  const derived = defineOperator({
    factoryId: 'sink_derived',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink();
    },
  });

  assert.deepEqual(
    validate(named).channels.map((channel) => [channel.name, channel.sink]),
    [
      ['in', null],
      ['items_output', 'items_output'],
    ],
  );
  assert.deepEqual(
    validate(derived).channels.map((channel) => [channel.name, channel.sink]),
    [
      ['in', null],
      ['producer', 'producer'],
    ],
  );
});

test('a channel named by both named() and sink() is refused naming both', () => {
  const spec = defineOperator({
    factoryId: 'named_twice',
    channels: (c) => {
      c.input('in').into(echo('producer')).named('a').sink('b');
    },
  });
  assert.throws(
    () => validate(spec),
    (error) =>
      error.code === 'topology.channel_named_twice' &&
      error.details.issues[0].details.named === 'a' &&
      error.details.issues[0].details.sink === 'b',
  );
});

test('a channel option with nothing wired after it still reaches the channel', () => {
  const renamed = defineOperator({
    factoryId: 'trailing_named',
    channels: (c) => {
      c.input('in').into(echo('producer')).named('renamed');
    },
  });
  assert.deepEqual(
    derivedChannels(renamed).map(([name]) => name),
    ['in', 'renamed'],
  );
  const keyed = refusal(() =>
    defineOperator({
      factoryId: 'trailing_keyed',
      channels: (c) => {
        c.input('in').into(echo('producer')).keyed();
      },
    }),
  );
  assert.deepEqual(codesOf(keyed), ['topology.keyed_channel_without_plugin_wire']);
});

test('two channels naming one sink is refused as a duplicate sink name', () => {
  const spec = defineOperator({
    factoryId: 'one_sink_two_channels',
    channels: (c) => {
      c.input('in').branch([
        (edge) => edge.into(echo('a')).sink('shared'),
        (edge) => edge.into(echo('b')).sink('shared'),
      ]);
    },
  });
  assert.throws(
    () => validate(spec),
    (error) => codesOf(error).includes('topology.duplicate_sink_name'),
  );
});

test('a steps class declares its run and its wall-clock stop beside its tasklets', () => {
  const spec = defineOperator({
    factoryId: 'saga',
    steps: { each: 'chained', onFailure: 'keep_progress' },
    maxDurationMs: 60_000,
    channels: (c) => c.input('in').into(echo('p')),
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.deepEqual(decl.run, { steps: { each: 'chained', on_failure: 'keep_progress' } });
  assert.equal(decl.max_duration_ms, 60_000);
  assert.equal(decl.durable, undefined, 'an unwritten durability is the core default');
  assert.deepEqual(
    decl.tasklets.map((p) => p.name),
    ['p'],
  );
});

test('state read but never folded is allowed; recording a change is not', async () => {
  // A value seeded at start and read is a real shape — the reference
  // `compare_filter` fixture is exactly it — so the declaration stands. What
  // is refused is recording a change with nothing to fold it through, at the
  // point of use rather than at declaration.
  const ReadOnly = defineTasklet({
    name: 'read_only',
    initState: () => ({ comparable: 'default' }),
    run: (input, ctx) => {
      if (ctx.state.comparable === input) ctx.emit(input);
    },
  });
  const harness = TaskletTestHarness.of(ReadOnly);
  assert.deepEqual((await harness.input('default')).emits, ['default']);
  assert.deepEqual((await harness.input('other')).emits, []);

  const Recording = defineTasklet({
    name: 'recording',
    initState: () => ({ n: 0 }),
    run: (_input, ctx) => ctx.change({ by: 1 }),
  });
  await assert.rejects(
    () => TaskletTestHarness.of(Recording).input(1),
    coded('runtime.change_without_apply_change'),
  );
});

test('applyChange with no state to fold into is refused at definition', () => {
  assert.throws(
    () => defineTasklet({ name: 'nowhere', applyChange: () => {}, run: () => {} }),
    coded('topology.change_without_state'),
  );
});

test('a declared state that cannot be encoded is refused at definition', () => {
  assert.throws(
    () =>
      defineTasklet({
        name: 'unencodable',
        initState: () => ({ pending: new Map() }),
        applyChange: () => {},
        run: () => {},
      }),
    (error) =>
      coded('topology.state_unrepresentable')(error) &&
      error.details.path === 'unencodable.state.pending',
  );
});

test('a sink names the channel a forwarded frame carries', () => {
  // The channel name is what a forwarded frame carries and what selects the
  // receiving operator's entry channel, so the name an author writes on the
  // sink has to be the one that rides the wire.
  const spec = defineOperator({
    factoryId: 'named_sink',
    channels: (c) => {
      c.input('in').into(echo('relay')).sink('input');
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => channel.name),
    ['in', 'input'],
  );
});

// `sdk-graph-authoring`: an underived channel name comes from the PRODUCER
// NODE, never from the sink it terminates on — the sink's name is the plugin's,
// and several channels may name one sink.
test('an unnamed edge terminating on a sink takes the producing node’s name', () => {
  const spec = defineOperator({
    factoryId: 'unnamed_sink',
    channels: (c) => {
      c.input('in').into(echo('plain')).sink();
    },
  });
  validate(spec);
  assert.deepEqual(
    toHostClassDecl(spec).channels.map((channel) => channel.name),
    ['in', 'plain'],
  );
});

// --- one output channel per producing tasklet ------------------------------
//
// The model Rust and Java carry: a tasklet node owns ONE output channel,
// created on first wiring and reused after. A held handle wired twice appends
// a second consumer to it — it does not declare a second channel claiming the
// same producer, which the core resolves by first match
// (`host/register.rs` `tasklet_wiring`) and so silently truncates.

// --- one output channel per producing tasklet ------------------------------
//
// The model Rust and Java carry: a tasklet node owns ONE output channel,
// created on first wiring and reused after. A held handle wired twice appends
// a second consumer to it — it does not declare a second channel claiming the
// same producer, which the core resolves by first match
// (`host/register.rs` `tasklet_wiring`) and so silently truncates.

test('a held handle wired twice is one channel with two consumers', () => {
  const producer = echo('producer');
  const spec = defineOperator({
    factoryId: 'fan_out',
    channels: (c) => {
      const edge = c.input('in').into(producer);
      edge.into(echo('b'));
      edge.into(echo('c'));
    },
  });
  const channels = derivedChannels(spec);
  assert.deepEqual(channels, [
    ['in', 'entry', null, [0]],
    ['producer', 'inter_task', 0, [1, 2]],
  ]);
  assert.equal(channels.filter((channel) => channel[2] === 0).length, 1);
});

test('branch(...) and two into(...) statements derive the identical topology', () => {
  // The record differs by one link — the sugar's declared arity — and the
  // topology the core derives from the two does not.
  const graph = (factoryId, wire) =>
    JSON.stringify(
      derivedChannels(
        defineOperator({
          factoryId,
          channels: (c) => wire(c.input('in').into(echo('producer'))),
        }),
      ),
    );

  const separate = graph('separate', (edge) => {
    edge.into(echo('b'));
    edge.into(echo('c'));
  });
  const branched = graph('branched', (edge) =>
    edge.branch([(arm) => arm.into(echo('b')), (arm) => arm.into(echo('c'))]),
  );
  assert.equal(separate, branched);
});

test('sink(...) returns the handle, so a consumer wires behind it', () => {
  const spec = defineOperator({
    factoryId: 'hybrid',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink('hybrid_indicator').into(echo('consumer'));
    },
  });
  assert.deepEqual(
    validate(spec).channels.map((channel) => [
      channel.name,
      channel.kind,
      channel.producer,
      channel.consumers.map((edge) => edge.tasklet),
      channel.sink,
    ]),
    [
      ['in', 'entry', null, [0], null],
      // A sink alongside a consumer stays inter-tasklet: the value reaches
      // both, and the channel is journalled as the edge it still is.
      ['hybrid_indicator', 'inter_task', 0, [1], 'hybrid_indicator'],
    ],
  );
});

test('response() records its link and ends the channel as a sink would, keeping the name', () => {
  const terminal = defineOperator({
    factoryId: 'answers',
    channels: (c) => {
      c.input('in').into(echo('producer')).response();
    },
  });
  const hybrid = defineOperator({
    factoryId: 'answers_and_feeds',
    channels: (c) => {
      c.input('in').into(echo('producer')).response().into(echo('consumer'));
    },
  });
  const channelsOf = (spec) =>
    validate(spec).channels.map((channel) => [
      channel.name,
      channel.kind,
      channel.consumers.map((edge) => edge.tasklet),
      channel.sink,
    ]);
  assert.deepEqual(toHostClassDecl(terminal).channels[1].chains, [[{ link: 'response' }]]);
  // A response is a plugin end but not a sink: the core names no sink for it.
  assert.deepEqual(channelsOf(terminal), [
    ['in', 'entry', [0], null],
    ['producer', 'plugin', [], null],
  ]);
  assert.deepEqual(channelsOf(hybrid), [
    ['in', 'entry', [0], null],
    ['producer', 'inter_task', [1], null],
  ]);
});

test('a sink beside a response on one channel is refused as ending twice', () => {
  const error = refusal(() =>
    defineOperator({
      factoryId: 'ends_twice',
      channels: (c) => {
        c.input('in').into(echo('producer')).sink('out').response();
      },
    }),
  );
  assert.deepEqual(codesOf(error), ['topology.channel_ends_twice']);
  assert.equal(error.details.issues[0].details.channel, 'out');
});

test('keyed() reaches the channel of a handle that also declares a sink', () => {
  const spec = defineOperator({
    factoryId: 'keyed_hybrid',
    channels: (c) => {
      c.input('in').into(echo('producer')).keyed().sink('mid').into(echo('consumer'));
    },
  });
  validate(spec);
  assert.equal(toHostClassDecl(spec).channels[1].keyed, true);
});

test('a transform declared before one into(...) reaches that consumer only', () => {
  const spec = defineOperator({
    factoryId: 'per_edge_transform',
    channels: (c) => {
      const edge = c.input('in').into(echo('producer'));
      edge.map((value) => `${value}!`).into(echo('b'));
      edge.into(echo('c'));
    },
  });
  validate(spec);
  const [, channel] = spec.channels;
  assert.deepEqual(
    channel.consumers.map((consumer) => [consumer.tasklet, consumer.transforms.length]),
    [
      [1, 1],
      [2, 0],
    ],
  );
});

// The refusal both references carry by name — Rust's
// `TopologyIssue::DanglingTransformChain` and Java's `danglingTransformChain`.
// A transform lives on the consumer edge it terminates in, so one no `into(...)`
// closed has nowhere to run and would be discarded without a word.

test('a bare re-declared edge is a no-op, not a second edge', () => {
  const target = echo('target');
  const spec = defineOperator({
    factoryId: 'edge_redeclared_bare',
    channels: (c) => {
      const edge = c.input('in');
      edge.into(target);
      edge.into(target);
    },
  });
  validate(spec);
  assert.deepEqual(
    spec.channels[0].consumers.map((edge) => [edge.tasklet, edge.transforms.length]),
    [[0, 0]],
  );
});

const forms = {
  full: { channels: (c) => c.input('in').into(echo('p')) },
  short: { sink: 'out', run: (input, ctx) => ctx.emit(input) },
};

test('a setting under a parent that does not carry it is refused naming both, in either form', () => {
  for (const [settings, field, parent] of [
    [{ flow: { each: 'per_input' } }, 'each', 'flow'],
    [{ steps: { correctionMs: 5_000 } }, 'correction_ms', 'steps'],
    [{ flow: { exactly: { at_least_once: { ack: 'checkpoint' } } } }, 'ack', 'at_least_once'],
    [{ durable: { false: { sleepAfter: 1_000 } } }, 'sleep_after', 'false'],
  ]) {
    for (const [form, body] of Object.entries(forms)) {
      const error = refusal(() => defineOperator({ factoryId: 'misplaced', ...settings, ...body }));
      assert.deepEqual(codesOf(error), ['topology.unknown_field'], `${field} on a ${form} form`);
      assert.ok(
        error.message.includes(field) && error.message.includes(`'${parent}'`),
        error.message,
      );
    }
  }
});

test('the durability reaches the declaration as the tree, its members as the core spells them', () => {
  const channels = (c) => c.input('in').into(echo('p'));
  const durable = (settings) =>
    toHostClassDecl(defineOperator({ factoryId: 'kept', ...settings, channels })).durable;
  assert.equal(durable({}), undefined);
  assert.equal(durable({ durable: false }), false);
  assert.equal(durable({ steps: {}, durable: 'remote' }), 'remote');
  assert.deepEqual(durable({ durable: { local: { sleepAfter: false } } }), {
    local: { sleep_after: false },
  });
  validate(
    defineOperator({ factoryId: 'awake', durable: { local: { sleepAfter: 2_000 } }, channels }),
  );
});

test('a remote flow class reaches the core and is refused by its name', () => {
  const error = refusal(() =>
    defineOperator({ factoryId: 'remote', durable: 'remote', ...forms.full }),
  );
  assert.deepEqual(codesOf(error), ['topology.remote_on_flow'], error.message);
});

test('a class that names both runs, or a kind word where its run is written, is refused as its run', () => {
  for (const [settings, value] of [
    [{ flow: {}, steps: {} }, '{"flow":{},"steps":{}}'],
    [{ bounded: {} }, 'bounded'],
  ]) {
    const error = refusal(() => defineOperator({ factoryId: 'runs', ...settings, ...forms.full }));
    assert.deepEqual(codesOf(error), ['topology.invalid_setting'], error.message);
    assert.ok(error.message.includes(value), error.message);
  }
});

test('the retired wake field is refused in either form, naming it', () => {
  for (const [form, body] of Object.entries(forms)) {
    const error = refusal(() =>
      defineOperator({ factoryId: 'w', steps: {}, wakeAfterMs: 5_000, ...body }),
    );
    assert.equal(error.code, 'topology.unknown_field', form);
    assert.deepEqual(error.details, { declaration: "operator 'w'", fields: '`wakeAfterMs`' });
  }
});

test('a loop shape or failure mode outside its choice is refused at registration, naming the value', () => {
  for (const [steps, value] of [
    [{ each: 'forever' }, 'forever'],
    [{ onFailure: 'sometimes' }, 'sometimes'],
  ]) {
    for (const body of Object.values(forms)) {
      const error = refusal(() => defineOperator({ factoryId: 'moded', steps, ...body }));
      assert.deepEqual(codesOf(error), ['topology.invalid_setting'], error.message);
      assert.ok(error.message.includes(value), error.message);
    }
  }
});

// -- channel codecs -------------------------------------------------------------
//
// A codec is resolved when the class is declared, into the two slots the
// dispatcher and the plugin frame already read. What every SDK sends alike is
// `test/fixtures/channel_codecs.txt`; what is Node's own is asserted here.

const CODEC_CASES = member('test/fixtures/channel_codecs.txt');

const codecSpec = (input = [], sink = []) =>
  defineOperator({
    factoryId: 'codecs',
    channels: (c) => {
      c.input('in', ...input)
        .into(echo('echo'))
        .sink('out', ...sink);
    },
  });

const codecSlots = (input, sink) => {
  const [entry, out] = codecSpec(input, sink).channels;
  return [entry.decode, out.sinkEncode];
};

const caseBytes = (field) => {
  if (field === '<empty>') return Buffer.alloc(0);
  return field.startsWith('hex:') ? Buffer.from(field.slice(4), 'hex') : Buffer.from(field, 'utf8');
};

// A case records that a step refused, whatever it threw.
const caseOutcome = (decode, encode, payload) => {
  let value;
  try {
    value = decode(payload);
  } catch {
    return 'decode_error';
  }
  try {
    return Buffer.from(encode(value));
  } catch {
    return 'encode_error';
  }
};

const emitRefusal = (channel) => (error) =>
  error instanceof FlezhaError &&
  (error.details.channel === channel || error.details.owner === `channel '${channel}'`);

test('every shared codec case sends the bytes the fixture records', () => {
  const rows = readFileSync(CODEC_CASES, 'utf8')
    .split('\n')
    .filter((row) => row && !row.startsWith('#'));
  assert.ok(rows.length > 0, 'the shared codec cases were read');
  const codec = (name) => (name === 'none' ? [] : [{ codec: name }]);
  for (const row of rows) {
    const [entry, sink, payload, expected] = row.split('|');
    const [decode, encode] = codecSlots(codec(entry), codec(sink));
    const want = expected.endsWith('_error') ? expected : caseBytes(expected);
    assert.deepEqual(caseOutcome(decode, encode, caseBytes(payload)), want, row);
  }
});

test('a JSON entry reads an object, and a sink sends bytes as the same array', () => {
  const [decode, encode] = codecSlots();
  assert.deepEqual(decode(Buffer.from('{"a":1}')), { a: 1 });
  const raw = new Uint8Array([0xff, 0x00]);
  assert.equal(encode(raw), raw);
});

test('msgpack is the class codec', () => {
  const [decode, encode] = codecSlots([{ codec: 'msgpack' }], [{ codec: 'msgpack' }]);
  assert.deepEqual(decode(encodeValue({ a: 1 })), { a: 1 });
  assert.deepEqual(encode({ a: 1 }), encodeValue({ a: 1 }));
});

// A Node string is not bytes, so a `bytes` sink refuses one, where Ruby and PHP
// take a string there.
test('a refusal names the channel, and a bytes sink refuses a string', () => {
  const [decode, text] = codecSlots([{ codec: 'text' }], [{ codec: 'text' }]);
  assert.throws(() => decode(new Uint8Array([0xff])), /`in`: the payload is not valid UTF-8/);
  assert.throws(() => text(42), emitRefusal('out'));

  const bytes = codecSlots([], [{ codec: 'bytes' }])[1];
  assert.throws(() => bytes(42), emitRefusal('out'));
  assert.throws(() => bytes('hello'), emitRefusal('out'));
});

test('a value JSON has no form for is refused naming the channel', () => {
  const encode = codecSlots()[1];
  assert.throws(() => encode(10n), emitRefusal('out'));
  assert.throws(() => encode(undefined), emitRefusal('out'));
});

test('an unknown codec name is refused naming the operator, the channel and the names', () => {
  for (const [input, sink, channel] of [
    [[{ codec: 'yaml' }], [], 'in'],
    [[], [{ codec: 'yaml' }], 'out'],
  ]) {
    const error = refusal(() => codecSpec(input, sink));
    assert.equal(error.code, 'topology.unknown_codec');
    assert.deepEqual(error.details, {
      channel,
      codec: 'yaml',
      codecs: ['bytes', 'text', 'json', 'msgpack'],
    });
  }
});

test('a codec and a callable together are refused', () => {
  const unnamed = () =>
    defineOperator({
      factoryId: 'codecs',
      channels: (c) => {
        c.input('in')
          .into(echo('echo'))
          .sink((value) => value, { codec: 'text' });
      },
    });
  for (const [build, channel] of [
    [() => codecSpec([(bytes) => bytes, { codec: 'text' }]), 'in'],
    [() => codecSpec([], [(value) => value, { codec: 'text' }]), 'out'],
    [unnamed, 'echo'],
  ]) {
    const error = refusal(build);
    assert.equal(error.code, 'topology.codec_with_callable');
    assert.deepEqual(error.details, { channel });
  }
});

// -- merge and the latest-document router ------------------------------------
//
// A merge records each source as an ordinary chain closed by the core's
// `merge` link; which edges it derives and every refusal are the core's answer.

const identity = (value) => value;

test('two sources merge into one node, each edge applying its own mapping', () => {
  const spec = fixtures.mergedInput;
  assert.deepEqual(derivedChannels(spec), [
    ['ticks', 'entry', null, [0]],
    ['quotes', 'entry', null, [0]],
    ['merged', 'plugin', 0, []],
  ]);
  // The key is evaluated on the value the source's own mapping produced.
  assert.deepEqual(
    spec.channels.slice(0, 2).map(({ consumers: [edge] }) => {
      const mapped = edge.transforms.reduce((value, transform) => transform(value), 'AAPL:1');
      return [mapped.source, edge.keyBy(mapped)];
    }),
    [
      ['ticks', 'AAPL'],
      ['quotes', 'AAPL'],
    ],
  );
});

test('a tasklet output and an entry are both merge sources', () => {
  const spec = defineOperator({
    factoryId: 'merged_hop',
    channels: (c) => {
      const parsed = c.input('raw').into(echo('parse'));
      c.merge()
        .from(parsed, (value) => ({ parsed: value }))
        .from(c.input('direct'), (value) => ({ direct: value }))
        .into(echo('join'))
        .sink();
    },
  });
  assert.deepEqual(derivedChannels(spec), [
    ['raw', 'entry', null, [0]],
    ['parse', 'inter_task', 0, [1]],
    ['direct', 'entry', null, [1]],
    ['join', 'plugin', 1, []],
  ]);
});

test('a merge is refused by the core’s codes', () => {
  const target = echo('target');
  const refused = (channels) =>
    codesOf(refusal(() => defineOperator({ factoryId: 'merge_refused', channels })));

  assert.deepEqual(
    refused((c) => c.merge().from(c.input('in'), identity).into(target)),
    ['topology.merge_too_few_sources'],
  );
  assert.ok(
    refused((c) => {
      const entry = c.input('in');
      c.merge().from(entry, identity).from(entry, identity).into(target);
    }).includes('topology.duplicate_merge_source'),
  );
  assert.deepEqual(
    refused((c) => {
      const entry = c.input('in');
      entry.into(target);
      c.merge().from(entry, identity).from(c.input('other'), identity).into(target);
    }),
    ['topology.edge_already_wired'],
  );
  const keyed = echo('keyed');
  assert.deepEqual(
    refused((c) =>
      c
        .merge()
        .from(c.input('in'), identity)
        .from(c.input('other'), identity)
        .keyBy(String)
        .keyBy(String)
        .into(keyed),
    ),
    ['topology.duplicate_key_by', 'topology.duplicate_key_by'],
  );
});

test('the router is a merge into the route_latest node, and a named one is its own node', () => {
  const spec = defineOperator({
    factoryId: 'routers',
    channels: (c) => {
      const trades = c.input('trades');
      const bySymbol = (trade) => trade.symbol;
      c.routeLatest(c.input('config'), trades, bySymbol).into(echo('detector'));
      c.routeLatestNamed(c.input('limits'), trades, bySymbol, 'limits_router').into(echo('guard'));
    },
  });
  assert.deepEqual(derivedChannels(spec), [
    ['trades', 'entry', null, [0, 2]],
    ['config', 'entry', null, [0]],
    ['route_latest', 'inter_task', 0, [1]],
    ['limits', 'entry', null, [2]],
    ['limits_router', 'inter_task', 2, [3]],
  ]);
  assert.deepEqual(
    spec.tasklets.map((node) => node.name),
    ['route_latest', 'detector', 'limits_router', 'guard'],
  );
});

test('the router hands each key each document once, on that key’s own traffic', async () => {
  const spec = defineOperator({
    factoryId: 'routed',
    channels: (c) => {
      c.routeLatest(c.input('config'), c.input('trades'), (trade) => trade.symbol).into(
        echo('detector'),
      );
    },
  });
  const router = spec.tasklets.find((node) => node.name === 'route_latest');
  const harness = TaskletTestHarness.of(router.tasklet);
  const document = { limit: 5 };

  // Traffic before any document has nothing to hand on.
  assert.deepEqual((await harness.input({ key: 'A' })).emits, []);

  const emits = [];
  for (const input of [
    { document },
    { key: 'A' },
    { key: 'B' },
    // An equal document is inert, whichever object carries it.
    { document: { limit: 5 } },
    { key: 'A' },
  ]) {
    emits.push((await harness.input(input)).emits);
  }
  assert.deepEqual(emits, [[], [{ key: 'A', document }], [{ key: 'B', document }], [], []]);
  assert.deepEqual(harness.currentState, { document, version: 1, delivered: { A: 1, B: 1 } });

  const changed = { limit: 7 };
  assert.deepEqual((await harness.input({ document: changed })).emits, []);
  assert.deepEqual((await harness.input({ key: 'A' })).emits, [{ key: 'A', document: changed }]);
  assert.equal(harness.currentState.version, 2);
  harness.replayEquals();
});

// --- the §5 short form ------------------------------------------------------
//
// An operator definition carrying one tasklet's `run` body in place of a
// `channels` body is a whole operator of the function's kind, built on the
// same graph path a hand-written one is built on. `topology.sink_without_output` is not
// among the refusals: no `run` returns output and the output type is erased
// before the SDK runs, so nothing here says whether a short form emits. The
// manifest leaves `shorthand_typed_output` off and the row skips by name.

const upper = (input, ctx) => ctx.emit(input.toUpperCase());

/** What the core is handed, and what it derives from it. */
const declared = (spec) => ({ decl: toHostClassDecl(spec), derived: validate(spec) });

// Each twin is the full form: a `channels` body still declares one.
test('a flow short form is its hand-written twin', () => {
  const short = defineOperator({ factoryId: 'upcase', sink: 'out', codec: 'text', run: upper });
  const twin = defineOperator({
    factoryId: 'upcase',
    channels: (c) => {
      c.input('input', { codec: 'text' })
        .into(defineTasklet({ name: 'upcase', run: upper }))
        .sink('out');
    },
  });

  assert.equal(short.run, undefined);
  assert.deepEqual(declared(short), declared(twin));
});

test('a steps short form is its hand-written steps twin', () => {
  const short = defineOperator({
    factoryId: 'place',
    steps: {},
    sink: 'placed',
    input: 'order',
    codec: 'text',
    run: upper,
  });
  const twin = defineOperator({
    factoryId: 'place',
    steps: {},
    channels: (c) => {
      c.input('order', { codec: 'text' })
        .into(defineTasklet({ name: 'place', run: upper }))
        .sink('placed');
    },
  });

  assert.deepEqual(short.run, { steps: {} });
  assert.deepEqual(declared(short), declared(twin));
});

test('a short form takes the class settings the full form takes', () => {
  const short = defineOperator({
    factoryId: 'chat',
    steps: { each: 'per_input' },
    durable: { local: { sleepAfter: 2_000 } },
    maxDurationMs: 60_000,
    sink: 'replies',
    codec: 'text',
    run: upper,
  });
  const twin = defineOperator({
    factoryId: 'chat',
    steps: { each: 'per_input' },
    durable: { local: { sleepAfter: 2_000 } },
    maxDurationMs: 60_000,
    channels: (c) => {
      c.input('input', { codec: 'text' })
        .into(defineTasklet({ name: 'chat', run: upper }))
        .sink('replies');
    },
  });

  assert.deepEqual(toHostClassDecl(short).run, { steps: { each: 'per_input' } });
  assert.deepEqual(declared(short), declared(twin));
});

test('every operator is declared with defineOperator, the one operator export', () => {
  assert.equal(sdk.defineOperator, defineOperator);
  assert.deepEqual(
    Object.keys(sdk).filter((name) => /^define.*Operator|Operator$/.test(name)),
    ['LocalOperator', 'defineOperator'],
  );
});

test('a short form with no sink wires its entry into the tasklet and no plugin channel', async () => {
  const stateOnly = fixtures.sugarStateOnly;

  assert.deepEqual(
    derivedChannels(stateOnly).map(([name, kind, , consumers]) => [name, kind, consumers]),
    [['ticks', 'entry', [0]]],
  );
  assert.deepEqual(
    stateOnly.tasklets.map((node) => node.name),
    ['sugar_state_only'],
  );
  assert.equal(stateOnly.channels[0].decode(Buffer.from('tick')), 'tick', 'a codec needs no sink');

  const harness = TaskletTestHarness.of(stateOnly.tasklets[0].tasklet);
  await harness.input('one');
  assert.deepEqual(harness.currentState, { seen: ['one'] });
});

test('every tasklet field a short form carries reaches its generated tasklet', () => {
  const codec = { encode: (value) => encodeValue(value), decode: () => ({ n: 0 }) };
  const keyed = {
    cells: { keys: ['a', 'b'], cellsFor: () => ['a'] },
    logicalClock: true,
    mailboxCapacity: 7,
    schemaVersion: 3,
    minReadableSchemaVersion: 2,
    migrateState: () => ({ n: 0 }),
    migrateChange: () => 1,
    initState: () => ({ n: 0 }),
    applyChange: (state, change) => {
      state.n += change;
    },
    stateCodec: codec,
    changeCodec: codec,
    onTimer: () => {},
    compensate: () => {},
    run: upper,
  };
  const dynamic = {
    idleTtlSeconds: 60,
    maxCells: 10,
    eventTime: () => 0n,
    run: upper,
  };

  for (const [settings, fields] of [
    [{ steps: {} }, keyed],
    [{}, dynamic],
  ]) {
    const node = defineOperator({ factoryId: 'fielded', sink: 'out', ...settings, ...fields })
      .tasklets[0];
    assert.equal(node.name, 'fielded');
    for (const [field, value] of Object.entries(fields)) {
      assert.equal(node.tasklet.definition[field], value, `${field} reached the tasklet`);
    }
  }
});

test('a short form names its tasklet after its factory id, and a name is refused', () => {
  for (const settings of [{}, { steps: {} }]) {
    const error = refusal(() =>
      defineOperator({ factoryId: 'short', sink: 'out', run: upper, name: 'other', ...settings }),
    );
    assert.equal(error.code, 'topology.setting_on_short_form', error.message);
    assert.deepEqual(error.details, { factory_id: 'short', fields: '`name`' });
  }
});

test('a retired class setting is refused under the operator, naming it', () => {
  for (const field of ['persistent', 'effectivelyOnce', 'correctionWindow']) {
    for (const body of [
      { sink: 'out', run: upper },
      { channels: (c) => c.input('in').into(echo('p')) },
    ]) {
      const error = refusal(() => defineOperator({ factoryId: 'retired', [field]: true, ...body }));
      assert.equal(error.code, 'topology.unknown_field', error.message);
      assert.deepEqual(error.details, {
        declaration: "operator 'retired'",
        fields: `\`${field}\``,
      });
    }
  }
});

test('a short-form key or a tasklet field beside a channels body is refused, naming it', () => {
  for (const settings of [{}, { steps: {} }]) {
    for (const [key, value] of [
      ['sink', 'out'],
      ['input', 'prices'],
      ['codec', 'text'],
      ['mailboxCapacity', 8],
      ['initState', () => ({ n: 0 })],
      ['cells', { keys: ['a'] }],
    ]) {
      const error = refusal(() =>
        defineOperator({
          factoryId: 'full',
          ...settings,
          [key]: value,
          channels: (c) => c.input('in').into(echo('p')),
        }),
      );
      assert.equal(error.code, 'topology.short_form_field_on_full_form', error.message);
      assert.deepEqual(error.details, { factory_id: 'full', fields: `\`${key}\`` });
    }
  }
});

test('a run body beside a channels body is refused as topology.short_form_with_topology', () => {
  for (const settings of [{}, { steps: {} }]) {
    const error = refusal(() =>
      defineOperator({
        factoryId: 'both',
        ...settings,
        sink: 'out',
        run: upper,
        channels: () => {},
      }),
    );
    assert.equal(error.code, 'topology.short_form_with_topology');
    assert.ok(error.message.includes('topology.short_form_with_topology'), error.message);
    assert.deepEqual(error.details, { factory_id: 'both' });
  }
});

test('a definition with neither body is refused, naming both', () => {
  for (const settings of [{}, { steps: {} }]) {
    const error = refusal(() => defineOperator({ factoryId: 'bodiless', ...settings }));
    assert.equal(error.code, 'topology.missing_body');
    assert.equal(error.details.declaration, "operator 'bodiless'");
    assert.ok(
      error.message.includes('`run`') && error.message.includes('`channels`'),
      error.message,
    );
  }
});

test('a definition with no factory id is refused, naming it', () => {
  for (const factoryId of [undefined, '', '  ']) {
    const error = refusal(() => defineOperator({ factoryId, sink: 'out', run: upper }));
    assert.equal(error.code, 'topology.missing_factory_id');
  }
});

test('an operator field on a tasklet is refused, naming it and defineOperator', () => {
  for (const [field, value] of [
    ['steps', {}],
    ['durable', false],
    ['sink', 'out'],
    ['input', 'prices'],
    ['codec', 'text'],
  ]) {
    const error = refusal(() => defineTasklet({ name: 'upcase', [field]: value, run: upper }));
    assert.equal(error.code, 'topology.operator_field_on_tasklet');
    assert.equal(error.details.fields, `\`${field}\``);
    assert.ok(error.message.includes('`defineOperator`'), error.message);
  }
});

test('a short form under a factory id already held is refused across runs', () => {
  const registry = new Registry();
  registry.add(defineOperator({ factoryId: 'twice', sink: 'out', run: upper }));
  assert.throws(
    () =>
      registry.add(
        defineOperator({ factoryId: 'twice', steps: {}, sink: 'twin_sink', run: upper }),
      ),
    (error) =>
      error.message.startsWith('topology.duplicate_factory_id') &&
      error.details.factory_id === 'twice',
  );
});

// --- the tasklet body -----------------------------------------------------

test("a tasklet's body is its run field", async () => {
  const Upcase = defineTasklet({ name: 'upcase', run: upper });
  assert.deepEqual((await TaskletTestHarness.of(Upcase).input('abc')).emits, ['ABC']);
});

test('a tasklet carrying the old handle field is refused, naming run', () => {
  const error = refusal(() => defineTasklet({ name: 'upcase', handle: upper }));
  assert.equal(error.code, 'topology.retired_callback');
  assert.equal(error.details.instead, '`run`');
});
