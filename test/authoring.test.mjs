// The graph builder, what it records, and the refusals it still makes itself.
//
// Every topology rule now lives in `flz_core::host::register::check`, and
// `test/fixtures/reference_topologies.txt` is where each one's case lives — one
// row per refusal, read by every SDK's conformance run. What is left here is
// the authoring surface: the shape a declaration builds, the names it derives,
// which closure reaches which edge, and the two defects the record cannot
// carry. `validate` is the call that submits, so it needs the process context.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  DeclarationError,
  defineStream,
  defineTasklet,
  defineWorkflow,
  encodeValue,
  loadNative,
  Registry,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import * as sdk from '../dist/esm/index.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';
import { EmitError } from '../dist/esm/types.js';

import { member } from './contract.mjs';
import { testAppModule } from './test-app.mjs';

loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const fixtures = await testAppModule('fixtures.mjs');

const echo = (name, options = {}) => defineTasklet({ name, ...options, run: (input) => input });

const refusal = (build) => {
  try {
    const spec = build();
    validate(spec);
  } catch (error) {
    assert.ok(error instanceof DeclarationError, `expected a DeclarationError, got ${error}`);
    return error;
  }
  assert.fail('expected a refusal');
};

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
  const spec = defineStream({
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
    dynamicCells: { idleTtlMs: 60_000, maxCells: 100 },
    initialState: () => ({ seen: 0 }),
    applyChange: (state, change) => {
      state.seen += change.by;
    },
    run: (input, ctx) => {
      ctx.change({ by: 1 });
      return input;
    },
  });
  const spec = defineStream({
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
  assert.equal(decl.channels[1].keyed, true);
});

// The record carries `change_is_nochange` per tasklet: a handler that folds
// no change has the empty Change type, and a logical clock over it never
// advances. The refusal is the core's `UnservableClock`; what is asserted here
// is that this builder writes the fact it reads.
test('a tasklet that folds no change declares it, and a logical clock over it is refused', () => {
  const spec = defineStream({
    factoryId: 'unservable',
    channels: (c) =>
      c
        .input('in')
        .into(defineTasklet({ name: 'ticker', clock: 'logical', run: (i) => i }))
        .sink(),
  });
  assert.equal(toHostClassDecl(spec).tasklets[0].change_is_nochange, true);
  assert.deepEqual(refusal(() => spec).refusals, ['UnservableClock']);
});

// The verdict itself: one exception carrying every name the core answered with.
test('a refusal reaches the author under the core’s own names', () => {
  const target = echo('target');
  const refused = refusal(() =>
    defineStream({
      factoryId: 'edge_already_wired',
      channels: (c) => {
        const edge = c.input('in');
        edge.into(target);
        edge.map((value) => `${value}!`).into(target);
      },
    }),
  );
  assert.deepEqual(refused.refusals, ['EdgeAlreadyWired']);
  assert.match(
    refused.message,
    /^edge_already_wired: topology rejected \[EdgeAlreadyWired\]:\n {2}- /,
  );
  assert.match(refused.message, /already feeds tasklet `target`/);
});

// The one refusal this builder raises under a core name reads the way the core's do.
test('a node identity collision is refused under the core’s name, in the message too', () => {
  const refused = refusal(() =>
    defineStream({
      factoryId: 'collision',
      channels: (c) => {
        const entry = c.input('in');
        entry.into(echo('twin'));
        entry.into(echo('twin'));
      },
    }),
  );
  assert.deepEqual(refused.refusals, ['NodeIdentityCollision']);
  assert.match(
    refused.message,
    /^collision: twin: topology rejected \[NodeIdentityCollision\]:\n {2}- two different tasklets/,
  );
});

test('a named edge names the channel, not the consuming node', () => {
  const first = echo('first');
  const second = echo('second');
  const spec = defineStream({
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
  const spec = defineStream({
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
    const first = defineTasklet({ name: 'same', run: (i) => i });
    const second = defineTasklet({ name: 'same', run: (i) => i });
    return defineStream({
      factoryId: 'collide',
      channels: (c) => {
        c.input('in').branch([(edge) => edge.into(first), (edge) => edge.into(second)]);
      },
    });
  });
  assert.match(refused.message, /node identity/);
});

test('an event-time clock without an extractor is refused', () => {
  const refused = refusal(() => {
    const task = defineTasklet({ name: 'windowed', clock: 'event_time', run: (i) => i });
    return defineStream({
      factoryId: 'no_extractor',
      channels: (c) => c.input('in').into(task).sink(),
    });
  });
  assert.match(
    refused.message,
    /windowed: declares an event_time clock but no eventTime extractor/,
  );
});

// KI-122: three options, one implication. Each writes its own raw flag and the
// core derives the acknowledgement granularity from any of them, so no order
// of calls can clear another's.
test('the checkpoint options compose in any order, and none clears another', () => {
  const spec = defineStream({
    factoryId: 'compose',
    channels: (c) => {
      c.effectivelyOnce();
      c.outputsAtCheckpoint(250);
      c.checkpointAck();
      c.checkpointEvery(4);
      c.input('in').into(echo('p')).keyed().sink();
    },
  });
  assert.equal(validate(spec).ack_granularity, 'checkpoint');
  const decl = toHostClassDecl(spec);
  assert.equal(decl.checkpoint_ack, true);
  assert.equal(decl.checkpoint_every, 4);
  assert.equal(decl.checkpoint_outputs, 'at_checkpoint');
  assert.equal(decl.checkpoint_max_delay_ms, 250);
});

test('two tasklets each terminate on their own sink, named by their node', () => {
  const a = echo('a');
  const b = echo('b');
  const spec = defineStream({
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
  const named = defineStream({
    factoryId: 'sink_named',
    channels: (c) => {
      c.input('in').into(echo('producer')).sink('items_output');
    },
  });
  const derived = defineStream({
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
  const spec = defineStream({
    factoryId: 'named_twice',
    channels: (c) => {
      c.input('in').into(echo('producer')).named('a').sink('b');
    },
  });
  assert.throws(
    () => validate(spec),
    (error) => /ChannelNamedTwice/.test(error.message) && /both name it/.test(error.message),
  );
});

test('a channel option with nothing wired after it still reaches the channel', () => {
  const renamed = defineStream({
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
    defineStream({
      factoryId: 'trailing_keyed',
      channels: (c) => {
        c.input('in').into(echo('producer')).keyed();
      },
    }),
  );
  assert.deepEqual([...keyed.refusals], ['KeyedChannelWithoutPluginWire']);
});

test('two channels naming one sink is refused as a duplicate sink name', () => {
  const spec = defineStream({
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
    (error) => /DuplicateSinkName/.test(error.message),
  );
});

test('a workflow declares its lifecycle knobs beside its tasklets', () => {
  const spec = defineWorkflow({
    factoryId: 'saga',
    loopMode: 'chained',
    rollbackMode: 'keep_progress',
    maxDurationMs: 60_000,
    channels: (c) => c.input('in').into(echo('p')),
  });
  validate(spec);
  const decl = toHostClassDecl(spec);
  assert.equal(decl.workflow.loop_mode, 'chained');
  assert.equal(decl.workflow.rollback_mode, 'keep_progress');
  assert.equal(decl.workflow.max_duration_ms, 60_000);
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
    initialState: () => ({ comparable: 'default' }),
    run: (input, ctx) => (ctx.state.comparable === input ? input : undefined),
  });
  const harness = TaskletTestHarness.of(ReadOnly);
  assert.deepEqual((await harness.input('default')).emits, ['default']);
  assert.deepEqual((await harness.input('other')).emits, []);

  const Recording = defineTasklet({
    name: 'recording',
    initialState: () => ({ n: 0 }),
    run: (_input, ctx) => ctx.change({ by: 1 }),
  });
  await assert.rejects(
    () => TaskletTestHarness.of(Recording).input(1),
    (error) => /declares no applyChange/.test(error.message),
  );
});

test('applyChange with no state to fold into is refused at definition', () => {
  assert.throws(
    () => defineTasklet({ name: 'nowhere', applyChange: () => {}, run: () => {} }),
    (error) => /no initialState/.test(error.message),
  );
});

test('a declared state that cannot be encoded is refused at definition', () => {
  assert.throws(
    () =>
      defineTasklet({
        name: 'unencodable',
        initialState: () => ({ pending: new Map() }),
        applyChange: () => {},
        run: () => {},
      }),
    (error) => /unencodable\.state\.pending/.test(error.message),
  );
});

test('a sink names the channel a forwarded frame carries', () => {
  // The channel name is what a forwarded frame carries and what selects the
  // receiving operative's entry channel, so the name an author writes on the
  // sink has to be the one that rides the wire.
  const spec = defineStream({
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
  const spec = defineStream({
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
  const spec = defineStream({
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
        defineStream({
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
  const spec = defineStream({
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

test('keyed() reaches the channel of a handle that also declares a sink', () => {
  const spec = defineStream({
    factoryId: 'keyed_hybrid',
    channels: (c) => {
      c.input('in').into(echo('producer')).keyed().sink('mid').into(echo('consumer'));
    },
  });
  validate(spec);
  assert.equal(toHostClassDecl(spec).channels[1].keyed, true);
});

test('a transform declared before one into(...) reaches that consumer only', () => {
  const spec = defineStream({
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
  const spec = defineStream({
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

test('a stream-only builder call on a workflow reaches the core and is refused by its name', () => {
  for (const [name, call] of [
    ['EffectivelyOnceOnWorkflow', (c) => c.effectivelyOnce()],
    ['CheckpointOnWorkflow', (c) => c.checkpointEvery(500)],
    ['CorrectionWindowOnWorkflow', (c) => c.correctionWindow(60_000)],
  ]) {
    const { refusals, message } = refusal(() =>
      defineWorkflow({
        factoryId: 'w',
        channels: (c) => {
          call(c);
          c.input('in').into(echo('p'));
        },
      }),
    );
    assert.deepEqual(refusals, [name], message);
    assert.ok(message.includes(name), message);
  }
});

test('synchronous durability is the definition field, declared by a workflow, refused on a stream', () => {
  const channels = (c) => {
    assert.equal(c.syncDurability, undefined, 'the builder carries no syncDurability call');
    c.input('in').into(echo('p'));
  };
  const workflow = defineWorkflow({ factoryId: 'sd', syncDurability: true, channels });
  validate(workflow);
  assert.equal(toHostClassDecl(workflow).sync_durability, true);

  const { refusals, message } = refusal(() =>
    defineStream({ factoryId: 'sd_stream', syncDurability: true, channels }),
  );
  assert.deepEqual(refusals, ['SyncDurabilityOnStream'], message);
});

test('sleeping is a class setting of both kinds, defaulting to persistence on a workflow only', () => {
  const channels = (c) => c.input('in').into(echo('p'));
  const sleeps = (spec) => toHostClassDecl(spec).sleep_when_idle;
  assert.equal(sleeps(defineWorkflow({ factoryId: 'w', channels })), true);
  assert.equal(sleeps(defineWorkflow({ factoryId: 'w', persistent: false, channels })), false);
  assert.equal(sleeps(defineStream({ factoryId: 's', channels })), false);
  assert.equal(sleeps(defineStream({ factoryId: 's', sleepWhenIdle: true, channels })), true);

  for (const define of [defineStream, defineWorkflow]) {
    const { message } = refusal(() =>
      define({ factoryId: 'nap', persistent: false, sleepWhenIdle: true, channels }),
    );
    assert.ok(message.includes('SleepWithoutPersistence'), message);
  }
});

test('a workflow setting on a stream is refused in either form, naming it', () => {
  const forms = {
    full: { channels: (c) => c.input('in').into(echo('p')) },
    short: { sink: 'out', run: (input) => input },
  };
  for (const [field, value] of [
    ['loopMode', 'per_input'],
    ['rollbackMode', 'keep_progress'],
    ['maxDurationMs', 60_000],
    ['wakeAfterMs', 1_000],
  ]) {
    for (const [form, body] of Object.entries(forms)) {
      assert.throws(
        () => defineStream({ factoryId: 'streamed', [field]: value, ...body }),
        (error) =>
          error instanceof DeclarationError &&
          error.factoryId === 'streamed' &&
          error.message.includes(`\`${field}\``) &&
          error.message.includes('only a workflow carries'),
        `${field} on a ${form}-form stream`,
      );
    }
  }
});

test('an unknown loop mode or rollback mode is refused at definition, naming the value', () => {
  const channels = (c) => c.input('in').into(echo('p'));
  for (const [field, value, form] of [
    ['loopMode', 'forever', { channels }],
    ['rollbackMode', 'sometimes', { channels }],
    ['loopMode', 'forever', { sink: 'out', run: (input) => input }],
  ]) {
    assert.throws(
      () => defineWorkflow({ factoryId: 'moded', [field]: value, ...form }),
      (error) =>
        error instanceof DeclarationError &&
        error.message.includes(`\`${value}\``) &&
        error.message.includes(field),
    );
  }
});

// -- channel codecs -------------------------------------------------------------
//
// A codec is resolved when the class is declared, into the two slots the
// dispatcher and the plugin frame already read. What every SDK sends alike is
// `test/fixtures/channel_codecs.txt`; what is Node's own is asserted here.

const CODEC_CASES = member('test/fixtures/channel_codecs.txt');

const codecSpec = (input = [], sink = []) =>
  defineStream({
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
  error instanceof EmitError && error.message.includes(`\`${channel}\``);

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

test('an unknown codec name is refused naming the operative, the channel and the names', () => {
  for (const [input, sink, channel] of [
    [[{ codec: 'yaml' }], [], 'in'],
    [[], [{ codec: 'yaml' }], 'out'],
  ]) {
    const { message } = refusal(() => codecSpec(input, sink));
    assert.ok(message.includes(`codecs: ${channel}: unknown codec \`yaml\``), message);
    assert.ok(message.includes('bytes, text, json, msgpack'), message);
  }
});

test('a codec and a callable together are refused', () => {
  const unnamed = () =>
    defineStream({
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
    const { message } = refusal(build);
    assert.ok(message.includes(`codecs: ${channel}: names both a codec`), message);
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
  const spec = defineStream({
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

test('a merge is refused by the core’s names', () => {
  const target = echo('target');
  const refused = (channels) =>
    refusal(() => defineStream({ factoryId: 'merge_refused', channels })).refusals;

  assert.deepEqual(
    refused((c) => c.merge().from(c.input('in'), identity).into(target)),
    ['MergeTooFewSources'],
  );
  assert.ok(
    refused((c) => {
      const entry = c.input('in');
      c.merge().from(entry, identity).from(entry, identity).into(target);
    }).includes('DuplicateMergeSource'),
  );
  assert.deepEqual(
    refused((c) => {
      const entry = c.input('in');
      entry.into(target);
      c.merge().from(entry, identity).from(c.input('other'), identity).into(target);
    }),
    ['EdgeAlreadyWired'],
  );
  const keyed = echo('keyed', { dynamicCells: {} });
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
    ['DuplicateKeyBy', 'DuplicateKeyBy'],
  );
});

test('the router is a merge into the route_latest node, and a named one is its own node', () => {
  const spec = defineStream({
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
  const spec = defineStream({
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
// An operative definition carrying one tasklet's `run` body in place of a
// `channels` body is a whole operative of the function's kind, built on the
// same graph path a hand-written one is built on. `SinkWithoutOutput` is not
// among the refusals: a `.mjs` author runs no type checker and this SDK ships
// no build step, so a body that returns nothing cannot be seen here. The
// manifest leaves `shorthand_typed_output` off and the row skips by name.

const upper = (input) => input.toUpperCase();

/** What the core is handed, and what it derives from it. */
const declared = (spec) => ({ decl: toHostClassDecl(spec), derived: validate(spec) });

// Each twin is the full form: a `channels` body still declares one.
test('a stream short form is its hand-written twin', () => {
  const short = defineStream({ factoryId: 'upcase', sink: 'out', codec: 'text', run: upper });
  const twin = defineStream({
    factoryId: 'upcase',
    channels: (c) => {
      c.input('input', { codec: 'text' })
        .into(defineTasklet({ name: 'upcase', run: upper }))
        .sink('out');
    },
  });

  assert.equal(short.kind, 'stream');
  assert.deepEqual(declared(short), declared(twin));
});

test('a workflow short form is its hand-written once workflow', () => {
  const short = defineWorkflow({
    factoryId: 'place',
    sink: 'placed',
    input: 'order',
    codec: 'text',
    run: upper,
  });
  const twin = defineWorkflow({
    factoryId: 'place',
    channels: (c) => {
      c.input('order', { codec: 'text' })
        .into(defineTasklet({ name: 'place', run: upper }))
        .sink('placed');
    },
  });

  assert.equal(short.kind, 'workflow');
  assert.equal(short.workflow.loopMode, 'once');
  assert.deepEqual(declared(short), declared(twin));
});

test('a workflow short form declares its loop mode as the full form does', () => {
  const short = defineWorkflow({
    factoryId: 'chat',
    sink: 'replies',
    codec: 'text',
    loopMode: 'per_input',
    run: upper,
  });
  const twin = defineWorkflow({
    factoryId: 'chat',
    loopMode: 'per_input',
    channels: (c) => {
      c.input('input', { codec: 'text' })
        .into(defineTasklet({ name: 'chat', run: upper }))
        .sink('replies');
    },
  });

  assert.equal(short.workflow.loopMode, 'per_input');
  assert.equal(toHostClassDecl(short).workflow.loop_mode, 'per_input');
  assert.deepEqual(declared(short), declared(twin));
});

test('the stream is declared with defineStream, and no defineOperative is exported', () => {
  assert.equal(sdk.defineStream, defineStream);
  assert.equal('defineOperative' in sdk, false);
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
    clock: 'logical',
    mailboxCapacity: 7,
    schemaVersion: 3,
    minReadableSchemaVersion: 2,
    migrateState: () => ({ n: 0 }),
    migrateChange: () => 1,
    initialState: () => ({ n: 0 }),
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
    dynamicCells: { idleTtlMs: 60_000, maxCells: 10 },
    clock: 'event_time',
    eventTime: () => 0n,
    run: upper,
  };

  for (const [define, fields] of [
    [defineWorkflow, keyed],
    [defineStream, dynamic],
  ]) {
    const node = define({ factoryId: 'fielded', sink: 'out', ...fields }).tasklets[0];
    assert.equal(node.name, 'fielded');
    for (const [field, value] of Object.entries(fields)) {
      assert.equal(node.tasklet.definition[field], value, `${field} reached the tasklet`);
    }
  }
});

test('a field a short form does not carry is refused under the operative, naming it and why', () => {
  const kind = '`defineStream` or `defineWorkflow`';
  for (const [define, field, value, why] of [
    [defineStream, 'persistent', false, 'full form'],
    [defineStream, 'effectivelyOnce', true, 'full form'],
    [defineStream, 'correctionWindow', 5_000, 'full form'],
    [defineWorkflow, 'rollbackMode', 'keep_progress', 'full form'],
    [defineWorkflow, 'syncDurability', true, 'full form'],
    [defineWorkflow, 'maxDurationMs', 60_000, 'full form'],
    [defineStream, 'name', 'other', 'named after its `factoryId`'],
    [defineWorkflow, 'name', 'other', 'named after its `factoryId`'],
    [defineStream, 'kind', 'stream', kind],
    [defineWorkflow, 'kind', 'workflow', kind],
  ]) {
    const { factoryId, handler, message } = refusal(() =>
      define({ factoryId: 'short', sink: 'out', run: upper, [field]: value }),
    );
    assert.deepEqual([factoryId, handler], ['short', null]);
    assert.ok(message.includes(`\`${field}\``) && message.includes(why), message);
  }
});

test('a short-form key or a tasklet field beside a channels body is refused, naming it', () => {
  for (const define of [defineStream, defineWorkflow]) {
    for (const [key, value] of [
      ['sink', 'out'],
      ['input', 'prices'],
      ['codec', 'text'],
      ['mailboxCapacity', 8],
      ['initialState', () => ({ n: 0 })],
      ['cells', { keys: ['a'] }],
    ]) {
      const { message } = refusal(() =>
        define({ factoryId: 'full', [key]: value, channels: (c) => c.input('in').into(echo('p')) }),
      );
      assert.ok(message.includes(`\`${key}\``) && message.includes('full form wires'), message);
    }
  }
});

test('a run body beside a channels body is refused as ShortFormWithTopology', () => {
  for (const define of [defineStream, defineWorkflow]) {
    const { refusals, message } = refusal(() =>
      define({ factoryId: 'both', sink: 'out', run: upper, channels: () => {} }),
    );
    assert.ok(
      refusals.includes('ShortFormWithTopology') && message.includes('ShortFormWithTopology'),
      message,
    );
  }
});

test('a definition with neither body is refused, naming both', () => {
  for (const define of [defineStream, defineWorkflow]) {
    const { factoryId, message } = refusal(() => define({ factoryId: 'bodiless' }));
    assert.equal(factoryId, 'bodiless');
    assert.ok(message.includes('`run`') && message.includes('`channels`'), message);
  }
});

test('a definition with no factory id is refused, naming it', () => {
  for (const factoryId of [undefined, '', '  ']) {
    const { message } = refusal(() => defineStream({ factoryId, sink: 'out', run: upper }));
    assert.ok(message.includes('`factoryId`'), message);
  }
});

test('an operative field on a tasklet is refused, naming it and defineStream', () => {
  for (const [field, value] of [
    ['kind', 'stream'],
    ['sink', 'out'],
    ['input', 'prices'],
    ['codec', 'text'],
  ]) {
    const { message } = refusal(() =>
      defineTasklet({ name: 'upcase', [field]: value, run: upper }),
    );
    assert.ok(message.includes(`\`${field}\``) && message.includes('`defineStream`'), message);
  }
});

test('a short form under a factory id already held is refused across kinds', () => {
  const registry = new Registry();
  registry.add(defineStream({ factoryId: 'twice', sink: 'out', run: upper }));
  assert.throws(
    () => registry.add(defineWorkflow({ factoryId: 'twice', sink: 'twin_sink', run: upper })),
    (error) => error.message.startsWith('DuplicateFactoryId') && error.message.includes('twice'),
  );
});

// --- the tasklet body -----------------------------------------------------

test("a tasklet's body is its run field", async () => {
  const Upcase = defineTasklet({ name: 'upcase', run: upper });
  assert.deepEqual((await TaskletTestHarness.of(Upcase).input('abc')).emits, ['ABC']);
});

test('a tasklet carrying the old handle field is refused, naming run', () => {
  const { message } = refusal(() => defineTasklet({ name: 'upcase', handle: upper }));
  assert.ok(message.includes('`run`'), message);
});
