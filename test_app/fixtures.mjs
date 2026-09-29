// Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
// under the same factory ids, tasklet names, channel names and output bytes.
//
// Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
// reference: every entry names the text codec, and a sink naming none sends a
// string as its UTF-8 bytes. An inter-tasklet hop between two Node
// tasklets carries MessagePack, which no one else reads.

import { BroadcastChannel } from 'node:worker_threads';

import { defineUnbounded, defineTasklet, defineBounded } from '@flezha/sdk';

import { prefix } from './fixture_support.mjs';

// The persisted-state family is registered from its own module.
export * from './fixtures_state.mjs';

// -- do_nothing / bench_echo ------------------------------------------------

const DoNothing = defineTasklet({
  name: 'do_nothing_tasklet',
  run: (input, ctx) => ctx.emit(input),
});

export const doNothing = defineUnbounded({
  factoryId: 'do_nothing',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(DoNothing).sink('echo');
  },
});

// Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
// merge-blocking bench factory and must not change; this keeps the round-trip
// bench's back-pressure posture independent of it.
export const benchEcho = defineUnbounded({
  factoryId: 'bench_echo',
  channels: (c) => {
    c.input('input', { codec: 'text' }).onFull('drop_nothing').into(DoNothing).sink('echo');
  },
});

// -- compare_filter ---------------------------------------------------------

// State seeded by the controller's init values and then only read: there is
// nothing to journal, so the tasklet declares no change fold.
const CompareFilter = defineTasklet({
  name: 'compare_filter_tasklet',
  initState: () => ({ comparable: 'default', inverted: false }),
  run: (input, ctx) => {
    if (ctx.state.comparable === input) ctx.emit(input);
  },
});

export const compareFilter = defineUnbounded({
  factoryId: 'compare_filter',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(CompareFilter).sink('sink');
  },
});

// -- static_cell / sugar_static_cell / mixed_operator ------------------

const STATIC_KEYS = ['a', 'b', 'c', 'd'];
const staticCellsFor = (input) => {
  const first = String(input)[0];
  return [STATIC_KEYS.includes(first) ? first : 'd'];
};

const staticEntry = (name) =>
  defineTasklet({
    name,
    cells: { keys: STATIC_KEYS, cellsFor: staticCellsFor },
    initState: () => ({ seen: [] }),
    applyChange: (state, change) => {
      state.seen.push(change.push);
    },
    run: (input, ctx) => {
      ctx.change({ push: input });
      ctx.emit(input);
    },
  });

const passThrough = (name) => defineTasklet({ name, run: (input, ctx) => ctx.emit(input) });

const StaticEntry = staticEntry('static_entry_tasklet');
const StaticDownstream = passThrough('static_downstream_tasklet');

export const staticCell = defineUnbounded({
  factoryId: 'static_cell',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .into(StaticEntry)
      .named('hop')
      .into(StaticDownstream)
      .sink('kafka');
  },
});

const MacroStaticEntry = staticEntry('macro_static_entry_tasklet');
const MacroStaticDownstream = passThrough('macro_static_downstream_tasklet');

// The reference pipeline sugar leaves the hop unnamed and lets the sink name
// the producing channel.
export const sugarStaticCell = defineUnbounded({
  factoryId: 'sugar_static_cell',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .into(MacroStaticEntry)
      .into(MacroStaticDownstream)
      .sink('test_kafka');
  },
});

const MixedMacroEntry = passThrough('mixed_macro_entry_tasklet');
const MixedHandDownstream = passThrough('mixed_hand_downstream');

export const mixedOperator = defineUnbounded({
  factoryId: 'mixed_operator',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .into(MixedMacroEntry)
      .named('hop')
      .into(MixedHandDownstream)
      .sink('kafka');
  },
});

// -- the §5 short forms -----------------------------------------------------
//
// One operator declaration each, carrying its tasklet's `run` body in place
// of a `channels` body; the function's name is its kind. No tasklet is
// declared beside them.

export const sugarDoNothing = defineUnbounded({
  factoryId: 'sugar_do_nothing',
  sink: 'sugar_do_nothing_echo',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

export const sugarStoreInput = defineUnbounded({
  factoryId: 'sugar_store_input',
  sink: 'sugar_store_sink',
  codec: 'text',
  initState: () => ({ text_inputs: [] }),
  applyChange: (state, change) => {
    state.text_inputs.push(change.push);
  },
  run: (input, ctx) => {
    ctx.change({ push: input });
    ctx.emit(input);
  },
});

export const sugarNamedInput = defineUnbounded({
  factoryId: 'sugar_named_input',
  sink: 'sugar_named_input_sink',
  input: 'prices',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

export const sugarBounded = defineBounded({
  factoryId: 'sugar_bounded',
  sink: 'sugar_bounded_placed',
  input: 'order',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

// The one class-level setting a short form writes: one pass per message, so
// the instance runs on after the first.
export const sugarBoundedPerInput = defineBounded({
  factoryId: 'sugar_bounded_per_input',
  sink: 'sugar_bounded_per_input_out',
  codec: 'text',
  loopMode: 'per_input',
  run: (input, ctx) => ctx.emit(input),
});

// No sink: the entry feeds a tasklet that only keeps state.
export const sugarStateOnly = defineUnbounded({
  factoryId: 'sugar_state_only',
  input: 'ticks',
  codec: 'text',
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  run: (input, ctx) => {
    ctx.change({ push: input });
  },
});

// -- hybrid_channel ---------------------------------------------------------
//
// One emit reaching two destinations: an inter-tasklet consumer and a plugin
// wire with its own serializer. The serializer refuses a sentinel value, so an
// user can observe end to end that a refusal is isolated to the emit rather
// than failing the item.

const HYBRID_SENTINEL = 0xdeadbeef;

const HybridProducer = defineTasklet({
  name: 'hybrid_producer_tasklet',
  run: (input, ctx) => {
    const parsed = Number.parseInt(String(input), 10);
    const value = Number.isSafeInteger(parsed) && parsed >= 0 && parsed <= 0xffffffff ? parsed : 0;
    try {
      ctx.emit(value);
    } catch (error) {
      console.error(`hybrid_producer_tasklet: serializer refused an emit: ${error.message}`);
    }
  },
});

const HybridConsumer = defineTasklet({
  name: 'hybrid_consumer_tasklet',
  run: (input) => {
    console.log(`hybrid_consumer_tasklet: received ${input}`);
  },
});

export const hybridChannel = defineUnbounded({
  factoryId: 'hybrid_channel',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .into(HybridProducer)
      .branch([
        // The consumer edge first, so a refused plugin serialization leaves the
        // in-process fan-out already recorded — the reference's isolation.
        (edge) => edge.named('hybrid_indicator').into(HybridConsumer),
        (edge) =>
          edge.sink((value) => {
            if (value === HYBRID_SENTINEL) {
              throw new Error('hybrid_indicators: serializer rejected sentinel 0xDEADBEEF');
            }
            const out = Buffer.alloc(4);
            out.writeUInt32BE(value);
            return out;
          }),
      ]);
  },
});

// -- merged_input -----------------------------------------------------------
//
// Two entry channels merged into one tasklet. Each source's mapping tags the
// value with the channel it came from, the merge keys on the symbol the mapping
// read, and the emitted line is `<source>:<payload>`.

const tagged = (source) => (payload) => ({ source, symbol: prefix(payload), payload });

const MergeLabel = defineTasklet({
  name: 'merge_label_tasklet',
  run: (input, ctx) => ctx.emit(`${input.source}:${input.payload}`),
});

export const mergedInput = defineUnbounded({
  factoryId: 'merged_input',
  channels: (c) => {
    c.merge()
      .from(c.input('ticks', { codec: 'text' }), tagged('ticks'))
      .from(c.input('quotes', { codec: 'text' }), tagged('quotes'))
      .keyBy((value) => value.symbol)
      .into(MergeLabel)
      .keyed()
      .sink('merged');
  },
});

// -- transform_chain --------------------------------------------------------
//
// Every value transform on one edge: `drop` is filtered out, a comma list fans
// out into one value per element, each is upper-cased and keyed on its prefix.

const TransformLabel = passThrough('transform_label_tasklet');

export const transformChain = defineUnbounded({
  factoryId: 'transform_chain',
  channels: (c) => {
    c.input('trades', { codec: 'text' })
      .filter((trade) => trade !== 'drop')
      .flatMap((trade) => trade.split(','))
      .map((trade) => trade.toUpperCase())
      .keyBy(prefix)
      .into(TransformLabel)
      .keyed()
      .sink('transformed');
  },
});

// -- cell_stress ----------------------------------------------------------

const STRESS_KEYS = ['p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'];

const stress = (name, cellsFor) =>
  defineTasklet({
    name,
    cells: { keys: STRESS_KEYS, cellsFor },
    run: (input, ctx) => ctx.emit(input),
  });

const StressA = stress('stress_a', () => STRESS_KEYS);
const StressB = stress('stress_b', () => ['p0']);
const StressC = stress('stress_c', () => ['p0']);
const StressD = stress('stress_d', () => ['p0']);

export const cellStress = defineUnbounded({
  factoryId: 'cell_stress',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .into(StressA)
      .named('a_to_b')
      .into(StressB)
      .named('b_to_c')
      .into(StressC)
      .named('c_to_d')
      .into(StressD)
      .sink('echo');
  },
});

// -- poison_probe / wedge_probe ---------------------------------------------

let antidoteTaken = false;

const PoisonProbe = defineTasklet({
  name: 'poison_probe_tasklet',
  run: (input, ctx) => {
    if (input === 'antidote') antidoteTaken = true;
    if (input === 'poison' && !antidoteTaken) throw new Error('poison payload');
    ctx.emit(`ok:${input}`);
  },
});

export const poisonProbe = defineUnbounded({
  factoryId: 'poison_probe',
  channels: (c) => {
    c.input('input', { codec: 'text' }).retryBackoffMs(25).into(PoisonProbe).sink('out');
  },
});

let wedgeAborted = false;

const WedgeProbe = defineTasklet({
  name: 'wedge_probe_tasklet',
  run: async (input, ctx) => {
    // Holds the dispatch loop forever: the runner stays connected and
    // heartbeating while its dispatch counter freezes — the wedge signature.
    // Synchronous on purpose; an await would free the loop, which is the whole
    // thing this probe must not do.
    if (input === 'wedge') {
      for (;;) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 3_600_000);
    }
    // Ends the activation as violently as this host allows, once per runner,
    // WITHOUT emitting.
    if (input === 'abort' && !wedgeAborted) {
      wedgeAborted = true;
      throw new Error('wedge_probe: killing the dispatch holding this activation');
    }
    ctx.emit(`ok:${input}`);
  },
});

export const wedgeProbe = defineUnbounded({
  factoryId: 'wedge_probe',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(WedgeProbe).sink('out');
  },
});

// -- candle -----------------------------------------------------------------
//
// Late-input correction is about fold ORDER, and the only way to prove that is
// to look at the order. Send 100, 300, then the late 200. Without correction
// the fold order is [100, 300, 200] and the candle is permanently wrong. With
// it, the engine rewinds behind 200, replays the retained input log sorted by
// event time, and the fold order becomes [100, 200, 300].

const Candle = defineTasklet({
  name: 'candle_tasklet',
  eventTime: (input) => Number(String(input).split(':')[0]) || 0,
  // Event times in fold order: the assertion surface.
  initState: () => ({ folded: [] }),
  applyChange: (state, change) => {
    state.folded.push(change.fold);
  },
  run: (input, ctx) => {
    // `ctx.now()` IS the extracted event time under an event-time clock.
    const eventTime = Number(ctx.now());
    ctx.change({ fold: eventTime });
    // One row per candle bucket, so a corrected replay overwrites by key
    // rather than appending a second row.
    const bucket = Math.floor(eventTime / 1000);
    ctx.emit(`${bucket}=${input}`, { key: `candle/${bucket}` });
  },
});

export const candle = defineUnbounded({
  factoryId: 'candle',
  channels: (c) => {
    c.effectivelyOnce().correctionWindow(600_000);
    c.input('input', { codec: 'text' }).into(Candle).keyed().sink('sink');
  },
});

// -- session_timeout --------------------------------------------------------
//
// The worked session-timeout shape: each input pushes the idle deadline out,
// and the firing closes the session. Single-cell so the e2e's crash-resume
// half exercises a durable Cell. The timeout is read from
// `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

const SessionTimeout = defineTasklet({
  name: 'session_timeout_tasklet',
  initState: () => ({ touched: [], closed_at: [] }),
  applyChange: (state, change) => {
    if ('touched' in change) state.touched.push(change.touched);
    else state.closed_at.push(change.closed);
  },
  run: (input, ctx) => {
    ctx.change({ touched: input });
    // A processing-time timer, because the answer to "nothing has arrived"
    // cannot be an event-time one: nothing would advance the clock.
    ctx.timerAfter(Number(process.env.TIMER_TEST_TIMEOUT_MS) || 1000);
  },
  onTimer: (firing, ctx) => {
    const firedAt = Number(firing.scheduledAt);
    ctx.change({ closed: firedAt });
    ctx.emit(`closed@${firedAt}`);
  },
});

export const sessionTimeout = defineUnbounded({
  factoryId: 'session_timeout',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(SessionTimeout).sink('kafka');
  },
});

// -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} -----------------

const boundedMailboxTasklet = (name) =>
  defineTasklet({
    name,
    mailboxCapacity: 4,
    initState: () => ({ processed: 0 }),
    applyChange: (state) => {
      state.processed += 1;
    },
    run: (input, ctx) => {
      ctx.change({ processed: true });
      ctx.emit(input);
    },
  });

const boundedMailbox = (policy) =>
  defineUnbounded({
    factoryId: `bounded_mailbox_${policy}`,
    channels: (c) => {
      c.input(`${policy}_input`, { codec: 'text' })
        .onFull(policy)
        .into(boundedMailboxTasklet(`${policy}_test_tasklet`))
        .sink(`${policy}_echo`);
    },
  });

export const boundedMailboxDropNothing = boundedMailbox('drop_nothing');
export const boundedMailboxDropNewest = boundedMailbox('drop_newest');
export const boundedMailboxDropOldest = boundedMailbox('drop_oldest');

// -- partition_relay --------------------------------------------------------
//
// The partitioned-operators upstream: keyed on the wiring so it can be
// partitioned at all, and emitting keyed so a partitioned forward target can
// resolve the owning member. The plugin channel is named `input` because a
// forwarded frame keeps its producing channel's name, and that is what selects
// the receiving operator's entry channel — so the entry here is `in`.

const PartitionRelay = defineTasklet({
  name: 'partition_relay_tasklet',
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  run: (input, ctx) => {
    ctx.change({ push: input });
    ctx.emit(input, { key: prefix(input) });
  },
});

export const partitionRelay = defineUnbounded({
  factoryId: 'partition_relay',
  channels: (c) => {
    c.input('in', { codec: 'text' }).keyBy(prefix).into(PartitionRelay).keyed().sink('input');
  },
});

// -- dynamic_cell ---------------------------------------------------------

const DynamicCell = defineTasklet({
  name: 'dynamic_cell_tasklet',
  // The e2e sets the TTL in milliseconds for every SDK; this one counts whole seconds.
  idleTtlSeconds: Math.ceil(Number(process.env.DYN_TEST_TTL_MS) / 1000) || undefined,
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  run: (input, ctx) => {
    ctx.change({ push: input });
    ctx.emit(input);
  },
});

export const dynamicCell = defineUnbounded({
  factoryId: 'dynamic_cell',
  channels: (c) => {
    c.input('input', { codec: 'text' })
      .onFull('drop_nothing')
      // Filtered before keying: a dropped value answers no key, so the core
      // never mints a cell for it.
      .filter((input) => input !== 'drop')
      .keyBy(prefix)
      .into(DynamicCell)
      .sink('kafka');
  },
});

// -- call_response ----------------------------------------------------------
//
// Operators an outside caller calls and whose graph answers it through
// `response()`: from the first tasklet, from a downstream one, and from
// whichever of two filtered paths the input took. `call_turn` is the bounded
// one, in fixtures_bounded.mjs.

export const callEcho = defineUnbounded({
  factoryId: 'call_echo',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(passThrough('call_echo')).response();
  },
});

const CallAnswer = defineTasklet({
  name: 'call_answer',
  run: (input, ctx) => ctx.emit(`re:${input}`),
});

export const callDownstream = defineUnbounded({
  factoryId: 'call_downstream',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(passThrough('call_parse')).into(CallAnswer).response();
  },
});

// An input starting `no` takes the refused path, every other the accepted one.
const CallAccept = defineTasklet({
  name: 'call_accept',
  run: (input, ctx) => ctx.emit(`accepted:${input}`),
});
const CallRefuse = defineTasklet({
  name: 'call_refuse',
  run: (input, ctx) => ctx.emit(`refused:${input}`),
});

export const callSplit = defineUnbounded({
  factoryId: 'call_split',
  channels: (c) => {
    const admitted = c.input('in', { codec: 'text' }).into(passThrough('call_admit'));
    admitted
      .filter((input) => !input.startsWith('no'))
      .into(CallAccept)
      .response();
    admitted
      .filter((input) => input.startsWith('no'))
      .into(CallRefuse)
      .response();
  },
});
