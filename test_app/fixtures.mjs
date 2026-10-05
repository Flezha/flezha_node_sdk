// Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
// under the same factory ids, tasklet names, channel names and output bytes.
//
// Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
// reference: every entry names the text codec, and a sink naming none sends a
// string as its UTF-8 bytes. An inter-tasklet hop between two Node
// tasklets carries MessagePack, which no one else reads.

import { BroadcastChannel } from 'node:worker_threads';

import { defineRelay, defineTasklet } from '@flezha/sdk';

import { fixtureDurable, prefix } from './fixture_support.mjs';

// The persisted-state family is registered from its own module.
export * from './fixtures_state.mjs';

// -- do_nothing / bench_echo ------------------------------------------------

const DoNothing = defineTasklet({
  name: 'do_nothing_tasklet',
  run: (input, ctx) => ctx.emit(input),
});

export const doNothing = defineRelay({
  factoryId: 'do_nothing',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(DoNothing).sink('echo');
  },
});

// Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
// merge-blocking bench factory and must not change; this keeps the round-trip
// bench's back-pressure posture independent of it.
export const benchEcho = defineRelay({
  factoryId: 'bench_echo',
  channels: (c) => {
    c.input('input', { codec: 'text' }).onFull('drop_nothing').into(DoNothing).sink('echo');
  },
});

// -- complete_flow / chained_steps -----------------------------------------

// A flow class that echoes each input on `complete_flow_out` and ends its
// instance on `bye`, the echo of `bye` included.
const CompleteFlow = defineTasklet({
  name: 'complete_flow_tasklet',
  run: (input, ctx) => {
    if (input === 'bye') ctx.complete(input);
    ctx.emit(input);
  },
});

export const completeFlow = defineRelay({
  factoryId: 'complete_flow',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(CompleteFlow).sink('complete_flow_out');
  },
});

// A durable chained loop: each pass prints `chained_pass <n>` and hands the
// next pass `n - 1` on its own channel; the pass at zero hands nothing, and
// the empty batch ends the instance.
const ChainedCountdown = defineTasklet({
  name: 'chained_countdown',
  run: (input, ctx) => {
    console.log(`chained_pass ${input}`);
    const n = Number.parseInt(input, 10);
    if (n > 0) ctx.emit(String(n - 1));
  },
});

export const chainedSteps = defineRelay({
  factoryId: 'chained_steps',
  steps: { each: 'chained' },
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(ChainedCountdown).named('carry');
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

export const compareFilter = defineRelay({
  factoryId: 'compare_filter',
  channels: (c) => {
    c.input('input', { codec: 'text' }).into(CompareFilter).sink('sink');
  },
});

// -- static_cell / sugar_static_cell / mixed_relay ------------------

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

export const staticCell = defineRelay({
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
export const sugarStaticCell = defineRelay({
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

export const mixedRelay = defineRelay({
  factoryId: 'mixed_relay',
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
// One relay declaration each, carrying its tasklet's `run` body in place
// of a `channels` body. No tasklet is declared beside them.

export const sugarDoNothing = defineRelay({
  factoryId: 'sugar_do_nothing',
  sink: 'sugar_do_nothing_echo',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

export const sugarStoreInput = defineRelay({
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

export const sugarNamedInput = defineRelay({
  factoryId: 'sugar_named_input',
  sink: 'sugar_named_input_sink',
  input: 'prices',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

export const sugarSteps = defineRelay({
  factoryId: 'sugar_steps',
  steps: {},
  durable: fixtureDurable(),
  sink: 'sugar_steps_placed',
  input: 'order',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

// The one class-level setting a short form writes: one pass per message, so
// the instance runs on after the first.
export const sugarStepsPerInput = defineRelay({
  factoryId: 'sugar_steps_per_input',
  steps: { each: 'per_input' },
  durable: fixtureDurable(),
  sink: 'sugar_steps_per_input_out',
  codec: 'text',
  run: (input, ctx) => ctx.emit(input),
});

// No sink: the entry feeds a tasklet that only keeps state.
export const sugarStateOnly = defineRelay({
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

export const hybridChannel = defineRelay({
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

export const mergedInput = defineRelay({
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

export const transformChain = defineRelay({
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

export const cellStress = defineRelay({
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

export const poisonProbe = defineRelay({
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

export const wedgeProbe = defineRelay({
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

export const candle = defineRelay({
  factoryId: 'candle',
  flow: { exactly: { effectively_once: { correctionMs: 600_000 } } },
  channels: (c) => {
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

export const sessionTimeout = defineRelay({
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
  defineRelay({
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

// -- partition_pass --------------------------------------------------------
//
// The partitioned-relays upstream: keyed on the wiring so it can be
// partitioned at all, and emitting keyed so a partitioned forward target can
// resolve the owning member. The plugin channel is named `input` because a
// forwarded frame keeps its producing channel's name, and that is what selects
// the receiving relay's entry channel — so the entry here is `in`.

const PartitionPass = defineTasklet({
  name: 'partition_pass_tasklet',
  initState: () => ({ seen: [] }),
  applyChange: (state, change) => {
    state.seen.push(change.push);
  },
  run: (input, ctx) => {
    ctx.change({ push: input });
    ctx.emit(input, { key: prefix(input) });
  },
});

export const partitionPass = defineRelay({
  factoryId: 'partition_pass',
  channels: (c) => {
    c.input('in', { codec: 'text' }).keyBy(prefix).into(PartitionPass).keyed().sink('input');
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

export const dynamicCell = defineRelay({
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
// Relays an outside caller calls and whose sink output answers it: from the
// first tasklet, from a downstream one, and from whichever of two filtered
// paths the input took. `call_turn` is the steps
// one, in fixtures_steps.mjs.

export const callEcho = defineRelay({
  factoryId: 'call_echo',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(passThrough('call_echo')).sink();
  },
});

const CallAnswer = defineTasklet({
  name: 'call_answer',
  run: (input, ctx) => ctx.emit(`re:${input}`),
});

export const callDownstream = defineRelay({
  factoryId: 'call_downstream',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(passThrough('call_parse')).into(CallAnswer).sink();
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

export const callSplit = defineRelay({
  factoryId: 'call_split',
  channels: (c) => {
    const admitted = c.input('in', { codec: 'text' }).into(passThrough('call_admit'));
    admitted
      .filter((input) => !input.startsWith('no'))
      .into(CallAccept)
      .sink();
    admitted
      .filter((input) => input.startsWith('no'))
      .into(CallRefuse)
      .sink();
  },
});

// -- parts ------------------------------------------------------------------
//
// For an input `n` the tasklet sends the parts `1` .. `n` and then emits
// `done:<n>` on its sink `answer`, in a flow relay and in a
// `per_input` steps one. `retry:<n>` sends its parts and then fails
// retryably the first time this process sees it, so the retry's parts number
// from 0 again under the next attempt.

const failedOnce = new Set();

const sendParts = (input, ctx) => {
  const count = input.startsWith('retry:') ? input.slice('retry:'.length) : input;
  if (!/^\d+$/.test(count)) {
    throw Object.assign(new Error(`not a part count: ${input}`), { nonRetryable: true });
  }
  for (let part = 1; part <= Number(count); part += 1) ctx.part(String(part));
  if (input.startsWith('retry:') && !failedOnce.has(input)) {
    failedOnce.add(input);
    throw new Error(`first attempt of ${input}`);
  }
  ctx.emit(`done:${count}`);
};

export const partEcho = defineRelay({
  factoryId: 'part_echo',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .retryBackoffMs(25)
      .into(defineTasklet({ name: 'part_echo', run: sendParts }))
      .sink('answer');
  },
});

export const partEchoSteps = defineRelay({
  factoryId: 'part_echo_steps',
  steps: { each: 'per_input' },
  durable: fixtureDurable(),
  maxDurationMs: 600_000,
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .retryBackoffMs(25)
      .into(defineTasklet({ name: 'part_echo_turn', run: sendParts }))
      .sink('answer');
  },
});
