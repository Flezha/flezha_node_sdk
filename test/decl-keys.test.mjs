// The declaration key set is a cross-SDK contract pinned in one file. The core
// proves the file matches what it accepts and the Java builder proves it emits
// every key; this is the third reader. A key the core learns that Node cannot
// spell fails here.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  defineStream,
  defineTasklet,
  defineWorkflow,
  loadNative,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';

import { member } from './contract.mjs';

// The derived table below is the core's answer, so this file needs the process
// context `validate` submits through.
loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const FIXTURE = member('test/fixtures/host_class_decl_keys.txt');

function pinned(section) {
  return readFileSync(FIXTURE, 'utf8')
    .split('\n')
    .filter((line) => line && !line.startsWith('#'))
    .filter((line) => line.startsWith(`${section}.`))
    .map((line) => line.slice(section.length + 1));
}

/** Every stream-side option the Node surface has a spelling for, on a class `define` declares. */
function fullyDeclaredStream(define = defineStream) {
  const dynamic = defineTasklet({
    name: 'full_dynamic',
    dynamicCells: { idleTtlMs: 30_000, maxCells: 64 },
    clock: 'event_time',
    eventTime: (input) => BigInt(input.at),
    mailboxCapacity: 512,
    schemaVersion: 2,
    minReadableSchemaVersion: 1,
    migrateState: (_from, bytes) => ({ seen: bytes.length }),
    migrateChange: () => ({ by: 1 }),
    initialState: () => ({ seen: 0 }),
    applyChange: (state, change) => {
      state.seen += change.by;
    },
    compensate: () => {},
    run: (input, ctx) => {
      ctx.change({ by: 1 });
      return input;
    },
  });
  return define({
    factoryId: 'node_decl_full',
    persistent: true,
    sleepWhenIdle: true,
    channels: (c) => {
      c.effectivelyOnce();
      c.checkpointEvery(500);
      c.outputsAtCheckpoint(50);
      c.correctionWindow(600_000);
      c.beyondWindowPolicy('drop');
      c.compactAfterBytes(1_048_576);
      c.input('in')
        .retryMax(3)
        .retryBackoffMs(300)
        .keyBy((input) => input.symbol)
        .into(dynamic)
        .keyed()
        .sink();
    },
  });
}

/** Every static-mode tasklet key, which the dynamic one cannot carry. */
function fullyDeclaredStatic() {
  const statik = defineTasklet({
    name: 'full_static',
    cells: { keys: ['a', 'b'], cellsFor: () => ['a'] },
    run: (input) => input,
  });
  return defineStream({
    factoryId: 'node_decl_static',
    channels: (c) => c.input('in').into(statik).sink(),
  });
}

function fullyDeclaredWorkflow() {
  return defineWorkflow({
    factoryId: 'node_decl_workflow',
    loopMode: 'per_input',
    rollbackMode: 'keep_progress',
    maxDurationMs: 120_000,
    wakeAfterMs: 30_000,
    syncDurability: true,
    channels: (c) => c.input('in').into(defineTasklet({ name: 'step', run: () => {} })),
  });
}

test('every class key the fixture pins has a Node spelling', () => {
  // The kind splits the class keys the way cell mode splits the tasklet
  // ones: only a workflow class carries `workflow`.
  const emitted = new Set(
    [fullyDeclaredStream(), fullyDeclaredWorkflow()]
      .map(toHostClassDecl)
      .flatMap((decl) => Object.keys(decl)),
  );
  const missing = pinned('class').filter((key) => !emitted.has(key));
  assert.deepEqual(
    missing,
    [],
    `class keys the fixture pins that the Node builder never emits: ${missing.join(', ')}`,
  );
});

test('every channel key the fixture pins has a Node spelling', () => {
  const decl = toHostClassDecl(fullyDeclaredStream());
  const emitted = new Set(decl.channels.flatMap((channel) => Object.keys(channel)));
  const missing = pinned('channel').filter((key) => !emitted.has(key));
  assert.deepEqual(missing, [], `channel keys with no Node spelling: ${missing.join(', ')}`);
});

test('every tasklet key the fixture pins has a Node spelling', () => {
  // Cell mode splits the tasklet keys: a dynamic tasklet carries
  // `idle_ttl_ms` and `max_cells`, a static one `cell_keys`, and neither
  // carries the other's.
  const emitted = new Set(
    [fullyDeclaredStream(), fullyDeclaredStatic()]
      .map(toHostClassDecl)
      .flatMap((decl) => decl.tasklets)
      .flatMap((tasklet) => Object.keys(tasklet)),
  );
  const missing = pinned('tasklet').filter((key) => !emitted.has(key));
  assert.deepEqual(missing, [], `tasklet keys with no Node spelling: ${missing.join(', ')}`);
});

test('every workflow key the fixture pins has a Node spelling', () => {
  const decl = toHostClassDecl(fullyDeclaredWorkflow());
  const missing = pinned('workflow').filter((key) => !(key in decl.workflow));
  assert.deepEqual(missing, [], `workflow keys with no Node spelling: ${missing.join(', ')}`);
});

test('the builder invents no key the core never learned', () => {
  // `deny_unknown_fields` on the core's side makes an invented key a parse
  // error naming it; catching it here names it at the source instead.
  const stream = toHostClassDecl(fullyDeclaredStream());
  const workflow = toHostClassDecl(fullyDeclaredWorkflow());
  const check = (section, keys) => {
    const allowed = new Set(pinned(section));
    const extra = [...keys].filter((key) => !allowed.has(key));
    assert.deepEqual(extra, [], `${section} keys the core never learned: ${extra.join(', ')}`);
  };
  check('class', new Set([...Object.keys(stream), ...Object.keys(workflow)]));
  check('channel', new Set(stream.channels.flatMap((c) => Object.keys(c))));
  check(
    'tasklet',
    new Set(
      [stream, toHostClassDecl(fullyDeclaredStatic())]
        .flatMap((decl) => decl.tasklets)
        .flatMap((p) => Object.keys(p)),
    ),
  );
  check('workflow', new Set(Object.keys(workflow.workflow)));
});

// --- the stream-only keys on a workflow class -------------------------------
//
// One builder serves both kinds and neither drops what it recorded:
// `flz_core::host::register::refuse_workflow_facts` is what refuses a tier,
// checkpoint or correction key on a workflow class, by name.

test('a workflow class declares every key its builder recorded, as a stream does', () => {
  const { workflow, ...declared } = toHostClassDecl(fullyDeclaredStream(defineWorkflow));
  assert.ok(workflow, 'a workflow class carries its workflow section');
  assert.deepEqual(declared, toHostClassDecl(fullyDeclaredStream()));
});

// --- the cross-language topology golden ------------------------------------
//
// `test/fixtures/reference_topologies.txt` is the Rust reference's channel
// topology for two fixtures, transcribed by hand. Four SDKs implement one
// design with no compiler between them, so the file is what keeps them from
// drifting: this builder declares both shapes in Node's spelling and the lines
// must be the golden's, in order.

const TOPOLOGIES = member('test/fixtures/reference_topologies.txt');

function goldenLines(topology) {
  return readFileSync(TOPOLOGIES, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .filter((line) => line.startsWith(`${topology} |`));
}

/**
 * The DERIVED declaration rendered in the golden's columns, one line per
 * channel. The consumers, the kind after the wiring rule, the `named` override
 * and the sink are all the core's answer to the record this builder submitted.
 */
function topologyLines(topology, spec) {
  return validate(spec).channels.map((channel) =>
    [
      topology,
      channel.index,
      channel.name,
      channel.kind,
      channel.producer ?? '-',
      channel.consumers.length > 0 ? channel.consumers.map((edge) => edge.tasklet).join(',') : '-',
      channel.sink ?? '-',
    ].join(' | '),
  );
}

const bytes = (value) => Buffer.from(String(value), 'utf8');

function hybridChannel() {
  const producer = defineTasklet({
    name: 'hybrid_producer_tasklet',
    run: (input, ctx) => ctx.emit(input),
  });
  const consumer = defineTasklet({ name: 'hybrid_consumer_tasklet', run: () => {} });
  return defineStream({
    factoryId: 'hybrid_channel',
    channels: (c) => {
      c.input('input').into(producer).sink('hybrid_indicator', bytes).into(consumer);
    },
  });
}

function commitBlockChain() {
  const commitA = defineTasklet({
    name: 'tier_commit_a',
    run: (input, ctx) => ctx.emit(input, { key: 'job' }),
  });
  const blockB = defineTasklet({ name: 'block_b', run: () => {} });
  return defineWorkflow({
    factoryId: 'commit_block_chain',
    channels: (c) => {
      c.input('in').into(commitA).keyed().sink('mid', bytes).into(blockB);
    },
  });
}

test('the hybrid stream topology is the reference, channel for channel', () => {
  assert.deepEqual(topologyLines('hybrid_channel', hybridChannel()), goldenLines('hybrid_channel'));
});

test('the structured workflow topology is the reference, channel for channel', () => {
  assert.deepEqual(
    topologyLines('commit_block_chain', commitBlockChain()),
    goldenLines('commit_block_chain'),
  );
});

// A merge is no declaration key: each source is a chain the core reads, closed
// by the `merge` link `flz_core::host::decl` deserializes.
test('a merge source serialises as the core’s merge link, keyed after its mapping', () => {
  const merged = (name) => defineTasklet({ name, dynamicCells: {}, run: (i) => i });
  const decl = toHostClassDecl(
    defineStream({
      factoryId: 'node_decl_merge',
      channels: (c) => {
        for (const [first, second, into] of [
          ['a', 'b', 'first_merge'],
          ['c', 'd', 'second_merge'],
        ]) {
          c.merge()
            .from(c.input(first), (value) => value)
            .from(c.input(second), (value) => value)
            .keyBy(String)
            .into(merged(into));
        }
      },
    }),
  );
  const source = (tasklet, group) => [
    { link: 'transform' },
    { link: 'key' },
    { link: 'merge', tasklet, group },
  ];
  assert.deepEqual(
    decl.channels.map((channel) => channel.chains),
    [[source(0, 0)], [source(0, 0)], [source(1, 1)], [source(1, 1)]],
  );
});
