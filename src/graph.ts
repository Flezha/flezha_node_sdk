/**
 * The record the core is handed, and the verdict it answers.
 *
 * Every rule a declared topology can be refused under is one function in
 * `flz_core::host::register::check`, and `validate` is the call that submits
 * to it: this SDK records what the author declared, hands it over, and renders
 * the answer as its own exception. Nothing here restates a rule the core
 * holds — a refusal added there reaches Node with no edit to this file.
 *
 * Static typing catches a wrong edge type before the program runs (a
 * `.into(tasklet)` whose producer's value the tasklet's declared input
 * does not accept is a compile error, pinned by the fixtures under
 * `types-tests/`); that backstop is unchanged and earlier than this one.
 */

import type { ChannelKind, ChannelSpec, ClassSpec, ConsumerEdge, TaskNode } from './authoring.js';
import { loadNative, type Native } from './native.js';

/** The Node SDK's byte in the core's `sdk_lang` allocation table. */
export const SDK_LANG_NODE = 4;

/**
 * This SDK's own released version, stamped into every durable record it writes.
 * Pinned to `sdk/node/package.json` by `test/sdk_conformance/gate_test.exs`; a
 * source constant rather than a runtime read of the manifest, because the
 * package ships `dist/cjs`, `dist/esm` and `dist/types` from one `src`, so the
 * manifest sits at three different depths and any bundler that inlines the
 * module loses it entirely. The core packs it.
 */
export const SDK_VERSION = '0.1.0';

/** MessagePack, shared with every SDK whose codec is MessagePack. */
export const FORMAT_ID_MSGPACK = 5;

/** One consumer edge, as the core read it off the channel's chains. */
export interface DerivedEdge {
  tasklet: number;
  /** Which of the channel's chains produced this edge; the host holds its closures. */
  chain: number;
}

/** One channel, as the core derived it from the record. */
export interface DerivedChannel {
  index: number;
  /** The last `named` override, else the name the builder wrote. */
  name: string;
  /** The declared kind after the wiring rule: an output with no consumer is `plugin`. */
  kind: ChannelKind;
  producer: number | null;
  consumers: DerivedEdge[];
  sink: string | null;
}

/** The declaration the core derived: the answer a passing `validate` gives. */
export interface DerivedDecl {
  channels: DerivedChannel[];
  /** `checkpoint` when any checkpoint key was declared, else `per_input`. */
  ack_granularity: 'per_input' | 'checkpoint';
}

function clockOf(node: TaskNode): 'wall' | 'logical' | 'event_time' {
  const def = node.tasklet.definition;
  if (def.eventTime) return 'event_time';
  return def.logicalClock ? 'logical' : 'wall';
}

/**
 * Submit the record and answer what the core derived from it.
 *
 * On a refusal this throws the core's `FlezhaError`, whose `details.issues`
 * lists every refusal by its code. On success the derived channel names are written back,
 * so the name this host addresses a channel by is the one the core registered.
 */
export function validate(spec: ClassSpec, native: Native = loadNative()): DerivedDecl {
  const answer = native.checkClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));
  const derived = JSON.parse(answer.toString('utf8')) as DerivedDecl;
  for (const channel of derived.channels) {
    const declared = spec.channels[channel.index] as ChannelSpec;
    declared.name = channel.name;
    declared.derivedKind = channel.kind;
    // The runtime edges ARE the core's answer: it says which chains produced a
    // consumer and in what order, and the host attaches the closures that
    // chain declared.
    declared.consumers = channel.consumers.map(
      (edge) => declared.chainEdges.get(edge.chain) as ConsumerEdge,
    );
  }
  return derived;
}

/** Drop the keys whose value is undefined, so the JSON omits them. */
function compact<T extends Record<string, unknown>>(value: T): T {
  for (const key of Object.keys(value)) {
    if (value[key] === undefined) delete value[key];
  }
  return value;
}

function channelJson(channel: ChannelSpec): Record<string, unknown> {
  return compact({
    name: channel.name,
    kind: channel.kind,
    producer: channel.producer,
    chains: channel.chains,
    retry_max: channel.retryMax,
    retry_backoff_ms: channel.retryBackoffMs,
    overflow: channel.overflow,
    keyed: channel.keyed,
    codec: channel.codec,
    schema: channel.schema,
    description: channel.description,
  });
}

function taskletJson(spec: ClassSpec, node: TaskNode): Record<string, unknown> {
  const def = node.tasklet.definition;
  return compact({
    name: node.name,
    cell_keys: def.cells?.keys ?? [],
    idle_ttl_ms: def.idleTtlSeconds === undefined ? undefined : def.idleTtlSeconds * 1000,
    max_cells: def.maxCells,
    cells_for_overridden: def.cells?.cellsFor !== undefined,
    compensate_overridden: def.compensate !== undefined,
    clock: clockOf(node),
    mailbox_capacity: def.mailboxCapacity,
    max_timers_per_cell: def.maxTimersPerCell,
    // A tasklet that folds no change has the empty Change type, so a logical
    // clock over it would never advance; a class that keeps nothing has it
    // for every tasklet.
    change_is_nochange: spec.durable === false || def.applyChange === undefined,
    schema_version: def.schemaVersion ?? 1,
    min_readable_schema_version: def.minReadableSchemaVersion,
    migrates: def.migrateState !== undefined || def.migrateChange !== undefined,
  });
}

/**
 * The declaration the core deserializes. Every key here is pinned by
 * `test/fixtures/host_class_decl_keys.txt`, which this package's own test
 * reads: a key the core learns that Node cannot spell fails that test.
 */
export function toHostClassDecl(spec: ClassSpec): Record<string, unknown> {
  return compact({
    factory_id: spec.factoryId,
    description: spec.description,
    max_duration_ms: spec.maxDurationMs,
    run: spec.run,
    durable: spec.durable,
    sdk_lang: SDK_LANG_NODE,
    sdk_version: SDK_VERSION,
    change_format_id: FORMAT_ID_MSGPACK,
    compact_after_bytes: spec.compactAfterBytes,
    channels: spec.channels.map(channelJson),
    tasklets: spec.tasklets.map((node) => taskletJson(spec, node)),
  });
}
