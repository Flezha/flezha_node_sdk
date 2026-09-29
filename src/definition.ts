/**
 * What a definition may carry: the fields a tasklet and either operator form
 * read, and the refusals of a field written where it does not belong.
 *
 * `defineTasklet` checks a tasklet's own definition; `channelsOf` checks an
 * operator definition and turns a short form into the channels body a
 * hand-written single-tasklet operator carries.
 */

import { assertRepresentable } from './codec.js';
import type { ClassSpec } from './authoring.js';
import { flezhaError } from './errors.js';
import type {
  BoundedDefinition,
  ChannelBuilder,
  CodecOptions,
  OperatorDefinition,
  ShortFormDefinition,
  Stream,
  Tasklet,
  TaskletDefinition,
} from './types.js';

/**
 * Define a tasklet.
 *
 * The body's first parameter is the input type. The output type is never read
 * off the body, which returns nothing: it is the definition's type arguments,
 * `defineTasklet<Trade, Candle>(...)`, or the annotation on its context,
 * `(trade: Trade, ctx: Ctx<Candle, Window, Move>) => ...`, which names the
 * state and change types too. A tasklet that names none has no output, so an
 * `emit` on it does not compile.
 */
export function defineTasklet<TIn, TOut = never, TState = void, TChange = never>(
  definition: TaskletDefinition<TIn, TOut, TState, TChange>,
): Tasklet<TIn, TOut, TState, TChange> {
  const tasklet = definition.name || '<tasklet>';
  const operatorFields = written(definition, ['kind', ...SHORT_FORM_KEYS]);
  if (operatorFields) {
    throw flezhaError('topology.operator_field_on_tasklet', {
      tasklet,
      fields: operatorFields,
      instead: '`defineUnbounded` or `defineBounded`',
    });
  }
  if (!definition.name || !definition.name.trim()) {
    throw flezhaError('topology.tasklet_name_missing', { tasklet });
  }
  if ((definition as { handle?: unknown }).handle !== undefined) {
    throw flezhaError('topology.retired_callback', {
      tasklet,
      callback: '`handle`',
      instead: '`run`',
    });
  }
  const unknown = unknownFields(definition, TASKLET_DEFINITION_FIELDS);
  if (unknown) {
    throw flezhaError('topology.unknown_field', {
      declaration: `tasklet '${tasklet}'`,
      fields: unknown,
    });
  }
  if (typeof definition.run !== 'function') {
    throw flezhaError('topology.missing_body', {
      declaration: `tasklet '${tasklet}'`,
      body: '`run`',
    });
  }
  // State without a fold is legitimate: a value seeded at start and read but
  // never mutated has nothing to journal, and `ctx.change` already refuses at
  // the point of use. A fold with no state to fold into is not.
  if (!definition.initState && definition.applyChange) {
    throw flezhaError('topology.change_without_state', { tasklet });
  }
  const ttl = definition.idleTtlSeconds;
  if (ttl !== undefined && !(Number.isInteger(ttl) && ttl > 0)) {
    throw flezhaError('topology.invalid_setting', {
      declaration: `tasklet '${tasklet}'`,
      setting: 'idleTtlSeconds',
      value: typeof ttl === 'number' ? String(ttl) : JSON.stringify(ttl),
      expected: 'a positive whole number of seconds',
    });
  }
  if (definition.eventTime && definition.logicalClock) {
    throw flezhaError('topology.two_clocks', { tasklet });
  }
  if (definition.cells && definition.cells.keys.length === 0) {
    throw flezhaError('topology.static_tasklet_without_cell_keys', { tasklet });
  }
  // A state that cannot be encoded cannot be restored, and the first anyone
  // would learn of it is a restart. Walk the declared starting state now.
  if (definition.initState && !definition.stateCodec) {
    assertRepresentable(`${definition.name}.state`, definition.initState(undefined));
  }
  return {
    definition,
    name: definition.name,
  } as Tasklet<TIn, TOut, TState, TChange>;
}

type ShortForm = ShortFormDefinition<unknown, unknown, unknown, unknown>;
type AnyTaskletDefinition = TaskletDefinition<unknown, unknown, unknown, unknown>;

/** The short form's own keys, which a full form wires in `channels` instead. */
const SHORT_FORM_KEYS = ['sink', 'input', 'codec'] as const satisfies readonly (keyof ShortForm)[];

/**
 * A tasklet definition's fields but its `name`, which a short form writes as
 * `factoryId`, and `run`, which is what makes one. Keyed by the type, so a
 * field the tasklet definition gains is listed here or does not compile.
 */
const TASKLET_FIELDS = Object.keys({
  cells: true,
  idleTtlSeconds: true,
  maxCells: true,
  eventTime: true,
  logicalClock: true,
  mailboxCapacity: true,
  schemaVersion: true,
  minReadableSchemaVersion: true,
  migrateState: true,
  migrateChange: true,
  initState: true,
  applyChange: true,
  stateCodec: true,
  changeCodec: true,
  onTimer: true,
  compensate: true,
} satisfies Record<
  Exclude<keyof TaskletDefinition<unknown, unknown, unknown, unknown>, 'name' | 'run'>,
  true
>);

/** Every field `defineTasklet` reads, or refuses under a reason of its own. */
const TASKLET_DEFINITION_FIELDS = new Set([...TASKLET_FIELDS, 'name', 'run']);

/** What replaced each retired tasklet field, named when one is still written. */
const RETIRED_FIELDS = new Map([
  ['initialState', '`initState`'],
  ['dynamicCells', '`keyBy` on the wiring, with `idleTtlSeconds` and `maxCells`'],
  ['clock', '`eventTime` or `logicalClock: true`'],
]);

/**
 * Every field a definition carries, read along the prototype chain as its
 * fields are read, so a hook a class instance inherits counts as one it owns.
 */
function fieldNames(definition: object): Set<string> {
  const names = new Set<string>();
  for (let at = definition; at && at !== Object.prototype; at = Object.getPrototypeOf(at)) {
    for (const name of Object.getOwnPropertyNames(at)) {
      if (name !== 'constructor') names.add(name);
    }
  }
  return names;
}

/** The fields a definition carries outside `known`, quoted as a refusal names them, or `''`. */
function unknownFields(definition: object, known: ReadonlySet<string>): string {
  return [...fieldNames(definition)]
    .filter((name) => !known.has(name))
    .map((name) =>
      RETIRED_FIELDS.has(name) ? `\`${name}\` (now ${RETIRED_FIELDS.get(name)})` : `\`${name}\``,
    )
    .join(', ');
}

/**
 * A bounded operator definition's own options, which an unbounded definition carries none
 * of in either form. Keyed by the type, so an option the bounded operator definition
 * gains is listed here or does not compile. `syncDurability` is not among
 * them: the core refuses it on an unbounded operator by its own name.
 */
const BOUNDED_SETTINGS = Object.keys({
  loopMode: true,
  rollbackMode: true,
  maxDurationMs: true,
} satisfies Record<
  Exclude<keyof BoundedDefinition, keyof OperatorDefinition | 'syncDurability'>,
  true
>) as (keyof BoundedDefinition)[];

/**
 * The full form's class-level settings: a definition's options and the builder
 * calls that set one on the class. The short form keeps each at its default;
 * a bounded operator's loop mode is the one setting it may write.
 */
const CLASS_SETTINGS = [
  'persistent',
  'sleepWhenIdle',
  ...BOUNDED_SETTINGS.filter((key) => key !== 'loopMode'),
  'syncDurability',
  'effectivelyOnce',
  'checkpointAck',
  'checkpointEvery',
  'outputsAtCheckpoint',
  'correctionWindow',
  'beyondWindowPolicy',
  'compactAfterBytes',
] satisfies readonly (keyof BoundedDefinition | keyof ChannelBuilder)[];

/**
 * Every field either form reads, or refuses under a reason of its own. The
 * operator's own are keyed by the type, so a field the definition gains is
 * listed here or does not compile; anything else, a retired setting among
 * them, is refused by name.
 */
const DEFINITION_FIELDS = new Set<string>([
  ...Object.keys({
    factoryId: true,
    persistent: true,
    sleepWhenIdle: true,
    channels: true,
    syncDurability: true,
  } satisfies Record<keyof OperatorDefinition | 'syncDurability', true>),
  ...BOUNDED_SETTINGS,
  ...CLASS_SETTINGS,
  ...SHORT_FORM_KEYS,
  ...TASKLET_FIELDS,
  'run',
  'kind',
  'name',
]);

/** The keys among `keys` a definition writes, quoted as a refusal names them, or `''`. */
function written(definition: object, keys: readonly string[]): string {
  const present = keys.filter((key) => (definition as Record<string, unknown>)[key] !== undefined);
  return present.length === 0 ? '' : `\`${present.join('`, `')}\``;
}

/**
 * A definition as `defineUnbounded` and `defineBounded` read it: the full
 * form's fields with `channels` optional, which a short form also is, since
 * it carries `run` in place of `channels` and writes no class-level setting
 * but a bounded operator's loop mode.
 */
export type EitherForm<D extends OperatorDefinition> = Omit<D, 'channels'> &
  Partial<Pick<D, 'channels'>>;

/** A short form's one builder call: its entry, with the codec it may name. */
interface EntryBuilder {
  input(name: string, options?: CodecOptions): Stream<unknown>;
}

/**
 * The channels body a definition wires: its own `channels`, or, for a §5 short
 * form carrying one tasklet's `run` body in their place, the body a
 * hand-written single-tasklet operator carries. `topology.short_form_with_topology`
 * is the one contract refusal (`authoring_contract.md` §5.2) raised here.
 *
 * `topology.sink_without_output` is not raised: a `.mjs` definition declares no output
 * type the SDK can read, a TypeScript one names it only in types erased before
 * the SDK runs, and no `run` returns output, so nothing here says whether a
 * short form emits. The Node manifest does not list `shorthand_typed_output`
 * and the row skips by name.
 */
export function channelsOf(
  definition: EitherForm<OperatorDefinition>,
  kind: ClassSpec['kind'],
): (builder: ChannelBuilder & EntryBuilder) => void {
  if (typeof definition.factoryId !== 'string' || !definition.factoryId.trim()) {
    throw flezhaError('topology.missing_factory_id', { operator: '<operator>' });
  }
  const factoryId = definition.factoryId;
  const foreign = kind === 'unbounded' ? written(definition, BOUNDED_SETTINGS) : '';
  if (foreign) {
    throw flezhaError('topology.bounded_setting_on_unbounded', {
      factory_id: factoryId,
      settings: foreign,
    });
  }
  const unknown = unknownFields(definition, DEFINITION_FIELDS);
  if (unknown) {
    throw flezhaError('topology.unknown_field', {
      declaration: `operator '${factoryId}'`,
      fields: unknown,
    });
  }
  const { channels } = definition;
  const { run } = definition as { run?: unknown };
  if (run !== undefined && channels !== undefined) {
    throw flezhaError('topology.short_form_with_topology', { factory_id: factoryId });
  }
  if (run === undefined) {
    if (typeof channels !== 'function') {
      throw flezhaError('topology.missing_body', {
        declaration: `operator '${factoryId}'`,
        body: "`channels`, or one tasklet's `run` as the short form",
      });
    }
    const keys = written(definition, [...SHORT_FORM_KEYS, ...TASKLET_FIELDS]);
    if (keys) {
      throw flezhaError('topology.short_form_field_on_full_form', {
        factory_id: factoryId,
        fields: keys,
      });
    }
    return channels as (builder: ChannelBuilder) => void;
  }
  return shortFormBody(definition as ShortForm & Pick<BoundedDefinition, 'loopMode'>);
}

/** The channels body a §5 short form stands for, once its operator fields are judged. */
function shortFormBody(
  definition: ShortForm & Pick<BoundedDefinition, 'loopMode'>,
): (builder: ChannelBuilder & EntryBuilder) => void {
  // The kind is the declaring function's name, the tasklet is named after the
  // factory id, and every class-level setting keeps its default.
  const fields = written(definition, ['kind', 'name', ...CLASS_SETTINGS]);
  if (fields) {
    throw flezhaError('topology.setting_on_short_form', {
      factory_id: definition.factoryId,
      fields,
    });
  }
  const { factoryId, sink, input = 'input', codec } = definition;
  // The tasklet's own fields, own or inherited; the operator's stay behind.
  const tasklet: Record<string, unknown> = {};
  for (const name of fieldNames(definition)) {
    if (TASKLET_DEFINITION_FIELDS.has(name)) tasklet[name] = (definition as never)[name];
  }
  // Named after the factory id, verbatim: the name is a component of every
  // cell's state path, so growing into the full form keeps the state.
  const node = defineTasklet({ ...tasklet, name: factoryId } as AnyTaskletDefinition);
  return (builder) => {
    const output = builder.input(input, codec === undefined ? undefined : { codec }).into(node);
    if (sink !== undefined) output.sink(sink);
  };
}
