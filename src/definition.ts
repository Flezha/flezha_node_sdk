/**
 * What a definition may carry: the fields a tasklet and either relay form
 * read, and the refusals of a field written where it does not belong.
 *
 * `defineTasklet` checks a tasklet's own definition; `channelsOf` checks an
 * relay definition and turns a short form into the channels body a
 * hand-written single-tasklet relay carries.
 */

import { assertRepresentable } from './codec.js';
import type { ClassSpec } from './authoring.js';
import { flezhaError } from './errors.js';
import type {
  ChannelBuilder,
  ClassSettings,
  CodecOptions,
  RelayDefinition,
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
  const relayFields = written(definition, [...SHORT_FORM_KEYS, ...RUN_BRANCHES, 'durable']);
  if (relayFields) {
    throw flezhaError('topology.relay_field_on_tasklet', {
      tasklet,
      fields: relayFields,
      instead: '`defineRelay`',
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

/** The branches `run` takes, each written as the definition's own key. */
const RUN_BRANCHES = ['flow', 'steps'] as const;

/**
 * The retired kind words. The run is written as its branch key, so a kind
 * written where the run is is a run outside its choice.
 */
const KIND_LITERALS = ['bounded', 'unbounded'] as const;

/**
 * Every field either form reads. The relay's own are keyed by the type, so
 * a field the definition gains is listed here or does not compile; anything
 * else, a retired setting among them, is refused by name.
 */
const DEFINITION_FIELDS = new Set<string>([
  ...Object.keys({
    factoryId: true,
    channels: true,
    description: true,
    maxDurationMs: true,
    durable: true,
  } satisfies Record<keyof ClassSettings | 'factoryId' | 'channels', true>),
  ...RUN_BRANCHES,
  ...SHORT_FORM_KEYS,
  ...TASKLET_FIELDS,
  'run',
  'name',
]);

/** The keys among `keys` a definition writes, quoted as a refusal names them, or `''`. */
function written(definition: object, keys: readonly string[]): string {
  const present = keys.filter((key) => (definition as Record<string, unknown>)[key] !== undefined);
  return present.length === 0 ? '' : `\`${present.join('`, `')}\``;
}

/**
 * A definition as `defineRelay` reads it: the full form's fields with
 * `channels` optional, which a short form also is, since it carries `run` in
 * place of `channels`.
 */
export type EitherForm<D extends RelayDefinition> = Omit<D, 'channels'> &
  Partial<Pick<D, 'channels'>>;

/** A member name as the tree's JSON spells it: `sleepAfter` is `sleep_after`. */
function snakeCase(name: string): string {
  return name.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

/** A settings tree as its JSON, every member spelled as the core reads it. */
function treeJson(tree: unknown): unknown {
  if (tree === null || typeof tree !== 'object' || Array.isArray(tree)) return tree;
  return Object.fromEntries(
    Object.entries(tree).map(([name, member]) => [snakeCase(name), treeJson(member)]),
  );
}

/**
 * The class settings a definition writes, the tree as the host declaration
 * carries it. Both branches written are both passed on: the core refuses a
 * run naming two.
 */
export function settingsOf(
  definition: EitherForm<RelayDefinition>,
): Pick<ClassSpec, 'description' | 'maxDurationMs' | 'run' | 'durable'> {
  const branches = RUN_BRANCHES.filter((branch) => definition[branch] !== undefined);
  return {
    description: definition.description,
    maxDurationMs: definition.maxDurationMs,
    run:
      branches.length === 0
        ? undefined
        : Object.fromEntries(branches.map((branch) => [branch, treeJson(definition[branch])])),
    durable: treeJson(definition.durable),
  };
}

/** A short form's one builder call: its entry, with the codec it may name. */
interface EntryBuilder {
  input(name: string, options?: CodecOptions): Stream<unknown>;
}

/**
 * The channels body a definition wires: its own `channels`, or, for a §5 short
 * form carrying one tasklet's `run` body in their place, the body a
 * hand-written single-tasklet relay carries. `topology.short_form_with_topology`
 * is the one contract refusal (`authoring_contract.md` §5.2) raised here.
 *
 * `topology.sink_without_output` is not raised: a `.mjs` definition declares no output
 * type the SDK can read, a TypeScript one names it only in types erased before
 * the SDK runs, and no `run` returns output, so nothing here says whether a
 * short form emits. The Node manifest does not list `shorthand_typed_output`
 * and the row skips by name.
 */
export function channelsOf(
  definition: EitherForm<RelayDefinition>,
): (builder: ChannelBuilder & EntryBuilder) => void {
  if (typeof definition.factoryId !== 'string' || !definition.factoryId.trim()) {
    throw flezhaError('topology.missing_factory_id', { relay: '<relay>' });
  }
  const factoryId = definition.factoryId;
  const kind = KIND_LITERALS.find(
    (word) => (definition as Record<string, unknown>)[word] !== undefined,
  );
  if (kind) {
    throw flezhaError('topology.invalid_setting', {
      declaration: `relay '${factoryId}'`,
      setting: 'run',
      value: kind,
      expected: RUN_BRANCHES.join(' | '),
    });
  }
  const unknown = unknownFields(definition, DEFINITION_FIELDS);
  if (unknown) {
    throw flezhaError('topology.unknown_field', {
      declaration: `relay '${factoryId}'`,
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
        declaration: `relay '${factoryId}'`,
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
  return shortFormBody(definition as ShortForm);
}

/** The channels body a §5 short form stands for, once its relay fields are judged. */
function shortFormBody(definition: ShortForm): (builder: ChannelBuilder & EntryBuilder) => void {
  // The tasklet is named after the factory id.
  const fields = written(definition, ['name']);
  if (fields) {
    throw flezhaError('topology.setting_on_short_form', {
      factory_id: definition.factoryId,
      fields,
    });
  }
  const { factoryId, sink, input = 'input', codec } = definition;
  // The tasklet's own fields, own or inherited; the relay's stay behind.
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
