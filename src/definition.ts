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
import {
  DeclarationError,
  type BoundedDefinition,
  type ChannelBuilder,
  type CodecOptions,
  type OperatorDefinition,
  type ShortFormDefinition,
  type Stream,
  type Tasklet,
  type TaskletDefinition,
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
  const refuse = (defect: string): never => {
    throw new DeclarationError('<tasklet>', definition.name ?? null, defect);
  };
  const operatorFields = written(definition, ['kind', ...SHORT_FORM_KEYS]);
  if (operatorFields) {
    refuse(
      `declares ${operatorFields}, which a tasklet does not carry; the short form is declared ` +
        'with `defineUnbounded` or `defineBounded`, whose name is its kind',
    );
  }
  if (!definition.name || !definition.name.trim()) {
    throw new DeclarationError('<tasklet>', null, 'a tasklet declares a non-empty name');
  }
  if ((definition as { handle?: unknown }).handle !== undefined) {
    refuse("declares `handle`; a tasklet's body is named `run`");
  }
  const unknown = unknownFields(definition, TASKLET_DEFINITION_FIELDS);
  if (unknown) refuse(`declares ${unknown}, which a tasklet definition does not carry`);
  if (typeof definition.run !== 'function') refuse('a tasklet declares its body, `run`');
  // State without a fold is legitimate: a value seeded at start and read but
  // never mutated has nothing to journal, and `ctx.change` already refuses at
  // the point of use. A fold with no state to fold into is not.
  if (!definition.initState && definition.applyChange) {
    refuse('declares applyChange but no initState; there is nothing for a change to fold into');
  }
  const ttl = definition.idleTtlSeconds;
  if (ttl !== undefined && !(Number.isInteger(ttl) && ttl > 0)) {
    const shown = typeof ttl === 'number' ? String(ttl) : JSON.stringify(ttl);
    refuse(`declares idleTtlSeconds ${shown}; an idle TTL is a positive whole number of seconds`);
  }
  if (definition.eventTime && definition.logicalClock) {
    refuse('declares both eventTime and logicalClock; a tasklet runs on one clock');
  }
  if (definition.cells && definition.cells.keys.length === 0) {
    refuse('declares an empty static key set; a static tasklet names its cells');
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
 * hand-written single-tasklet operator carries. `ShortFormWithTopology` is
 * the one contract refusal (`authoring_contract.md` §5.2) raised here, and it
 * carries its name in `refusals` and at the start of the message.
 *
 * `SinkWithoutOutput` is not raised: a `.mjs` definition declares no output
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
    throw new DeclarationError('<operator>', null, 'an operator declares a non-empty `factoryId`');
  }
  const refuse = refuseFor(definition.factoryId);
  const foreign = kind === 'unbounded' ? written(definition, BOUNDED_SETTINGS) : '';
  if (foreign) {
    refuse(
      `declares ${foreign}, which only a bounded operator carries; an unbounded operator has no loop, rollback or ` +
        'lifecycle setting, so the operator is declared with `defineBounded` or without it',
    );
  }
  const unknown = unknownFields(definition, DEFINITION_FIELDS);
  if (unknown) refuse(`declares unknown ${unknown}; an operator definition carries no such field`);
  const { channels } = definition;
  const { run } = definition as { run?: unknown };
  if (run !== undefined && channels !== undefined) {
    refuse(
      'ShortFormWithTopology: declares both `run` and `channels`; a short form carries its one ' +
        "tasklet's body in place of a topology body",
      ['ShortFormWithTopology'],
    );
  }
  if (run === undefined) {
    if (typeof channels !== 'function') {
      refuse(
        'declares no body; an operator wires its tasklets in `channels`, or carries one ' +
          "tasklet's `run` body as the short form",
      );
    }
    const keys = written(definition, [...SHORT_FORM_KEYS, ...TASKLET_FIELDS]);
    if (keys) {
      refuse(
        `declares ${keys} beside \`channels\`; a full form wires its entry channels, codecs ` +
          "and sinks in `channels` itself, and declares each tasklet's fields on its " +
          '`defineTasklet`',
      );
    }
    return channels as (builder: ChannelBuilder) => void;
  }
  return shortFormBody(definition as ShortForm & Pick<BoundedDefinition, 'loopMode'>);
}

/** A refusal of the operator `factoryId` names, under the core's names when given. */
function refuseFor(factoryId: string): (defect: string, refusals?: string[]) => never {
  return (defect, refusals = []) => {
    throw new DeclarationError(factoryId, null, defect, refusals);
  };
}

/** The channels body a §5 short form stands for, once its operator fields are judged. */
function shortFormBody(
  definition: ShortForm & Pick<BoundedDefinition, 'loopMode'>,
): (builder: ChannelBuilder & EntryBuilder) => void {
  const refuse = refuseFor(definition.factoryId);
  if (written(definition, ['kind'])) {
    refuse(
      'declares `kind` on a short form; the kind is the name of the function that declares it ' +
        '(`defineUnbounded` or `defineBounded`), not a field',
    );
  }
  if (written(definition, ['name'])) {
    refuse(
      "declares `name` on a short form; a short form's tasklet is named after its `factoryId`",
    );
  }
  const settings = written(definition, CLASS_SETTINGS);
  if (settings) {
    refuse(
      `declares ${settings} on a short form; a class-level setting is the full form's, and the ` +
        'short form keeps every one at its default',
    );
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
