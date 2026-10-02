// The refusal table, driven through the Node SDK's own builder.
//
// `test/fixtures/reference_topologies.txt` names one refusal per row, with the
// authoring steps in a language-neutral spelling every SDK maps to its own
// surface. This module is that mapping for Node: it reads the fixture, binds
// the tasklet palette the fixture header declares, and submits each row the
// same way `validate()` does — so the Node SDK is measured against the whole
// table rather than against the rows it happens to declare itself.
//
// The palette lives here rather than in `fixtures.mjs`: these tasklets exist
// only for the conformance run and are not part of the app's declared set.
//
// `flz_core/src/host/check_tests.rs` is the same interpreter written against
// the record instead of a builder, and is the reference for what each step
// means.

import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  defineOperator,
  defineTasklet,
  loadNative,
  Registry,
  validate,
} from '@flezha/sdk';

import { ascendTo } from '../test/contract.mjs';
import { manifest } from '../test/test-app.mjs';
import { hybridChannel } from './fixtures.mjs';
import { tierCommitBlockSteps } from './fixtures_steps.mjs';
import { render } from './fixtures_call.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
// Walked up to rather than counted: this module runs from its real path, and
// the tree that carries the table sits a different depth above each layout.
const TABLE = 'test/fixtures/reference_topologies.txt';
const FIXTURE = resolve(ascendTo(HERE, TABLE) ?? HERE, TABLE);

/** The authoring surfaces this builder carries, as the manifest declares them. */
const SURFACES = manifest().topology_surfaces;

/**
 * The two rows the fixture cites rather than re-spells. They are this app's own
 * reference twins: `commit_block_chain` is `tier_commit_block_steps`'s
 * topology, which is why the fixture names no steps for either.
 */
const CITED = {
  hybrid_channel: hybridChannel,
  commit_block_chain: tierCommitBlockSteps,
};

/** The four-column refusal table, as data. */
export function rows() {
  return readFileSync(FIXTURE, 'utf8')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line !== '' && !line.startsWith('#'))
    .map((line) => line.split('|').map((part) => part.trim()))
    .filter((columns) => columns.length === 4)
    .map(([refusal, topology, steps, needs]) => ({ refusal, topology, steps, needs }));
}

/** `verb(arg)` → `['verb', 'arg']`; a bare word → `['word', '']`. */
function call(text) {
  const open = text.indexOf('(');
  if (open === -1) return [text.trim(), ''];
  return [text.slice(0, open).trim(), text.slice(open + 1, text.lastIndexOf(')')).trim()];
}

/** `<handle>.<rest>` → `['handle', 'rest']`; a call with no chain → `[step]`. */
function splitHead(step) {
  const at = step.indexOf('.');
  return at === -1 ? [step, undefined] : [step.slice(0, at), step.slice(at + 1)];
}

/**
 * One fixture tasklet per palette role. `mailbox` is the row's
 * `<task>.mailbox(n)`, which is a tasklet knob in this SDK rather than a
 * builder call, so it is read before the graph is declared.
 */
function palette(role, mailbox) {
  const common = { mailboxCapacity: mailbox, run: (value, ctx) => ctx.emit(value) };
  switch (role) {
    case 'plain':
    case 'second':
    // Dynamic by the row's `key` link into it, not by a field of its own.
    case 'dynamic':
      return defineTasklet({ name: role, ...common });
    // A twin is a DIFFERENT tasklet claiming `plain`'s name: that is the
    // collision the row names.
    case 'twin':
      return defineTasklet({ name: 'plain', ...common });
    case 'static':
      return defineTasklet({ name: role, cells: { keys: ['a', 'b'] }, ...common });
    case 'routed':
      return defineTasklet({
        name: role,
        cells: { keys: ['a', 'b'], cellsFor: () => ['a'] },
        ...common,
      });
    // A logical clock over a tasklet that journals nothing: no `applyChange`
    // is what makes the change type empty.
    case 'nochange':
      return defineTasklet({ name: role, logicalClock: true, ...common });
    case 'eventtime':
      return defineTasklet({ name: role, eventTime: () => 0n, ...common });
    default:
      throw new Error(`the fixture names a palette role this interpreter does not bind: ${role}`);
  }
}

/**
 * `<x>.call…(<arg>)`: a 500 ms call whose Response channel is wired back by
 * `wireBack` into the role that produces `x`, unless the step leaves it unread.
 */
function callStep(stream, verb, arg, wireBack) {
  const target = {
    call: arg,
    call_unread: arg,
    call_factory: { factory: arg },
    call_factory_nocreate: { factory: arg, create: false },
    call_deadline: 'pricing',
  }[verb];
  const deadlineMs = verb === 'call_deadline' ? Number(arg) : 500;
  const response = stream.call(target, { deadlineMs, codec: 'text' });
  if (verb !== 'call_unread') wireBack(response.map(render));
  return response;
}

/**
 * What a row states before its graph is built: the class settings its
 * `class.run(<tree>)` and `class.durable(<tree>)` write, and each tasklet's
 * mailbox capacity, which are definition fields here, not fluent calls.
 */
function preScan(steps) {
  const mailbox = new Map();
  const settings = {};
  for (const step of steps) {
    const [head, rest] = splitHead(step);
    if (rest === undefined) continue;
    const [verb, arg] = call(rest);
    if (head === 'class') Object.assign(settings, classSetting(verb, arg));
    else if (verb === 'mailbox') mailbox.set(head, Number(arg));
  }
  return { mailbox, settings };
}

/**
 * One `class.<setting>(<tree>)` as a definition writes it. The run is written
 * as its branch key, so `"steps"` is `steps: {}` and `{"flow": {...}}` is
 * `flow: {...}`, a misplaced member and all.
 */
function classSetting(verb, arg) {
  const tree = JSON.parse(arg);
  switch (verb) {
    case 'run':
      return typeof tree === 'string' ? { [tree]: {} } : tree;
    case 'durable':
      return { durable: tree };
    default:
      throw new Error(`the fixture spells a class setting this interpreter does not map: ${verb}`);
  }
}

/** The class one row's steps declare, built through the ordinary builder. */
function specOf(row) {
  const steps = row.steps
    .split(';')
    .map((step) => step.trim())
    .filter(Boolean);
  const { mailbox, settings } = preScan(steps);
  const tasklets = new Map();
  const taskletFor = (role) => {
    if (!tasklets.has(role)) tasklets.set(role, palette(role, mailbox.get(role)));
    return tasklets.get(role);
  };

  const channels = (c) => {
    const handles = new Map();
    const handle = (id) => {
      const stream = handles.get(id);
      if (!stream) throw new Error(`row \`${row.topology}\` addresses an undeclared handle: ${id}`);
      return stream;
    };

    // One fluent sequence, left to right. `into` answers the consuming
    // tasklet's output channel, which the role then addresses.
    const chain = (id, rest) => {
      let stream = handle(id);
      for (const link of rest.split('.')) {
        const [verb, arg] = call(link);
        switch (verb) {
          case 'map':
            stream = stream.map((value) => value);
            break;
          case 'filter':
            stream = stream.filter(() => true);
            break;
          case 'flat_map':
            stream = stream.flatMap((value) => [value]);
            break;
          case 'key':
            stream = stream.keyBy((value) => String(value));
            break;
          case 'named':
            stream = stream.named(arg);
            break;
          case 'keyed':
            stream = stream.keyed();
            break;
          case 'retry_max':
            stream = stream.retryMax(Number(arg));
            break;
          case 'on_full':
            stream = stream.onFull(arg);
            break;
          case 'sink':
            stream = arg === '' ? stream.sink() : stream.sink(arg);
            break;
          case 'response':
            stream = stream.response();
            break;
          case 'call':
          case 'call_unread':
          case 'call_factory':
          case 'call_factory_nocreate':
          case 'call_deadline':
            stream = callStep(stream, verb, arg, (response) => response.into(taskletFor(id)));
            break;
          case 'into':
            stream = stream.into(taskletFor(arg));
            handles.set(arg, stream);
            break;
          default:
            throw new Error(`the fixture spells a link this interpreter does not map: ${verb}`);
        }
      }
    };

    for (const step of steps) {
      const [head, rest] = splitHead(step);
      if (rest === undefined) {
        const [verb, arg] = call(step);
        if (verb === 'entry') {
          handles.set(arg, c.input(arg));
          continue;
        }
        if (verb === 'branch') {
          const [id, arms] = arg.split(',').map((part) => part.trim());
          // Each arm wires itself off this same handle; the fixture's arms are
          // empty, so the arity is all the row states.
          handles.set(id, handle(id).branch(Array.from({ length: Number(arms) }, () => () => {})));
          continue;
        }
        if (verb === 'merge') {
          const [role, ...sources] = arg.split(',').map((part) => part.trim());
          const merge = sources.reduce(
            (pending, id) => pending.from(handle(id), (value) => value),
            c.merge(),
          );
          handles.set(role, merge.into(taskletFor(role)));
          continue;
        }
        throw new Error(`the fixture spells a step this interpreter does not map: ${step}`);
      }
      // The class settings and the mailbox capacity were read by `preScan`.
      if (head === 'class' || call(rest)[0] === 'mailbox') continue;
      chain(head, rest);
    }
  };

  // The tree goes as the row wrote it, so a misplaced setting reaches the
  // core, which refuses it by name.
  return defineOperator({ factoryId: row.topology, ...settings, channels });
}

/** The class a topology id declares — the cited twins by name, the rest by steps. */
export function specForTopology(topology) {
  const cited = CITED[topology];
  if (cited) return cited;
  const row = rows().find((candidate) => candidate.topology === topology);
  if (!row) throw new Error(`${FIXTURE} has no row for topology \`${topology}\``);
  return specOf(row);
}

/**
 * Whether a row needs a surface this builder does not carry.
 *
 * `detached` is in no builder's list: a fluent handle cannot name a node
 * nothing wired. The fixture header states it.
 */
function skipped(row) {
  return row.needs !== '-' && !SURFACES.includes(row.needs);
}

/** The derived declaration in the fixture's own columns: consumers are indices. */
function derivedOf(declaration) {
  return declaration.channels.map((channel) => ({
    index: channel.index,
    name: channel.name,
    kind: channel.kind,
    producer: channel.producer ?? null,
    consumers: channel.consumers.map((edge) => edge.tasklet),
    sink: channel.sink ?? null,
  }));
}

// ---------------------------------------------------------------------------
// The §5 short form. Node refuses a malformed declaration where `defineOperator`
// runs and a duplicate factory id where the registry takes the second spec, so every row this SDK can write boots for real.

/** The `shorthand(<id>|-, <sink>|-, <input>|-)[.noout|.topology]` steps of a row. */
function shorthandSteps(row) {
  const parsed = [];
  for (const raw of row.steps.split(';')) {
    const step = raw.trim();
    const modifier = ['noout', 'topology'].find((name) => step.endsWith(`.${name}`));
    const [verb, args] = call(modifier ? step.slice(0, -`.${modifier}`.length) : step);
    if (verb !== 'shorthand') continue;
    const fields = String(args)
      .split(',')
      .map((field) => field.trim());
    if (fields.length !== 3) {
      throw new Error(`a shorthand step declares three fields: read \`${step}\``);
    }
    const written = (field) => (field === '-' ? undefined : field);
    parsed.push({
      factoryId: written(fields[0]),
      sink: written(fields[1]),
      input: written(fields[2]),
      noout: modifier === 'noout',
      topology: modifier === 'topology',
    });
  }
  return parsed;
}

/**
 * Declare one short form exactly as an author writes it: the row's first as a
 * flow, any later one in steps, so a collision crosses runs.
 */
function declareShorthand(declaration, index) {
  return defineOperator({
    factoryId: declaration.factoryId,
    ...(index === 0 ? {} : { steps: {} }),
    sink: declaration.sink,
    input: declaration.input,
    run: declaration.noout ? () => {} : (input, ctx) => ctx.emit(input),
    ...(declaration.topology ? { channels: () => {} } : {}),
  });
}

/**
 * A refused row's verdict. The names are the error's own codes, never
 * re-derived here: every issue the core listed, or the one code of a refusal
 * this SDK made itself.
 */
function refused(row, error) {
  if (!(error instanceof FlezhaError)) throw error;
  const issues = error.details.issues;
  const names = Array.isArray(issues) ? issues.map((issue) => issue.code) : [error.code];
  return { topology: row.topology, outcome: 'refused', names, defect: error.message };
}

/** A short-form row's verdict: what declaring and registering it answered. */
function judgeShorthand(row, declarations) {
  try {
    const registry = new Registry();
    for (const [index, declaration] of declarations.entries()) {
      registry.add(declareShorthand(declaration, index));
    }
  } catch (error) {
    return refused(row, error);
  }
  return { topology: row.topology, outcome: 'accepted' };
}

/** Drive one row: skipped, refused with the core's own names, or accepted. */
function judge(row) {
  if (skipped(row)) return { topology: row.topology, outcome: 'skipped', needs: row.needs };
  const declarations = shorthandSteps(row);
  if (declarations.length > 0) return judgeShorthand(row, declarations);
  try {
    return {
      topology: row.topology,
      outcome: 'accepted',
      derived: derivedOf(validate(specForTopology(row.topology))),
    };
  } catch (error) {
    return refused(row, error);
  }
}

/**
 * Drive the whole table and write the report the conformance drill reads.
 *
 * No controller is reached: the context the check needs is created and freed
 * here, and nothing else in the runner is started.
 */
export function writeConformanceReport(path) {
  const native = loadNative();
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const report = { runtime: 'node', rows: rows().map(judge) };
    writeFileSync(path, `${JSON.stringify(report, null, 2)}\n`);
  } finally {
    native.contextFree();
  }
}

/**
 * With `FLEZHA_REFUSAL_FIXTURE` set, the ONE topology this process registers.
 * `undefined` otherwise, so the runner's registry scan ignores this module.
 */
const refusalRow = process.env.FLEZHA_REFUSAL_FIXTURE
  ? rows().find((row) => row.topology === process.env.FLEZHA_REFUSAL_FIXTURE)
  : undefined;

/**
 * A short-form row's declarations, made here so a malformed one is refused
 * while this module loads — which is where an author's own would be refused.
 */
const refusalShorthand = refusalRow ? shorthandSteps(refusalRow).map(declareShorthand) : [];

export const refusalFixture =
  refusalShorthand.length > 0
    ? refusalShorthand[0]
    : process.env.FLEZHA_REFUSAL_FIXTURE
      ? specForTopology(process.env.FLEZHA_REFUSAL_FIXTURE)
      : undefined;

/** The duplicate row's second declaration, under the first's factory id. */
export const refusalFixtureTwin = refusalShorthand[1];
