// The native surface, and the two properties asserted rather than reviewed:
// the veneer adds no ABI surface, and no blocking entry point is
// synchronously reachable from an event loop.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  ACTIVATION_SCHEMA_VERSION,
  defineRelay,
  defineTasklet,
  FlezhaError,
  loadNative,
  toHostClassDecl,
} from '../dist/esm/index.js';
import { envPositive } from '../dist/esm/env.js';

import { read } from './contract.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const native = loadNative();

test('the addon exports exactly its surface table, each row an entry point or host bookkeeping', () => {
  // The veneer adds no ABI surface: each row either reaches an entry point the
  // C ABI exports, or reaches none at all. The fixture, not the crate's
  // source: a `flz_core_ffi` test holds the two equal in both directions, and
  // reading the source would make this suite runnable only inside a checkout
  // of it.
  const abi = new Set(
    read('test/fixtures/abi_surface.txt')
      .split('\n')
      .map((line) => line.trim())
      .filter((line) => line && !line.startsWith('#')),
  );
  const surface = native.nativeSurface();
  for (const entry of surface) {
    if (!entry.abi) continue;
    assert.ok(
      abi.has(entry.abi),
      `${entry.name} names ${entry.abi}, which flz_core_ffi does not export`,
    );
  }
  const exported = Object.keys(native).sort();
  const named = surface.map((entry) => entry.name).sort();
  assert.deepEqual(
    exported.filter((name) => !named.includes(name)),
    [],
    'an export with no entry in the surface table',
  );
  assert.deepEqual(
    named.filter((name) => !exported.includes(name)),
    [],
    'a surface-table entry that is not exported',
  );
});

test('poll and the starvation wait are not reachable from JavaScript at all', () => {
  // The rule this closes: under the default overflow policy a completion parks
  // until the consumer's mailbox drains, and the consumer drains only when its
  // handler runs. An event loop parked inside the core is the one thread that
  // could run it.
  for (const forbidden of ['poll', 'waitStarvation', 'flzCorePoll']) {
    assert.equal(native[forbidden], undefined, `${forbidden} must not be exported`);
  }
});

test('every blocking entry point is asynchronous', () => {
  const blocking = native.nativeSurface().filter((entry) => entry.blocking);
  // Held against the package's own typing of the addon rather than a list kept
  // here: the table's blocking entries are exactly the `Native` members typed
  // as a promise.
  const typescript = readFileSync(resolve(here, '../src/native.ts'), 'utf8');
  const at = typescript.indexOf('export interface Native {');
  assert.notEqual(at, -1, 'src/native.ts declares no interface Native');
  const body = typescript.slice(at, typescript.indexOf('\n}', at));
  const promised = [...body.matchAll(/^ {2}(\w+)\([^;]*?\): Promise</gm)].map(([, name]) => name);
  assert.deepEqual(blocking.map((entry) => entry.name).sort(), promised.sort());
  for (const entry of blocking) {
    assert.equal(typeof native[entry.name], 'function');
  }
  // `stopHost` is the one that can be called with no context and observed
  // returning a promise; the others need a live context and a running core,
  // and are exercised end to end by the conformance suite.
  const returned = native.stopHost();
  assert.ok(returned instanceof Promise, 'stopHost returns a promise');
  return returned;
});

test('the package and the binary agree on the activation schema', () => {
  assert.equal(
    native.schemaVersion(),
    ACTIVATION_SCHEMA_VERSION,
    'the package was built against a different activation schema than the binary',
  );
});

test('a core refusal is the one error, carrying the code and the engine detail', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    assert.throws(
      () => native.declareClass(Buffer.from('{"factory_id":"x"}')),
      (error) =>
        error instanceof FlezhaError &&
        error.code === 'topology.declaration_unreadable' &&
        error.class === 'invalid' &&
        error.details.reason.includes('sdk_lang'),
    );
    assert.equal(JSON.parse(native.lastError()).error.code, 'topology.declaration_unreadable');
  } finally {
    native.contextFree();
  }
});

// The checkpoint acknowledgements the core accepts. Asserted by registering,
// because the core is what reads the tree and refuses a malformed one.
const checkpointClass = (factoryId, ack) =>
  defineRelay({
    factoryId,
    flow: { exactly: { effectively_once: { ack } } },
    channels: (c) => {
      c.input('input')
        .into(defineTasklet({ name: `${factoryId}_tasklet`, run: (input, ctx) => ctx.emit(input) }))
        .keyed()
        .output('sink');
    },
  });

const declaresTo = (native, spec) =>
  native.declareClass(Buffer.from(JSON.stringify(toHostClassDecl(spec))));

test('holding outputs at the checkpoint registers with its delay', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const spec = checkpointClass('outputs_only', {
      checkpoint: { outputs: 'at_checkpoint', maxDelayMs: 1500 },
    });
    assert.deepEqual(toHostClassDecl(spec).run.flow.exactly.effectively_once.ack, {
      checkpoint: { outputs: 'at_checkpoint', max_delay_ms: 1500 },
    });
    declaresTo(native, spec);
  } finally {
    native.contextFree();
  }
});

test('the bare checkpoint acknowledgement registers with no cadence', () => {
  native.contextNew(ACTIVATION_SCHEMA_VERSION);
  try {
    const spec = checkpointClass('ack_only', 'checkpoint');
    assert.equal(toHostClassDecl(spec).run.flow.exactly.effectively_once.ack, 'checkpoint');
    declaresTo(native, spec);
  } finally {
    native.contextFree();
  }
});

test('the monotonic clock advances', () => {
  const first = native.monotonicNanos();
  assert.equal(typeof first, 'bigint');
  assert.ok(native.monotonicNanos() >= first);
});

test('a knob is parsed by the core, which answers unset for text that is no count', () => {
  const name = 'FLZ_TEST_KNOB';
  for (const [raw, expected] of [
    ['5', 5],
    ['0', 7],
    ['lots', 7],
    [undefined, 7],
  ]) {
    if (raw === undefined) delete process.env[name];
    else process.env[name] = raw;
    assert.equal(envPositive(name, 7), expected, String(raw));
  }
});

test('both module systems import the package and expose the same surface', () => {
  const script = `
    const cjs = require('${resolve(here, '../dist/cjs/index.js')}');
    import('${resolve(here, '../dist/esm/index.js')}').then((esm) => {
      const cjsKeys = Object.keys(cjs).sort();
      const esmKeys = Object.keys(esm).filter((k) => k !== 'default').sort();
      if (JSON.stringify(cjsKeys) !== JSON.stringify(esmKeys)) {
        console.error('surfaces differ', cjsKeys, esmKeys);
        process.exit(1);
      }
      if (cjs.ACTIVATION_SCHEMA_VERSION !== esm.ACTIVATION_SCHEMA_VERSION) process.exit(1);
      if (typeof cjs.createRunner !== 'function' || typeof esm.createRunner !== 'function') {
        process.exit(1);
      }
      console.log(cjsKeys.length);
    });
  `;
  const out = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8' });
  assert.ok(Number(out.trim()) > 15, `expected a populated surface, got ${out}`);
});
