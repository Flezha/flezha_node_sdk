import { test } from 'node:test';
import assert from 'node:assert/strict';

import { ACTIVATION_SCHEMA_VERSION, defineTasklet } from '../dist/esm/index.js';
import { FORMAT_ID_MSGPACK, SDK_LANG_NODE } from '../dist/esm/graph.js';
import { RESERVED_PREFIX } from '../dist/esm/ctx.js';
import { loadNative } from '../dist/esm/native.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

import { read } from './contract.mjs';

// This runtime's wire constants, held to the shared constants fixture.
function row(table, name) {
  const line = read('test/fixtures/wire_constants.txt')
    .split('\n')
    .find((l) => l.startsWith(`${table}|${name}|`));
  assert.ok(line, `test/fixtures/wire_constants.txt has no row ${table}|${name}`);
  return line.split('|')[2];
}

test('the language byte, codec and schema are the fixture rows', () => {
  assert.equal(SDK_LANG_NODE, Number(row('sdk_lang', 'node')));
  assert.equal(FORMAT_ID_MSGPACK, Number(row('format_id', 'msgpack')));
  assert.equal(ACTIVATION_SCHEMA_VERSION, Number(row('schema_version', 'activation')));
});

test('the reserved prefix is the fixture keys', () => {
  const key = row('flz_key', 'key');
  assert.equal(RESERVED_PREFIX, key.slice(0, key.indexOf('.') + 1));
});

test("the harness's effect name bound is the core's", async () => {
  // A refusal is rendered by the addon.
  loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);
  const bound = Number(row('effect', 'name_max_bytes'));
  const Named = defineTasklet({
    name: 'effect_name_bound',
    run: (name, ctx) => ctx.effect(name, () => 'ran'),
  });

  await TaskletTestHarness.of(Named).feed('m'.repeat(bound));
  await assert.rejects(
    TaskletTestHarness.of(Named).feed('m'.repeat(bound + 1)),
    (error) => error.code === 'effect.bad_name',
  );
});
