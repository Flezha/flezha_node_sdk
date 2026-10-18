import { test } from 'node:test';
import assert from 'node:assert/strict';

import { ACTIVATION_SCHEMA_VERSION, FORMAT_ID_MSGPACK, SDK_LANG_NODE } from '../dist/esm/index.js';
import { CELL_STAMP, KEY_STAMP, RESERVED_PREFIX, SEQ_STAMP } from '../dist/esm/ctx.js';

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

test('the output stamps and the reserved prefix are the fixture keys', () => {
  assert.deepEqual(
    [CELL_STAMP, SEQ_STAMP, KEY_STAMP],
    [row('flz_key', 'cell'), row('flz_key', 'seq'), row('flz_key', 'key')],
  );
  const key = row('flz_key', 'key');
  assert.equal(RESERVED_PREFIX, key.slice(0, key.indexOf('.') + 1));
});
