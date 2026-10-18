import { test } from 'node:test';
import assert from 'node:assert/strict';

import { ACTIVATION_SCHEMA_VERSION, FORMAT_ID_MSGPACK, SDK_LANG_NODE } from '../dist/esm/index.js';

import { read } from './contract.mjs';

// This runtime's wire constants, held to the shared constants fixture.
function row(table, name) {
  const line = read('test/fixtures/wire_constants.txt')
    .split('\n')
    .find((l) => l.startsWith(`${table}|${name}|`));
  assert.ok(line, `test/fixtures/wire_constants.txt has no row ${table}|${name}`);
  return Number(line.split('|')[2]);
}

test('the language byte, codec and schema are the fixture rows', () => {
  assert.equal(SDK_LANG_NODE, row('sdk_lang', 'node'));
  assert.equal(FORMAT_ID_MSGPACK, row('format_id', 'msgpack'));
  assert.equal(ACTIVATION_SCHEMA_VERSION, row('schema_version', 'activation'));
});
