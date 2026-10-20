// A relay's API as the declaration announces it: each channel's codec, an
// entry's description and JSON Schema, and the schemas the core refuses.
import assert from 'node:assert/strict';
import test from 'node:test';

import { z } from 'zod';

import { ACTIVATION_SCHEMA_VERSION, defineRelay, defineTasklet } from '../dist/esm/index.js';
import { toHostClassDecl, validate } from '../dist/esm/graph.js';
import { loadNative } from '../dist/esm/native.js';

import { codesOf, refusal } from './dispatch-fixtures.mjs';

loadNative().contextNew(ACTIVATION_SCHEMA_VERSION);

const place = defineTasklet({ name: 'place', run: (order, ctx) => ctx.emit(order) });
const audit = defineTasklet({ name: 'audit', run: () => {} });

const ORDER = {
  type: 'object',
  properties: { pair: { type: 'string' }, amount: { type: 'number' } },
  required: ['pair', 'amount'],
};

const orders = (entry) =>
  defineRelay({
    factoryId: 'orders',
    channels: (c) => {
      c.input('place', entry).into(place).output('placed');
      c.input('audit', { codec: 'text' }).into(audit);
    },
  });

/** Each channel's API keys, by name, as the declaration carries them. */
const apiOf = (spec) =>
  Object.fromEntries(
    toHostClassDecl(spec).channels.map(({ name, codec, schema, description }) => [
      name,
      { codec, schema, description },
    ]),
  );

test('an entry announces its codec, description and schema, an output its codec', () => {
  const spec = orders({ description: 'Places an order', schema: ORDER });
  validate(spec);
  assert.deepEqual(apiOf(spec), {
    place: { codec: 'json', schema: ORDER, description: 'Places an order' },
    audit: { codec: 'text', schema: undefined, description: undefined },
    placed: { codec: 'json', schema: undefined, description: undefined },
  });
});

test('a decoder or a serializer of its own makes a channel custom', () => {
  const spec = defineRelay({
    factoryId: 'custom_codecs',
    channels: (c) =>
      c
        .input('in', (bytes) => bytes, { description: 'raw' })
        .into(place)
        .output('out', (value) => value),
  });
  validate(spec);
  assert.deepEqual(apiOf(spec), {
    in: { codec: 'custom', schema: undefined, description: 'raw' },
    out: { codec: 'custom', schema: undefined, description: undefined },
  });
});

test('a schema zod writes is accepted as declared', () => {
  const schema = z.toJSONSchema(z.object({ pair: z.string(), amount: z.number() }));
  const spec = orders({ schema });
  validate(spec);
  assert.deepEqual(apiOf(spec).place.schema, schema);
});

test('a schema that is not an object with a type is refused at registration', () => {
  for (const schema of ['object', { properties: {} }]) {
    const refused = refusal(() => orders({ schema }));
    assert.deepEqual(codesOf(refused), ['topology.schema_invalid']);
    assert.equal(refused.details.issues[0].details.channel, 'place');
  }
});

test('a schema on a channel that is not json is refused at registration', () => {
  const refused = refusal(() => orders({ codec: 'text', schema: ORDER }));
  assert.deepEqual(codesOf(refused), ['topology.schema_invalid']);
});
