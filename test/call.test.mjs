// `call(...)`: the link the builder records, the Response channel it declares
// and decodes, the requests an emit makes, and the reference rows.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test, { after } from 'node:test';

import {
  ACTIVATION_SCHEMA_VERSION,
  FlezhaError,
  defineTasklet,
  defineRelay,
  loadNative,
  toHostClassDecl,
  validate,
} from '../dist/esm/index.js';
import { answer } from '../dist/esm/dispatch.js';

import { member, read } from './contract.mjs';
import { activationFor, codesOf, depsFor, inputItem, refusal } from './dispatch-fixtures.mjs';
import { testAppModule } from './test-app.mjs';

// The dispatch case answers against the core, which renders its refusals.
const native = loadNative();
native.contextNew(ACTIVATION_SCHEMA_VERSION);
after(() => native.contextFree());

const echo = (name) => defineTasklet({ name, run: (input, ctx) => ctx.emit(input) });
const b64 = (bytes) => Buffer.from(bytes).toString('base64');

/** A caller whose `ask` channel calls with `codec`, the outcome fed back into `asker`. */
function caller(codec) {
  const Asker = echo('asker');
  return defineRelay({
    factoryId: 'caller',
    channels: (c) => {
      c.input('in', { codec: 'text' })
        .into(Asker)
        .named('ask')
        .filter((line) => !line.startsWith('drop'))
        .flatMap((line) => [line, `${line}!`])
        .keyBy((line) => line.split(':')[0])
        .call(
          { factory: 'agent', create: false, channel: 'turn' },
          { deadlineMs: 60_000, answerChannel: 'reply', codec },
        )
        .map((response) => `answered ${response.request}=${response.value}@${response.target}`)
        .into(Asker);
    },
  });
}

test('call closes its chain with the call link and declares <channel>.response', () => {
  const spec = caller('text');
  const declared = toHostClassDecl(spec).channels;
  assert.deepEqual(declared[1].chains.at(-1), [
    { link: 'transform' },
    { link: 'transform' },
    { link: 'key' },
    {
      link: 'call',
      factory: 'agent',
      deadline_ms: 60_000,
      create: false,
      channel: 'turn',
      answer_channel: 'reply',
      response: 'ask.response',
    },
  ]);
  assert.deepEqual(
    validate(spec).channels.map((c) => [
      c.name,
      c.kind,
      c.producer,
      c.consumers.map((e) => e.tasklet),
    ]),
    [
      ['in', 'entry', null, [0]],
      ['ask', 'plugin', 0, []],
      ['ask.response', 'entry', null, [0]],
    ],
  );
});

test('a call to a named instance records its target and create true', () => {
  const spec = defineRelay({
    factoryId: 'named_call',
    channels: (c) => {
      const asked = c.input('in').into(echo('asker'));
      asked.call('pricing', { deadlineMs: 500 }).into(echo('reader'));
    },
  });
  assert.deepEqual(toHostClassDecl(spec).channels[1].chains, [
    [
      {
        link: 'call',
        target: 'pricing',
        deadline_ms: 500,
        create: true,
        response: 'asker.response',
      },
    ],
  ]);
});

test('a call on an entry channel is refused as an output on one is', () => {
  assert.throws(
    () =>
      defineRelay({
        factoryId: 'entry_call',
        channels: (c) => {
          c.input('in').call('pricing', { deadlineMs: 500 });
        },
      }),
    (error) =>
      error instanceof FlezhaError &&
      error.code === 'topology.output_on_entry' &&
      error.details.channel === 'in',
  );
});

test('every envelope case of the shared fixture decodes to its expected fields', () => {
  const decode = caller('bytes').channels[2].decode;
  const { cases } = JSON.parse(
    readFileSync(member('test/fixtures/call_response_envelope.json'), 'utf8'),
  );
  assert.ok(cases.length > 0);
  for (const { name, payload, expect } of cases) {
    const response = decode(Buffer.from(payload, 'utf8'));
    const got = {
      target: response.target,
      created: response.created,
      request: b64(response.request),
      ok: response.ok,
    };
    if (response.ok) got.value = b64(response.value);
    else {
      assert.ok(response.error instanceof FlezhaError, name);
      got.error_code = response.error.code;
      got.error_class = response.error.class;
      // The one error type prefixes its message with the code.
      got.error_message = response.error.message.slice(`${response.error.code}: `.length);
    }
    assert.deepEqual(got, expect, name);
  }
});

test('the call codec decodes a text request and a JSON answer', () => {
  const envelope = (codecValue) =>
    Buffer.from(
      JSON.stringify({
        call: 'c',
        target: 'anna-42',
        created: true,
        request: b64(Buffer.from(codecValue.request)),
        value: b64(Buffer.from(codecValue.value)),
      }),
    );
  const text = caller('text').channels[2].decode(
    envelope({ request: 'anna-42:hello', value: 'hi' }),
  );
  assert.deepEqual([text.request, text.value], ['anna-42:hello', 'hi']);
  const json = caller(undefined).channels[2].decode(
    envelope({ request: '"anna-42:hello"', value: '{"tokens":812}' }),
  );
  assert.deepEqual([json.request, json.value], ['anna-42:hello', { tokens: 812 }]);
});

test('each request the call chain makes leaves as a plugin output keyed by the chain', async () => {
  const spec = caller('text');
  const run = async (item) =>
    (await answer(activationFor('caller')({ items: [item] }), depsFor(spec))).effects[0].outputs;
  const text = (bytes) => Buffer.from(bytes).toString('utf8');

  const asked = await run(inputItem(null, { payload: Buffer.from('s1:hi') }));
  assert.deepEqual(
    asked.map((o) => [o.target, o.pluginChannel, o.key, text(o.payload)]),
    [
      ['plugin', 'ask', 's1', 's1:hi'],
      ['plugin', 'ask', 's1', 's1:hi!'],
    ],
  );
  assert.deepEqual(await run(inputItem(null, { payload: Buffer.from('drop:x') })), []);

  const envelope = {
    call: 'c',
    target: 's1',
    created: true,
    request: b64('s1:hi'),
    value: b64('ok'),
  };
  const answered = await run(
    inputItem(null, { channel: 2, payload: Buffer.from(JSON.stringify(envelope)) }),
  );
  assert.deepEqual(
    [answered[0].key, text(answered[0].payload)],
    ['answered s1', 'answered s1:hi=ok@s1'],
  );
});

test('the reference call rows refuse or derive as the fixture states', async () => {
  const { specForTopology } = await testAppModule('conformance.mjs');
  const table = read('test/fixtures/reference_topologies.txt')
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith('#'))
    .map((line) => line.split('|').map((part) => part.trim()));
  const rows = table.filter((columns) => columns.length === 4 && columns[1].startsWith('call_'));
  assert.equal(rows.length, 7);
  for (const [verdict, topology] of rows) {
    if (verdict !== 'ok') {
      assert.ok(codesOf(refusal(() => specForTopology(topology))).includes(verdict), topology);
      continue;
    }
    const lines = validate(specForTopology(topology)).channels.map((c) =>
      [
        topology,
        c.index,
        c.name,
        c.kind,
        c.producer ?? '-',
        c.consumers.length > 0 ? c.consumers.map((e) => e.tasklet).join(',') : '-',
        c.sink ?? '-',
      ].join(' | '),
    );
    const golden = table.filter((columns) => columns.length === 7 && columns[0] === topology);
    assert.deepEqual(
      lines,
      golden.map((columns) => columns.join(' | ')),
      topology,
    );
  }
});

test('the relay_call fixtures the manifest lists are declared and pass the core', async () => {
  const { manifest } = await import('./test-app.mjs');
  const fixtures = Object.values(await testAppModule('fixtures_call.mjs')).filter(
    (spec) => typeof spec === 'object' && spec.factoryId,
  );
  assert.deepEqual(
    fixtures.map((spec) => spec.factoryId).sort(),
    [...manifest().fixtures.relay_call].sort(),
  );
  for (const spec of fixtures) validate(spec);
});
