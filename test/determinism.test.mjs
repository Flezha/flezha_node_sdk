// Ambient non-determinism is reported, not refused.
import assert from 'node:assert/strict';
import test from 'node:test';

import { defineTasklet, setLogger } from '../dist/esm/index.js';
import {
  installDeterminismWarnings,
  uninstallDeterminismWarnings,
} from '../dist/esm/determinism.js';
import { TaskletTestHarness } from '../dist/esm/testing.js';

function capture() {
  const lines = [];
  setLogger({
    debug: (m) => lines.push(m),
    info: (m) => lines.push(m),
    warn: (m) => lines.push(m),
    error: (m) => lines.push(m),
  });
  return lines;
}

test('an ambient clock read names the file, the line and the replacement', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Ambient = defineTasklet({
      name: 'ambient_clock',
      run: () => {
        Date.now();
        return undefined;
      },
    });
    await TaskletTestHarness.of(Ambient).feed(1);
    const warning = lines.find((line) => line.includes('Date.now()'));
    assert.ok(warning, `expected a warning, got ${JSON.stringify(lines)}`);
    assert.match(warning, /ambient_clock/);
    assert.match(warning, /determinism\.test\.mjs:\d+:\d+/);
    assert.match(warning, /ctx\.now\(\)/);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('new Date() with no argument is reported, and with one is not', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Mixed = defineTasklet({
      name: 'mixed_clock',
      run: (_input, ctx) => {
        const derived = new Date(Number(ctx.now()));
        assert.ok(derived instanceof Date);
        new Date();
        return undefined;
      },
    });
    await TaskletTestHarness.of(Mixed).feed(1);
    assert.equal(lines.filter((line) => line.includes('new Date()')).length, 1);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('random identifiers are reported and the invocation still completes', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Random = defineTasklet({
      name: 'random_id',
      run: (_input, ctx) => {
        Math.random();
        globalThis.crypto.randomUUID();
        ctx.emit('finished');
      },
    });
    const applied = await TaskletTestHarness.of(Random).feed(1);
    assert.deepEqual(applied.emits, ['finished'], 'the warning does not abort the invocation');
    assert.ok(lines.some((line) => line.includes('Math.random()')));
    assert.ok(lines.some((line) => line.includes('crypto.randomUUID()')));
    assert.ok(lines.every((line) => line.includes('ctx.')));
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('one site reports once, however hot the loop', async () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    const Hot = defineTasklet({
      name: 'hot_loop',
      run: () => {
        for (let i = 0; i < 50; i += 1) Math.random();
        return undefined;
      },
    });
    await TaskletTestHarness.of(Hot).feed(1);
    assert.equal(lines.filter((line) => line.includes('Math.random()')).length, 1);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('an ambient read outside an invocation is not reported', () => {
  const lines = capture();
  installDeterminismWarnings();
  try {
    Date.now();
    Math.random();
    assert.deepEqual(lines, []);
  } finally {
    uninstallDeterminismWarnings();
  }
});

test('uninstalling restores the originals', () => {
  const before = Date.now;
  installDeterminismWarnings();
  assert.notEqual(Date.now, before);
  uninstallDeterminismWarnings();
  assert.equal(Date.now, before);
});
