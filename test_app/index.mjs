#!/usr/bin/env node
// The Node SDK's e2e test app.
//
// Registers the reference fixture twins and runs until shutdown. The namespace
// is argv[1] (default "default"), matching rust_runner / java_runner /
// elixir_runner. Dispatch topology is an axis of the run rather than a property
// of this file: FLEZHA_NODE_WORKERS selects the single-loop default or a
// worker-thread pool, and the fixtures are identical under both.
//
// Two environment variables select a conformance run instead:
// FLEZHA_CONFORMANCE_REPORT writes the refusal table's verdicts and exits
// without connecting, and FLEZHA_REFUSAL_FIXTURE registers ONE row of that
// table in place of the app's fixtures. Both are `conformance.mjs`.

import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { consoleLogger, createRunner } from '@flezha/sdk';

const here = dirname(fileURLToPath(import.meta.url));
const namespace = process.argv[2] || 'default';
const fixtures = resolve(here, 'fixtures.mjs');
const steps = resolve(here, 'fixtures_steps.mjs');
const calls = resolve(here, 'fixtures_call.mjs');
const inner = resolve(here, 'fixtures_inner.mjs');
const conformance = resolve(here, 'conformance.mjs');
const workers = Number(process.env.FLEZHA_NODE_WORKERS ?? 0) || 0;
const reportAt = process.env.FLEZHA_CONFORMANCE_REPORT;
const refusalFixture = process.env.FLEZHA_REFUSAL_FIXTURE;

process.on('unhandledRejection', (reason) => {
  console.error(`[node_runner] unhandled rejection: ${reason}`);
});

// The harness stops a runner by closing its pipes. A write that lands after
// that raises EPIPE on the stream, and an unhandled stream error would turn a
// clean drain into a crash the run reports as a runner failure.
for (const stream of [process.stdout, process.stderr]) {
  stream.on('error', () => {});
}

if (reportAt) {
  const { writeConformanceReport } = await import(conformance);
  writeConformanceReport(reportAt);
  process.exit(0);
}

const runner = createRunner(namespace)
  .logger(consoleLogger(process.env.FLEZHA_LOG_LEVEL ?? 'info'))
  .workers(workers);

if (refusalFixture) {
  // ONE row of the refusal table, in place of every fixture: the drill reads
  // this process's exit status and output, because a runner whose class the
  // core refuses never reaches a controller.
  await runner.registerFactory(conformance);
} else {
  await runner.registerFactory(fixtures);
  await runner.registerFactory(steps);
  await runner.registerFactory(calls);
  await runner.registerFactory(inner);
}

try {
  await runner.run();
} catch (error) {
  // A refused declaration names itself: the error's code, and every issue's
  // code the core listed, ride the exception as data, and the drill reads them
  // off this process's output.
  if (typeof error.code === 'string' && error.details) {
    const issues = Array.isArray(error.details.issues) ? error.details.issues : [error];
    console.error(`[node_runner] declaration refused: ${issues.map((i) => i.code).join(', ')}`);
  }
  console.error(String(error.stack ?? error));
  process.exit(1);
}

console.log(
  `[node_runner] namespace=${namespace} transport=${process.env.FLEZHA_TRANSPORT ?? 'uds'} ` +
    `dispatch=${workers === 0 ? 'main-loop' : `${workers} workers`}`,
);
