/**
 * What runs on a dispatch worker thread.
 *
 * A worker imports the handler modules itself — the reason registration takes
 * a module specifier rather than a value — builds its own registry, and
 * registers its event loop as a dispatch target with the native host. From
 * there it is the same dispatch path the main loop runs: the fetcher chose
 * this loop, and nothing else differs.
 *
 * Module-scope state in a handler module is therefore instantiated once per
 * worker. That is a semantic, not an implementation detail: a connection pool
 * or a cache is fine, an in-memory counter or a run-once flag is not. Durable
 * state is unaffected — the core owns it, through the declared change fold.
 */

import { parentPort, workerData } from 'node:worker_threads';

import { type Registry, dispatch, dispatchDeps } from './dispatch.js';
import { validate } from './graph.js';
import { LoopWatch } from './loop-watch.js';
import { log } from './logger.js';
import { loadNative } from './native.js';
import { importModule, loadRegistry, specsFrom } from './runner.js';
import type { FromWorker, ToWorker } from './workers.js';

interface WorkerConfig {
  namespace: string;
  specifiers: string[];
  index: number;
}

async function main(): Promise<void> {
  const port = parentPort;
  if (!port) throw new Error('worker-entry ran outside a worker thread');
  const config = workerData as WorkerConfig;

  // The addon is one shared library loaded once per process, so this worker's
  // `require` reaches the same core the main thread created the context on.
  const native = loadNative();
  const registry = await loadRegistry(config.specifiers);

  const watch = new LoopWatch(`worker-${config.index}`);
  watch.start();

  const deps = dispatchDeps(native, registry);

  const dispatchId = native.registerDispatch((activation) => {
    const registered = registry.get(activation.factory);
    const node = registered?.spec.tasklets[activation.tasklet];
    watch.observe(node?.name ?? activation.factory);
    void dispatch(activation, native, deps);
  });

  port.on('message', (message: ToWorker) => {
    if (message.type === 'stop') {
      // Releasing the registration is what abandons whatever this loop still
      // held, natively, before the process frees the core.
      native.unregisterDispatch(dispatchId);
      watch.stop();
      port.close();
      return;
    }
    void change(registry, message).then(
      () => port.postMessage({ type: 'changed' } satisfies FromWorker),
      (error) => port.postMessage({ type: 'failed', message: String(error) } satisfies FromWorker),
    );
  });

  port.postMessage({ type: 'ready' } satisfies FromWorker);
}

/** Declare or remove a module's classes on this worker's registry. */
async function change(
  registry: Registry,
  message: Extract<ToWorker, { specifier: string }>,
): Promise<void> {
  for (const spec of specsFrom(message.specifier, await importModule(message.specifier))) {
    if (message.type === 'remove') {
      registry.remove(spec.factoryId);
    } else {
      validate(spec);
      registry.add(spec);
    }
  }
}

main().catch((error) => {
  log().error(`dispatch worker failed to start: ${String(error)}`);
  parentPort?.postMessage({ type: 'failed', message: String(error) } satisfies FromWorker);
  process.exit(1);
});
