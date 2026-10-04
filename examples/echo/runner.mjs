import { createRunner } from '@flezha/sdk';

/**
 * Starts a runner: a process that connects to the controller and serves your
 * operators.
 *
 * An operator is registered by module specifier rather than as an imported
 * value, because every dispatch loop imports the module itself. That is what
 * lets `.workers(n)` move dispatch onto worker threads without a rewrite.
 * Registering therefore imports, which is asynchronous, so each one is its own
 * `await` and a module that fails to load says so at the line that names it.
 */
const namespace = process.argv[2] ?? 'default';

const runner = createRunner(namespace);
await runner.registerFactory('./echo_operator.mjs'); // the operators this runner serves

// Resolves once the runner is connected and serving. The process then keeps
// running until a SIGTERM, or until standard input closes.
await runner.run(); // connects to the controller

// Everything `flz operator` and `flz plugin` do is also a method here.
// Uncomment to drive this namespace from the application instead of from a
// terminal:
//
// const admin = runner.admin();
// await admin.startOperator('echo', { id: 'my_echo' });
// await admin.enablePlugin('writer', 'operator_file_sink', { opts: { path: 'echo-out.txt' } });
// await admin.bindPlugin('writer', 'my_echo', { hooks: [['on_output', {}]] });
// await admin.send('my_echo', 'hello world', 'in');
//
// docs/sdk/admin.md has the whole surface. An enforced controller refuses all
// of it until `flz role bind runner@default app default`.
