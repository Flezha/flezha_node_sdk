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

// Everything `flz operator` and `flz plugin` do is also a method of
// `control()`, and `send` hands a message to an operator. Uncomment to drive
// this namespace from the application instead of from a terminal:
//
// const control = runner.control();
// await control.startOperator('echo', { id: 'my_echo' });
// await control.enablePlugin('writer', 'operator_file_sink', { opts: { path: 'echo-out.txt' } });
// await control.bindPlugin('writer', 'my_echo', { hooks: [['on_output', {}]] });
// await runner.send('my_echo', 'hello world', 'in', { msgId: 'hello-1' });
//
// docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
// controller refuses all of it until `flz role bind runner@default app default`.
