import { createRunner } from '@flezha/sdk';

/**
 * Starts the desk's runner: the process that connects to the controller and
 * serves agentdesk's operators, in the `agents` namespace. An operator is
 * registered by module specifier, which resolves against the working
 * directory, so start the runner from this folder.
 */
const runner = createRunner('agents');
await runner.registerFactory('./agent_operator.mjs');
await runner.registerFactory('./gateway_operator.mjs');

await runner.run();
