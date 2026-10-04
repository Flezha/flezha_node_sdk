import { defineOperator } from '@flezha/sdk';

import { Deliver } from './deliver.mjs';
import { Reply } from './reply.mjs';

/**
 * One conversation: an operator that runs in steps, one turn per message it is
 * sent, for as long as the session lasts. Every session is an instance of its
 * own, with its own state.
 */
export default defineOperator({
  factoryId: 'agent',
  // one turn per message; a failed turn fails alone
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } }, // an idle session sleeps after 10 s
  channels: (c) => {
    c.input('turn', { codec: 'text' }) // entry 1: `flz operator call`
      .into(Reply)
      .sink('answer') // to whoever listens; the chain goes on
      .into(Deliver)
      .response(); // to whoever called
    c.input('message') // entry 2: the gateway, into the same Reply
      .map((message) => message.text)
      .into(Reply);
  },
});
