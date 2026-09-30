import { defineBounded } from '@flezha/sdk';

import { Deliver } from './deliver.mjs';
import { Reply } from './reply.mjs';

/**
 * One conversation: a bounded operator that runs one turn per message it is
 * sent, for as long as the session lasts. Every session is an instance of its
 * own, with its own state.
 */
export default defineBounded({
  factoryId: 'agent',
  loopMode: 'per_input', // one turn per message
  rollbackMode: 'keep_progress', // a failed turn fails alone
  channels: (c) => {
    c.input('turn', { codec: 'text' })
      .into(Reply)
      .sink('answer') // to whoever listens
      .into(Deliver)
      .response(); // to whoever called
    c.input('message') // from the gateway
      .map((message) => message.text)
      .into(Reply);
  },
});
