import { defineOperator } from '@flezha/sdk';

import { Admit } from './admit.mjs';
import { Budget, admitted, used } from './budget.mjs';
import { Request } from './request.mjs';
import { Watch } from './watch.mjs';

const DEADLINE_MS = 60_000;
const user = (value) => value.user;

/**
 * The HTTP side of the desk: a flow operator that carries the HTTP
 * plugin, answers every request, and calls the session a message is for.
 */
export default defineOperator({
  factoryId: 'gateway',
  flow: { exactly: 'effectively_once' }, // each request and answer on disk before it runs
  channels: (c) => {
    const http = c.input('http', Request.decode);
    http
      .filter((request) => !request.sends())
      .into(Watch)
      .response(); // a stream, or 404
    const verdicts = http
      .filter((request) => request.sends())
      .map((request) => request.message())
      .keyBy(user) // one cell per user
      .into(Budget);
    verdicts.into(Admit).response(); // 202, or 429 over the budget
    verdicts
      .filter(admitted)
      .map((verdict) => verdict.message)
      .keyBy(user) // the user's session
      .call({ factory: 'agent', channel: 'message' }, { deadlineMs: DEADLINE_MS })
      .map(used)
      .keyBy(user)
      .into(Budget); // the tokens the answer used
  },
});
