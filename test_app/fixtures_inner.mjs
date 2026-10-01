// The inner_operator fixtures: one stepped session per key inside a flow
// gateway, the mirror of `sdk/rust/test_app/src/inner.rs`. `key:text` inputs;
// each key's session counts its turns in its own state and answers
// `key#turns:text` on the gateway's `replies` sink. The session sleeps after
// `FLEZHA_FIXTURE_SLEEP_AFTER_MS` and wakes on its key's next message with its
// count intact, while the gateway and every other key go on. Its second stage
// fails on the text `fail` (the turn is undone) and holds `hold` until the
// release env (a crash lands between the two stages); the gateway's reply holds
// `stall` the same way (a crash lands after the session answered).
//
// `loopGateway` embeds the chained `countdown`: `label:n` counts down one per
// pass, and only the converged `label:0` reaches the gateway's `replies`.

import { defineOperator, defineTasklet } from '@flezha/sdk';

import { blockUntilReleased, fixtureDurable, prefix } from './fixture_support.mjs';

const SessionTurn = defineTasklet({
  name: 'session_turn',
  initState: () => ({ value: 0 }),
  applyChange: (state, change) => {
    state.value += change.delta;
  },
  run: (input, ctx) => {
    const [key, text] = String(input).split(':');
    ctx.change({ delta: 1 });
    ctx.emit(`${key}#${ctx.state.value}:${text}`);
  },
  compensate: (_outputs, ctx) => {
    ctx.change({ delta: -1 });
  },
});

const SessionCheck = defineTasklet({
  name: 'session_check',
  run: async (input, ctx) => {
    const text = String(input).split(':')[1];
    if (text === 'fail') throw new Error('session_check refuses fail');
    if (text === 'hold') await blockUntilReleased();
    ctx.emit(input);
  },
});

// Not exported: the runner registers what a module exports, and the session
// is the gateway's, never a factory of its own.
const session = defineOperator({
  factoryId: 'session',
  steps: { each: 'per_input' },
  durable: fixtureDurable(),
  channels: (c) => {
    // A host tasklet emits only on a channel it declares: this one is the exit.
    c.input('turns', { codec: 'text' }).into(SessionTurn).into(SessionCheck).named('answer');
  },
});

const GatewayReply = defineTasklet({
  name: 'gateway_reply',
  run: async (input, ctx) => {
    if (String(input).split(':')[1] === 'stall') await blockUntilReleased();
    ctx.emit(input);
  },
});

export const gateway = defineOperator({
  factoryId: 'session_gateway',
  // It journals its input (a kept instance that may sleep), so an answer a
  // session passed it is replayed after a crash.
  durable: { local: { sleepAfter: 3_600_000 } },
  channels: (c) => {
    c.input('requests', { codec: 'text' })
      .keyBy(prefix)
      .into(session)
      .into(GatewayReply)
      .sink('replies', { codec: 'text' });
  },
});

const Countdown = defineTasklet({
  name: 'countdown_step',
  run: (input, ctx) => {
    const [label, n] = String(input).split(':');
    if (Number.parseInt(n, 10) > 0) ctx.emit(`${label}:${Number.parseInt(n, 10) - 1}`);
  },
});

const countdown = defineOperator({
  factoryId: 'countdown',
  steps: { each: 'chained' },
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(Countdown).named('carry');
  },
});

export const loopGateway = defineOperator({
  factoryId: 'loop_gateway',
  channels: (c) => {
    c.input('requests', { codec: 'text' })
      .into(countdown)
      .into(GatewayReply)
      .sink('replies', { codec: 'text' });
  },
});
