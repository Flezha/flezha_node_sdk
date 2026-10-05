// The relay_call fixtures: relays that call another relay from their
// graph and read the outcome as a `Response`, the mirror of
// `sdk/rust/test_app/src/relay_call.rs`. Every outcome is written as one
// line, `<request>=ok:<answer>@<target>/<created>` or
// `<request>=error:<code>@<target>/<created>`.

import { setTimeout as sleep } from 'node:timers/promises';

import { defineRelay, defineTasklet } from '@flezha/sdk';

import { fixtureDurable } from './fixture_support.mjs';

/** The deadline every fixture call carries, with UTF-8 requests and answers. */
const CALL = { deadlineMs: 30_000, codec: 'text' };

/** The prefix an outcome line carries, which no request does. */
const OUTCOME = 'response ';

/** One outcome as the line every SDK's fixture writes. */
export const render = (response) =>
  `${response.request}=${response.ok ? `ok:${response.value}` : `error:${response.error.code}`}` +
  `@${response.target}/${response.created}`;

/** The prefix of a turn that may only reach a session already running. */
const RESUME = 'resume:';

const isRequest = (line) => !line.startsWith(OUTCOME);
const isTurn = (line) => isRequest(line) && !line.startsWith(RESUME);
const isResume = (line) => line.startsWith(RESUME);
const isOutcome = (line) => line.startsWith(OUTCOME);

/** The session a turn or an outcome line belongs to: what precedes its `:`. */
const sessionOf = (line) => (isOutcome(line) ? line.slice(OUTCOME.length) : line).split(':')[0];

const step = (name, run) => defineTasklet({ name, run });

/** A caller's tasklet: a turn goes on as it came, an outcome as its line. */
const account = (name) =>
  step(name, (input, ctx) =>
    ctx.emit(input.turn !== undefined ? input.turn : `${OUTCOME}${render(input.outcome)}`),
  );

/** An outcome line without its prefix. */
const outcomeLog = (name) => step(name, (input, ctx) => ctx.emit(input.slice(OUTCOME.length)));

const sessionKnobs = { durable: fixtureDurable(), maxDurationMs: 600_000 };

export const callPricing = defineRelay({
  factoryId: 'call_pricing',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(
        step('pricing_quote', (input, ctx) => {
          if (!input.startsWith('silent')) ctx.emit(`price:${input}`);
        }),
      )
      .response();
  },
});

export const callOrders = defineRelay({
  factoryId: 'call_orders',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(step('order_ask', (input, ctx) => ctx.emit(input)))
      .named('order')
      .call('pricing', CALL)
      .into(step('order_priced', (response, ctx) => ctx.emit(render(response))))
      .sink('priced');
  },
});

export const callAgent = defineRelay({
  factoryId: 'call_agent',
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  ...sessionKnobs,
  channels: (c) => {
    c.input('turn', { codec: 'text' })
      .into(
        step('agent_turn', async (input, ctx) => {
          const colon = input.indexOf(':');
          const text = colon === -1 ? input : input.slice(colon + 1);
          if (text === 'boom')
            throw Object.assign(new Error('the turn failed'), { nonRetryable: true });
          if (text === 'slow') await sleep(2_000);
          ctx.emit(`answer:${input}`);
        }),
      )
      .response();
  },
});

export const callGateway = defineRelay({
  factoryId: 'call_gateway',
  flow: { exactly: 'effectively_once' },
  channels: (c) => {
    const GatewayAccount = account('gateway_account');
    const gateway = c
      .input('in', { codec: 'text' })
      .map((turn) => ({ turn }))
      .into(GatewayAccount)
      .named('gateway');
    gateway
      .filter(isTurn)
      .keyBy(sessionOf)
      .call({ factory: 'call_agent' }, CALL)
      .map((outcome) => ({ outcome }))
      .into(GatewayAccount);
    gateway
      .filter(isResume)
      .into(step('gateway_resume', (input, ctx) => ctx.emit(input.slice(RESUME.length))))
      .named('resume')
      .keyBy(sessionOf)
      .call({ factory: 'call_agent', create: false }, CALL)
      .map((outcome) => ({ outcome }))
      .into(GatewayAccount);
    gateway
      .filter(isOutcome)
      .keyBy(sessionOf)
      .into(outcomeLog('gateway_log'))
      .keyed()
      .sink('gateway_out');
  },
});

export const callToolSession = defineRelay({
  factoryId: 'call_tool_session',
  steps: { each: 'per_input' },
  ...sessionKnobs,
  channels: (c) => {
    const ToolTurn = account('tool_turn');
    const tool = c
      .input('turn', { codec: 'text' })
      .map((turn) => ({ turn }))
      .into(ToolTurn)
      .named('tool');
    tool
      .filter(isRequest)
      .call('pricing', CALL)
      .map((outcome) => ({ outcome }))
      .into(ToolTurn);
    tool.filter(isOutcome).into(outcomeLog('tool_log')).sink('tool_out');
  },
});
