import { defineTasklet } from '@flezha/sdk';

/**
 * Hands the finished answer to the caller: a channel has one end, and `answer`
 * is a sink.
 */
export const Deliver = defineTasklet({
  name: 'deliver',

  // The whole answer, to whoever called.
  run: (answer, ctx) => ctx.emit(answer),

  // The undo, when a turn is taken back: an answer already given has nothing to take back.
  compensate: () => {},
});
