/**
 * Compile-time refusals, pinned.
 *
 * Rust pins its refusals with compile-fail tests; TypeScript can pin the same
 * class of defect with `@ts-expect-error`. Each directive below asserts that
 * the line beneath it does NOT compile — so a refusal that stops working
 * fails the build with "Unused '@ts-expect-error' directive" rather than
 * silently becoming a runtime error, or nothing at all.
 *
 * These are the defects static typing can see. Everything it cannot — cycles,
 * missing entry channels, unservable clocks, keyed-channel rules — is refused
 * at registration and covered by `test/authoring.test.mjs`.
 */

import { defineUnbounded, defineTasklet, defineBounded, type Ctx } from '../src/index.js';

interface Trade {
  symbol: string;
  price: number;
}

interface Candle {
  symbol: string;
  close: number;
}

// The output type is the definition's type arguments, never the body's.
const Ohlc = defineTasklet<Trade, Candle>({
  name: 'ohlc',
  run: (trade, ctx) => ctx.emit({ symbol: trade.symbol, close: trade.price }),
});

const Publish = defineTasklet<Candle, string>({
  name: 'publish',
  run: (candle, ctx) => ctx.emit(`${candle.symbol}:${candle.close}`),
});

// Or the annotation on the context, which names the state and change types too.
const OhlcByContext = defineTasklet({
  name: 'ohlc_by_context',
  initState: () => ({ last: 0 }),
  applyChange: (state, move: { price: number }) => {
    state.last = move.price;
  },
  run: (trade: Trade, ctx: Ctx<Candle, { last: number }, { price: number }>) => {
    ctx.change({ price: trade.price });
    ctx.emit({ symbol: trade.symbol, close: trade.price });
  },
});

/** A body that emits nothing, for the fixtures whose subject is elsewhere. */
const quiet = (_line: string): void => {};

const Count = defineTasklet({
  name: 'count',
  initState: () => ({ total: 0 }),
  applyChange: (state, change: { by: number }) => {
    state.total += change.by;
  },
  run: (_input: Trade, ctx) => {
    ctx.change({ by: 1 });
  },
});

// The topology this is all about: it compiles, and every refusal below is a
// deviation from it.
export const wellTyped = defineUnbounded({
  factoryId: 'well_typed',
  channels: (c) => {
    c.input<Trade>('trades').into(Ohlc).into(Publish).sink();
  },
});

export const refusals = defineUnbounded({
  factoryId: 'refusals',
  channels: (c) => {
    // A producer wired to a consumer whose declared input it does not satisfy.
    // @ts-expect-error the entry carries Trade; `Publish` consumes Candle
    c.input<Trade>('a').into(Publish);

    // The same defect one hop in: `Ohlc` emits Candle, `Ohlc` consumes Trade.
    // @ts-expect-error a Candle does not satisfy Trade
    c.input<Trade>('b').into(Ohlc).into(Ohlc, 'second');

    // A transform reshapes the edge, and the reshaped type is what is checked.
    c.input<Trade>('c')
      .map((trade) => ({ symbol: trade.symbol, close: trade.price }))
      .into(Publish);

    // @ts-expect-error the transform produced a Candle, not a Trade
    c.input<Trade>('d').map((trade) => ({ symbol: trade.symbol, close: trade.price })).into(Ohlc);

    // A routing key is a string.
    // @ts-expect-error keyBy answers a string, never the raw value
    c.input<Trade>('e').keyBy((trade) => trade.price).into(Ohlc);

    // A sink encoder is applied to the edge's own value.
    // @ts-expect-error the edge carries a string here, not a Trade
    c.input<Trade>('f').into(Ohlc).into(Publish).sink((value: Trade) => Buffer.from(value.symbol));

    // A named codec types the stream it opens.
    c.input('k', { codec: 'text' }).map((line) => line.toUpperCase());
    c.input('l', { codec: 'bytes' }).map((raw) => raw.byteLength);
    c.input<Trade>('m', { codec: 'json' }).into(Ohlc).into(Publish).sink('n', { codec: 'text' });
    c.input<Trade>('o', { codec: 'msgpack' }).into(Ohlc).sink({ codec: 'json' });

    // @ts-expect-error a text entry carries a string, not bytes
    c.input('p', { codec: 'text' }).map((line) => line.byteLength);

    // @ts-expect-error `yaml` is not a channel codec
    c.input('q', { codec: 'yaml' });

    // A wrong overflow policy is a typo the compiler catches.
    // @ts-expect-error `drop_everything` is not an overflow policy
    c.input<Trade>('h').onFull('drop_everything').into(Ohlc);

    // And a change the fold does not accept.
    c.input<Trade>('i').into(Count);

    // A merge's mappings lift every source into the input its tasklet reads.
    c.merge<Trade>()
      .from(c.input<Trade>('r'), (trade) => trade)
      .from(c.input('s', { codec: 'text' }), (line) => ({ symbol: line, price: 0 }))
      .keyBy((trade) => trade.symbol)
      .into(Ohlc)
      .into(Publish);

    // @ts-expect-error a merge of Trade does not feed a tasklet reading Candle
    c.merge<Trade>().from(c.input<Trade>('t'), (trade) => trade).into(Publish);

    // @ts-expect-error the mapping answers a string, not the merged Trade
    c.merge<Trade>().from(c.input('u', { codec: 'text' }), (line) => line);

    // Either spelling of the output type is what the next hop is checked against.
    c.input<Trade>('y').into(OhlcByContext).into(Publish);
    // @ts-expect-error `OhlcByContext` emits Candle, not the Trade `Ohlc` reads
    c.input<Trade>('z').into(OhlcByContext).into(Ohlc);

    // A fan yields the element type, and the router hands on the document.
    c.input('v', { codec: 'text' })
      .flatMap((line) => line.split(','))
      .map((word) => word.length);
    c.routeLatest(c.input<Candle>('w'), c.input<Trade>('x'), (trade) => trade.symbol).map(
      (routed): Candle => routed.document,
    );
  },
});

const BadChange = defineTasklet({
  name: 'bad_change',
  initState: () => ({ total: 0 }),
  applyChange: (state, change: { by: number }) => {
    state.total += change.by;
  },
  run: (_input: Trade, ctx) => {
    // @ts-expect-error the declared change carries `by`, not `amount`
    ctx.change({ amount: 1 });
  },
});

const BadEmit = defineTasklet<Trade, Candle>({
  name: 'bad_emit',
  run: (trade, ctx) => {
    // @ts-expect-error the declared output is Candle, not a string
    ctx.emit(trade.symbol);
  },
});

const NoOutput = defineTasklet({
  name: 'no_output',
  run: (trade: Trade, ctx) => {
    // @ts-expect-error a tasklet that names no output type has nothing to emit
    ctx.emit(trade);
  },
});

export const moreRefusals = defineUnbounded({
  factoryId: 'more_refusals',
  channels: (c) => {
    c.input<Trade>('j').branch([
      (edge) => edge.into(BadChange),
      (edge) => edge.into(BadEmit),
      (edge) => edge.into(NoOutput),
    ]);
  },
});

// The §5 short form: one tasklet's fields and its `run` body in place of a
// `channels` body, at the function whose name is its kind.
export const shortForm = defineUnbounded({
  factoryId: 'short_form',
  sink: 'out',
  codec: 'text',
  run: (line: string, ctx: Ctx<string>) => ctx.emit(line.toUpperCase()),
});

export const statefulShortForm = defineBounded({
  factoryId: 'stateful_short_form',
  initState: () => ({ seen: [] as string[] }),
  applyChange: (state, change: string) => {
    state.seen.push(change);
  },
  run: (line: string, ctx) => {
    ctx.change(line);
    // @ts-expect-error the declared change is a string
    ctx.change(1);
  },
});

// @ts-expect-error a short form carries its body in place of a topology body, never beside one
defineUnbounded({ factoryId: 'both', run: quiet, channels: () => {} });

// @ts-expect-error the kind is the function's own name, never a field
defineBounded({ factoryId: 'kinded', kind: 'bounded', run: quiet });

// @ts-expect-error a short form's tasklet is named after its `factoryId`
defineUnbounded({ factoryId: 'named', name: 'other', run: quiet });

// A bounded short form writes its loop mode, the one class-level setting it carries.
export const loopedShortForm = defineBounded({
  factoryId: 'looped',
  loopMode: 'per_input',
  run: quiet,
});

// @ts-expect-error any other class-level setting is the full form
defineBounded({ factoryId: 'rolled', rollbackMode: 'keep_progress', run: quiet });

// @ts-expect-error `forever` is not a loop mode
defineBounded({ factoryId: 'forever', loopMode: 'forever', run: quiet });

// @ts-expect-error a stream has no loop mode
defineUnbounded({ factoryId: 'streamed', loopMode: 'per_input', run: quiet });

// @ts-expect-error retry is the edge's `retryMax`, never a bounded setting
defineBounded({ factoryId: 'retried', retryAttempts: 0, channels: () => {} });

export const syncDurable = defineBounded({
  factoryId: 'sync_durable',
  syncDurability: true,
  channels: (c) => {
    // @ts-expect-error synchronous durability is the definition's field, never a builder call
    c.syncDurability();
  },
});

// @ts-expect-error a tasklet carries no sink; the short form is an operator
defineTasklet({ name: 'sinking', sink: 'out', run: quiet });

// @ts-expect-error a tasklet's body is `run`
defineTasklet({ name: 'old', handle: quiet });

// @ts-expect-error a retired field is no field of a tasklet definition
defineTasklet({ name: 'retired', dynamicCells: {}, run: quiet });

// A value leaves a tasklet only through its context, so no hook returns one.
const upper = (line: string): string => line.toUpperCase();

// @ts-expect-error `run` returns nothing; a value leaves through `ctx.emit`
defineTasklet({ name: 'returning', run: (trade: Trade) => trade });

// @ts-expect-error an async `run` resolves to nothing either
defineTasklet({ name: 'async_returning', run: async (trade: Trade) => trade });

// @ts-expect-error a `run` handed over by reference is held to the same rule
defineTasklet({ name: 'by_reference', run: upper });

// @ts-expect-error a short form's `run` is a tasklet's
defineUnbounded({ factoryId: 'returning_short', sink: 'out', run: upper });

// @ts-expect-error `onTimer` returns nothing either
defineTasklet({ name: 'returning_timer', run: quiet, onTimer: (firing) => firing.scheduledAt });
