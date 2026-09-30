# @flezha/sdk

A poll host over `flz_core`, the runner kernel the Rust and Java SDKs also
drive. The kernel owns transport, ordering, per-cell exclusivity, durability,
retries, timers, credit, routing and bounded runs. This package supplies
threads and user code, and nothing else.

**Support tier:**
[tier 2](https://github.com/flezha/flezha/blob/main/docs/publishing/maturity.md#sdk-node),
passes the full conformance run in CI exactly as tier 1 does, with every capability
present, and is permanently supported; it is not yet run by the maintainer in a live
deployment, so a defect specific to it is likely to be found by its users first.

```bash
npm install @flezha/sdk
```

TypeScript-first, usable from plain JavaScript with no build step, and importable
from both module systems: `import` from an ECMAScript module and `require` from
CommonJS resolve to the same surface.

## Supported Node versions

Node **20 or newer**. The binding targets N-API version 9, which every release in
that window speaks. N-API is ABI-stable across Node versions, so one binary per
platform serves every supported release and upgrading Node needs no rebuild.

## The native binary

The binary is not in this package. It ships as a per-platform package resolved
through `optionalDependencies`, so installing fetches one binary rather than
five and needs no Rust toolchain:

| Platform            | Package                       |
| ------------------- | ----------------------------- |
| Linux x64 (glibc)   | `@flezha/sdk-linux-x64-gnu`   |
| Linux arm64 (glibc) | `@flezha/sdk-linux-arm64-gnu` |
| macOS x64           | `@flezha/sdk-darwin-x64`      |
| macOS arm64         | `@flezha/sdk-darwin-arm64`    |
| Windows x64         | `@flezha/sdk-win32-x64-msvc`  |

On any other platform, or where npm resolved no platform package, the SDK
refuses at the first load naming the host it detected, the package it looked for
and how to build the binary. It does not fall through to a loader error, and it
has no bundled binary to fall back on: a source build is a checkout of the
repository with a stable Rust toolchain.

```bash
cargo build -p flz_core_ffi --features napi --release
node sdk/node/scripts/install-native.mjs
```

`FLEZHA_NATIVE_BINARY` points the loader at an already-built addon from
anywhere, and is consulted before the platform package.

One caution about installs: npm treats an optional dependency it cannot resolve
as a non-event, so an install that fetched no binary succeeds and prints
nothing. That is why the refusal at load is explicit about what is missing.

## Writing an operator

```ts
import { createRunner, defineUnbounded, defineTasklet, type Ctx } from '@flezha/sdk';

interface Trade {
  symbol: string;
  price: number;
}

interface Candle {
  symbol: string;
  close: number;
}

interface Close {
  close: number;
}

const Ohlc = defineTasklet({
  name: 'ohlc',
  initState: () => ({ close: 0 }),
  applyChange: (state, change: Close) => {
    state.close = change.close;
  },
  run: (trade: Trade, ctx: Ctx<Candle, Close, Close>) => {
    ctx.change({ close: trade.price });
    ctx.emit({ symbol: trade.symbol, close: ctx.state.close });
  },
});

export default defineUnbounded({
  factoryId: 'candles',
  channels: (c) => {
    c.input<Trade>('trades')
      .keyBy((trade) => trade.symbol)
      .into(Ohlc)
      .keyed()
      .sink('candles');
  },
});
```

```ts
const runner = createRunner('default');
await runner.registerFactory('./candles.js');
await runner.run();
```

A value leaves a tasklet only through `ctx.emit`, and what `run` returns is
ignored; `ctx.part(value)` sends a listener a preview of that output while `run`
is still working. The output type is the annotation on `ctx`, as here, or the type
arguments, `defineTasklet<Trade, Candle>(...)`. `keyBy` gives each symbol a cell
of its own. State is mutated only through declared changes: declared state is
rebuilt from its recorded changes on restart, so assigning to a field of
`ctx.state` throws rather than being silently lost. A mismatched `.into(...)` is
a compile error.

## Answering and calling

A channel ends in one `sink`, one `response()` or one `call(...)`. `response()`
answers the outside caller whose input caused the value. `call(target, { deadlineMs, codec? })`
sends each value, after the chain's transforms and key, to another operator:
a string names an instance, and `{ factory, create?, channel? }` an instance of a
factory, the one the chain's `keyBy` names (`create: false` refuses to start it,
`channel` picks its entry channel). One codec writes the request and reads the
answer, JSON when none is named.

The call waits for nothing. Its outcome arrives later as a `Response` on a new
entry channel, `<channel>.response`, which must feed a tasklet, the calling one
included. A `Response` carries `request`, `ok`, then `value` or `error` (a
`FlezhaError`; `call.timeout` once the deadline passes), `target` (the callee's
instance id) and `created` (whether this call started it).

```ts
const gateway = c.input<string>('in', { codec: 'text' }).into(Account).named('gateway');
gateway
  .keyBy((line) => line.split(':')[0])
  .call({ factory: 'agent' }, { deadlineMs: 30_000, codec: 'text' })
  .into(Outcome); // Outcome's input is Response<string, string>
```

## Licence

Apache-2.0. See the repository for the full text and third-party notices.
