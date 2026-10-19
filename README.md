# @flezha/sdk

A poll host over `flz_core`, the runner kernel the Rust and Java SDKs also
drive. The kernel owns transport, ordering, per-cell exclusivity, durability,
retries, timers, credit, routing and steps runs. This package supplies
threads and user code, and nothing else.

**Support tier:**
[tier 2](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-node),
passes the full conformance run in CI exactly as tier 1 does, with every capability
present, and is permanently supported; it is not yet run by the maintainer in a live
deployment, so a defect specific to it is likely to be found by its users first.

## Install

```bash
npm install @flezha/sdk
```

`@flezha/sdk` 0.1.0 is not on npm yet. Until it is, build it from a checkout as
[`examples/echo`](examples/echo/README.md#building-from-a-checkout) shows.

TypeScript-first, usable from plain JavaScript with no build step, and importable
from both module systems: `import` from an ECMAScript module and `require` from
CommonJS resolve to the same surface.

## Supported Node versions

Node **20 or newer**. The binding targets N-API version 9, which every release in
that window speaks. N-API is ABI-stable across Node versions, so one binary per
platform serves every supported release and upgrading Node needs no rebuild.

## The native library

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

The five come from one table, `src/platforms.ts`, which the loader also
resolves against. On any other platform, or where npm resolved no platform
package, the SDK refuses at the first load naming the host it detected, the
package it looked for and how to build the binary. It has no bundled binary to
fall back on. A source build needs a stable Rust toolchain and this package,
not a checkout of the core: `native/` is a wrapper crate whose one dependency is
the core at a git tag.

```bash
cargo build --manifest-path native/Cargo.toml --release
node scripts/install-native.mjs
```

`npm run build:native` runs both. `FLEZHA_NATIVE_BINARY` points the loader at an
already-built addon from anywhere, and is consulted before the platform package.

One caution about installs: npm treats an optional dependency it cannot resolve
as a non-event, so an install that fetched no binary succeeds and prints
nothing. That is why the refusal at load is explicit about what is missing.

## Writing a relay

```ts
import { createRunner, defineRelay, defineTasklet, type Ctx } from '@flezha/sdk';

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

export default defineRelay({
  factoryId: 'candles',
  channels: (c) => {
    c.input<Trade>('trades')
      .keyBy((trade) => trade.symbol)
      .into(Ohlc)
      .keyed()
      .output('candles');
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

## Class settings

Every relay is declared with `defineRelay`, in the full form above or as
the short form, one tasklet's fields and its `run` body with an `output`. Both
forms take the class settings:

```ts
export default defineRelay({
  factoryId: 'orders',
  steps: { each: 'per_input', onFailure: 'keep_progress' },
  durable: { local: { sleepAfter: 10_000 } },
  maxDurationMs: 600_000,
  channels: (c) => c.input('order', { codec: 'text' }).into(Place).output('placed'),
});
```

A relay runs as a flow unless it writes `steps`; `flow` and `steps` are
written as the branch itself, since a short form's `run` is its body. A flow
may take `exactly: 'effectively_once'`, or
`{ effectively_once: { ack, correctionMs, beyondWindow } }` with `ack` either
`'per_input'` or `{ checkpoint: { every, maxDelayMs, outputs } }`. `durable` is
`false`, `'local'` (the default) or `'remote'`; unless `sleepAfter` names an
interval or `false`, a kept flow instance never sleeps and a kept steps instance
sleeps after six idle hours. The core
reads the tree when the class registers: a setting under a parent that does not
carry it is refused as `topology.unknown_field`, naming both.

## Local mode, testing and the relay API

- **Local mode**: `LocalRelay.start` runs a relay's declared graph in your own
  process, with no controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: `TaskletTestHarness`, from `@flezha/sdk/testing`, drives one
  tasklet and checks its replay. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own).
- **The relay API**: a relay's entry channels, answering a caller, `call(...)`
  to another relay, and `runner.send` and `runner.call` from application code.
  See [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

## Licence

Apache-2.0. See the repository for the full text and third-party notices.
