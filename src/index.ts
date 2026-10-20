/**
 * `@flezha/sdk` — the Flezha SDK for Node.
 *
 * The runner kernel is `flz_core`, reached through its C ABI: it owns
 * transport, ordering, per-cell exclusivity, durability, retries, timers,
 * credit, routing and the steps driver. This package supplies threads and
 * user code, and nothing else.
 */

export { Control } from './control.js';
export type {
  BindOptions,
  PluginOptions,
  StartOptions,
  StartOutcome,
  StartResult,
} from './control.js';
export type { SendOptions, SendVerdict } from './runner.js';
export { defineRelay } from './authoring.js';
export { defineTasklet } from './definition.js';
export type { CallOptions, CallTarget, Response } from './call.js';
export type { ChannelSpec, ClassSpec, Link, TaskNode } from './authoring.js';
export type { SpawnHandle } from './ctx.js';
export { assertRepresentable, decodeValue, encodeValue, msgpack } from './codec.js';
export type { Codec } from './codec.js';
export type { DerivedChannel, DerivedDecl, DerivedEdge } from './graph.js';
export { LocalRelay } from './local.js';
export type { LocalOutput } from './local.js';
export { consoleLogger, setLogger } from './logger.js';
export type { Logger, LogLevel } from './logger.js';
export type { Native } from './native.js';
export { ACTIVATION_SCHEMA_VERSION, createRunner, Runner } from './runner.js';
export { FlezhaError } from './errors.js';
export type { ErrorClass, ErrorFields } from './errors.js';
export type {
  Ack,
  BeyondWindowPolicy,
  ChannelBuilder,
  ClassSettings,
  ChannelCodec,
  CodecOptions,
  EntryOptions,
  Ctx,
  Durable,
  Each,
  EffectOptions,
  EmitOptions,
  Exactly,
  FlowSettings,
  IfRunning,
  Merge,
  OnFailure,
  RelayDefinition,
  RelaySettings,
  OverflowPolicy,
  Tasklet,
  TaskletDefinition,
  RoutedDocument,
  RunSettings,
  ShortFormDefinition,
  SleepAfter,
  SpawnOptions,
  SpawnOutcome,
  SpawnResult,
  StepsSettings,
  Stream,
  TimerFiring,
  TimerKind,
} from './types.js';
