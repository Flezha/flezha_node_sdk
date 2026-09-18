/**
 * `@flezha/sdk` — the Flezha SDK for Node.
 *
 * The runner kernel is `flz_core`, reached through its C ABI: it owns
 * transport, ordering, per-entity exclusivity, durability, retries, timers,
 * credit, routing and the workflow driver. This package supplies threads and
 * user code, and nothing else.
 */

export { Admin } from './admin.js';
export type { StartOptions, StartOutcome, StartResult } from './admin.js';
export { defineOperative, defineProcedure, defineWorkflow } from './authoring.js';
export type { ChannelSpec, ClassSpec, Link, ProcNode } from './authoring.js';
export type { SpawnHandle } from './ctx.js';
export {
  assertRepresentable,
  decodeValue,
  encodeValue,
  msgpack,
  UnrepresentableValueError,
} from './codec.js';
export type { Codec } from './codec.js';
// The runner's declared-class registry. Exported because it owns "a runner
// declares each factory id once", and the conformance run drives that rule
// rather than restating it.
export { Registry } from './dispatch.js';
export { FORMAT_ID_MSGPACK, SDK_LANG_NODE, toHostClassDecl, validate } from './graph.js';
export type { DerivedChannel, DerivedDecl, DerivedEdge } from './graph.js';
export { LocalOperative } from './local.js';
export type { SinkOutput } from './local.js';
export { consoleLogger, setLogger } from './logger.js';
export type { Logger, LogLevel } from './logger.js';
export { loadNative, nativePackageName } from './native.js';
export type { Native } from './native.js';
export { ACTIVATION_SCHEMA_VERSION, createRunner, Runner } from './runner.js';
export { StateMutationError } from './state.js';
export { DELAY_WARN_MS } from './loop-watch.js';
export {
  DeclarationError,
  SpawnAlreadyRunningError,
  SpawnError,
  SpawnFactoryUnknownError,
  SpawnRefusedError,
  SpawnTimeoutError,
  SpawnUnavailableError,
} from './types.js';
export type {
  BeyondWindowPolicy,
  ChannelBuilder,
  ChannelCodec,
  CodecOptions,
  ClockKind,
  Ctx,
  DynamicEntities,
  EmitOptions,
  IfRunning,
  LoopMode,
  Merge,
  OperativeDefinition,
  OverflowPolicy,
  Procedure,
  ProcedureDefinition,
  RollbackMode,
  RoutedDocument,
  ShortFormDefinition,
  SpawnOptions,
  SpawnOutcome,
  SpawnResult,
  StaticEntities,
  Stream,
  TimerFiring,
  TimerKind,
  WorkflowDefinition,
} from './types.js';
