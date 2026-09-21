// Twins of the reference's durable structured workflow fixtures
// (test/apps/rust_runner/src/workflow.rs and workflow_loop.rs) under the same
// factory ids, tasklet names, channel names and output bytes.
//
// Entry payloads and plugin frames are UTF-8 text on the wire, as in the
// reference: every entry names the text codec, and a bare `.sink(name)` sends a
// string as UTF-8 here as it does there, so only a sink that prefixes its value
// declares an encoder.
// An inter-tasklet hop between two Node tasklets carries MessagePack, and
// the core derives that channel's journal codec from the channel's kind, so no
// fixture declares one.

import { defineOperative, defineTasklet, defineWorkflow } from '@flezha/sdk';

// -- the release gate -------------------------------------------------------

const RELEASE_ENV = 'FLEZHA_WORKFLOW_E2E_RELEASE';

/**
 * Hold this activation until a runner boots with the release variable set.
 *
 * The harness kills the first runner and launches its replacement with the
 * variable present, so the gate is a boot-time decision: never settling on the
 * first runner, returning at once on the second. It SUSPENDS rather than holds
 * — every other activation on this event loop, the control pings among them,
 * has to keep running while a fixture waits here.
 */
const blockUntilReleased = () =>
  process.env[RELEASE_ENV] ? Promise.resolve() : new Promise(() => {});

/** Read at handler time: each scenario arms it on the runner it launches. */
const armed = (name) => process.env[name] !== undefined;

// -- the three factories ----------------------------------------------------

/**
 * Insert into a sorted, de-duplicated array.
 *
 * The reference's `SeenState` is a `BTreeSet<String>`, chosen because a set
 * keeps snapshot bytes deterministic (workflow.rs:50). A `Set` has no durable
 * form here — it is refused at definition, since it would decode as a plain
 * object or array — so the twin is the array it would decode to, kept sorted
 * for the same determinism and idempotent for the same re-run.
 */
const insertSorted = (items, value) => {
  const at = items.findIndex((item) => item >= value);
  if (at < 0) items.push(value);
  else if (items[at] !== value) items.splice(at, 0, value);
};

/**
 * The reference's `durable_step!` (workflow.rs:66-108): a `String -> String`
 * step committing one idempotent Change keyed by its own name, so a re-run
 * after a resume folds to the state it already had.
 *
 * `emit` and `compensate` are the two axes that macro parameterises. `change`
 * and `gate` are the two the reference varies by hand-writing the tasklet
 * instead — the value recorded, and whether this step waits for the release
 * gate BEFORE recording it. Carrying all four here is what lets every durable
 * tasklet in this module be one call.
 */
const durableStep = (name, { change, emit, compensate, gate } = {}) =>
  defineTasklet({
    name,
    initialState: () => ({ items: [] }),
    applyChange: (state, recorded) => insertSorted(state.items, recorded.insert),
    run: async (input, ctx) => {
      // A gated step waits BEFORE its change and its emit, so the kill window
      // always finds it as the uncommitted producer while its sibling arm has
      // committed.
      if (gate && gate(input)) await blockUntilReleased();
      ctx.change({ insert: change ? change(input) : name });
      if (emit) emit(input, ctx);
      else ctx.emit(input);
    },
    // A crash-resumed compensation reconstructs completed stages with EMPTY
    // outputs, so a hook that records an undo does so unconditionally.
    ...(compensate ? { compensate } : {}),
  });

/**
 * The reference's `stateless_step!` (workflow.rs:113-140): no state, no
 * change, the whole behaviour is the handler.
 */
const statelessStep = (name, run) => defineTasklet({ name, run });

/** A terminal step that always fails, which is how a saga is made to unwind. */
const failingStep = (name) =>
  statelessStep(name, () => {
    throw new Error(`${name} always fails`);
  });

// -- echo_workflow ----------------------------------------------------------
//
// A `once` workflow of one tasklet (workflow.rs): its single pass runs on the
// first message and emits it back, so the instance completes and the
// controller deregisters it.

const Echo = statelessStep('echo', (input, ctx) => ctx.emit(`echo:${input}`));

export const echoWorkflow = defineWorkflow({
  factoryId: 'echo_workflow',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(Echo).sink('out');
  },
});

// -- commit_block_workflow / sync_durable_workflow --------------------------
//
// Task A commits durable state, task B blocks until the resumed runner sets
// the release variable — so a SIGKILL lands with A's state already on the WAL
// and B never finished. `sync_durable_workflow` is the same chain declaring
// SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
// destroy the state directory and still resume with A committed.

const CommitA = durableStep('commit_a');
const BlockB = statelessStep('block_b', blockUntilReleased);

const commitBlockChain = (c) => {
  c.input('in', { codec: 'text' }).into(CommitA).named('mid').into(BlockB);
};

export const commitBlockWorkflow = defineWorkflow({
  factoryId: 'commit_block_workflow',
  channels: commitBlockChain,
});

export const syncDurableWorkflow = defineWorkflow({
  factoryId: 'sync_durable_workflow',
  syncDurability: true,
  channels: commitBlockChain,
});

// -- optout_commit_block ----------------------------------------------------
//
// The opt-out twin with a live mid-chain signal: task A's output also reaches
// a plugin sink, so the crash-resume drill can observe the mid-run point
// without durable state. A crash re-runs the chain from the top, so the sink
// sees A's marker once per run and no journal file exists.

export const optoutCommitBlock = defineWorkflow({
  factoryId: 'optout_commit_block',
  persistent: false,
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(CommitA)
      .sink('mid', (value) => Buffer.from(`a-ran:${value}`, 'utf8'))
      .into(BlockB);
  },
});

// -- tier_commit_block_workflow ---------------------------------------------
//
// The effectively-once fixture. Task A commits durable state AND emits
// `committed:<input>` under a stable key on a keyed hybrid channel, captured
// into the atomic effect record; task B blocks. A SIGKILL lands with A's
// effect record durable and B unfinished, and the resume re-runs A — the
// change is idempotent and the re-emit deterministic.

const TierCommitA = durableStep('tier_commit_a', {
  emit: (input, ctx) => ctx.emit(`committed:${input}`, { key: 'job' }),
});

export const tierCommitBlockWorkflow = defineWorkflow({
  factoryId: 'tier_commit_block_workflow',
  channels: (c) => {
    c.effectivelyOnce();
    c.input('in', { codec: 'text' }).into(TierCommitA).keyed().sink('mid').into(BlockB);
  },
});

// -- saga_block_workflow ----------------------------------------------------
//
// SA and SB commit durable state and compensate; SC fails, triggering
// reverse-order rollback (SB then SA). SA's compensation BLOCKS, so a SIGKILL
// lands mid-rollback with SB's done-marker already durable — the respawn
// finishes SA and SKIPS the already-done SB.

const SagaA = durableStep('saga_a', { compensate: blockUntilReleased });
const SagaB = durableStep('saga_b', { compensate: () => {} });
const SagaC = failingStep('saga_c');

export const sagaBlockWorkflow = defineWorkflow({
  factoryId: 'saga_block_workflow',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(SagaA).named('s1').into(SagaB).named('s2').into(SagaC);
  },
});

// -- outbox_block_workflow --------------------------------------------------
//
// The transactional outbox. Task A commits durable state AND emits
// plugin-bound output on a hybrid channel — STAGED, so it reaches the sink
// only after the controller has journaled and fsynced A's committed entry with
// the output inside it and flushed the outbox. Task B blocks, so a SIGKILL
// lands with A committed and flushed and B unfinished.

export const outboxBlockWorkflow = defineWorkflow({
  factoryId: 'outbox_block_workflow',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(CommitA)
      .sink('mid', (value) => Buffer.from(`a-out:${value}`, 'utf8'))
      .into(BlockB);
  },
});

// -- undo_outbox_workflow ---------------------------------------------------
//
// The compensation half of the transactional outbox. Task UA commits, emits
// its forward output on a hybrid plugin channel, and compensates by EMITTING
// an undo record — plugin-bound, so it is staged and rides a compensated
// journal entry rather than the wire. Task C fails immediately, so the chain
// always rolls back; the hook BLOCKS, so on the first runner the undo is
// emitted-but-never-journaled when the SIGKILL lands, proving staged undo
// output cannot leak. The respawn re-runs the outstanding hook, whose done
// marker never landed, and the undo is delivered exactly once.

const UndoOutboxA = durableStep('undo_outbox_a', {
  compensate: async (_outputs, ctx) => {
    await blockUntilReleased();
    ctx.emit('undone');
  },
});

const UndoC = failingStep('undo_outbox_c');

export const undoOutboxWorkflow = defineWorkflow({
  factoryId: 'undo_outbox_workflow',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(UndoOutboxA)
      .sink('mid', (value) => Buffer.from(`out:${value}`, 'utf8'))
      .into(UndoC);
  },
});

// -- branch_workflow --------------------------------------------------------
//
//     BranchA ─┬─▶ BranchB ─▶ BranchD
//              └─▶ BranchC
//
// The tasklet TREE (workflow.rs:352). Every tasklet commits durable state
// and compensates by emitting an undo record, so both tree behaviours are
// observable at one sink:
//
//   * crash-resume — BranchD BLOCKS, so the SIGKILL lands with A, B and C
//     committed and only D's subtree outstanding. The respawn re-runs D alone,
//     seeded from B's journaled edge, so the sibling branch's `c:` marker
//     appears exactly ONCE across both runs.
//   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
//     blocking. A, B and C have all completed, so the rollback unwinds the two
//     branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
//
// The inter-task channels are hybrid, which is what makes a non-leaf
// tasklet's forward and undo output visible at the sink.

const BRANCH_FAIL_ENV = 'FLEZHA_BRANCH_FAIL';

const undoing = (record) => (_outputs, ctx) => ctx.emit(record);

const BranchA = durableStep('branch_a', { compensate: undoing('undo-a') });
const BranchB = durableStep('branch_b', { compensate: undoing('undo-b') });

// The sibling leaf: emits straight to the sink, and its undo does too.
const BranchC = durableStep('branch_c', {
  emit: (input, ctx) => ctx.emit(`c:${input}`),
  compensate: undoing('undo-c'),
});

// The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
const BranchD = statelessStep('branch_d', async (input, ctx) => {
  if (armed(BRANCH_FAIL_ENV)) throw new Error('branch_d failed');
  await blockUntilReleased();
  ctx.emit(`d:${input}`);
});

export const branchWorkflow = defineWorkflow({
  factoryId: 'branch_workflow',
  // Fail fast: the compensation scenario is about the rollback order, not the
  // ladder that precedes it.
  retryAttempts: 0,
  channels: (c) => {
    // One channel, two consumers: THE fan-out. Both branches read exactly what
    // BranchA released on it (held handle, wired twice).
    const mid = c
      .input('in', { codec: 'text' })
      .into(BranchA)
      .sink('mid', (value) => Buffer.from(`a:${value}`, 'utf8'));
    mid
      .into(BranchB)
      .sink('bmid', (value) => Buffer.from(`b:${value}`, 'utf8'))
      .into(BranchD)
      .sink('d_out');
    mid.into(BranchC).sink('c_out');
  },
});

// -- diamond_workflow -------------------------------------------------------
//
//     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
//            └─▶ DiamC ────────────────────┘
//
// The fan-in DAG (workflow.rs:475). The join accumulates BOTH arms' outputs
// and is dispatched exactly once, after both commit. DiamB2 blocks one level
// BELOW its sibling arm — siblings commit at one level barrier — so a SIGKILL
// lands with A, B1 and C committed and B2 mid-run: the respawn re-runs B2
// alone, and the join runs once with C's JOURNALED handoff banked and B2's
// fresh output appended, so `c:` appears exactly once across both runs.

const DiamA = durableStep('diam_a');
const DiamB1 = durableStep('diam_b1');

const DiamB2 = durableStep('diam_b2', {
  change: () => 'diam_b',
  gate: () => true,
  emit: (input, ctx) => ctx.emit(`b:${input}`),
});

const DiamC = durableStep('diam_c', { emit: (input, ctx) => ctx.emit(`c:${input}`) });

// The join: one `j:`-tagged record per accumulated contribution.
const DiamJoin = statelessStep('diam_join', (input, ctx) => ctx.emit(`j:${input}`));

export const diamondWorkflow = defineWorkflow({
  factoryId: 'diamond_workflow',
  channels: (c) => {
    // Held handle wired twice = the fan-out; two arms naming DiamJoin with no
    // node name = the fan-in, since two unnamed mentions are one node.
    const fan = c
      .input('in', { codec: 'text' })
      .into(DiamA)
      .sink('dfan', (value) => Buffer.from(`a:${value}`, 'utf8'));
    fan.into(DiamB1).named('db1').into(DiamB2).sink('db2').into(DiamJoin);
    fan.into(DiamC).sink('dc').into(DiamJoin).sink('j_out');
  },
});

// -- retry_workflow ---------------------------------------------------------
//
// The per-cell retry ladder (workflow.rs:607). Task RA commits durable
// state, forwards on a hybrid channel, and compensates by emitting an undo
// record; `retry_flaky` fails its first FLEZHA_WORKFLOW_RETRY_FAILS attempts
// (none by default — each scenario arms it) and then emits
// `attempt:<n>:<input>`. One armed failure is absorbed by the ladder, so the
// run completes and the sink sees attempt 2; arming more failures than the
// ladder has attempts exhausts it, RA rolls back and the run reports an error.

const RETRY_FAILS_ENV = 'FLEZHA_WORKFLOW_RETRY_FAILS';

// Attempts made in this process. A retry re-runs the tasklet of the same
// instance in place and dispatch is cell-affine, so a module-level counter
// is the whole ladder's view even under a worker pool.
let retryAttempts = 0;

const armedFailures = () => Number.parseInt(process.env[RETRY_FAILS_ENV] ?? '0', 10) || 0;

const RetryA = durableStep('retry_a', { compensate: undoing('undone') });

const RetryFlaky = statelessStep('retry_flaky', (input, ctx) => {
  retryAttempts += 1;
  if (retryAttempts <= armedFailures()) throw new Error(`flaky attempt ${retryAttempts}`);
  ctx.emit(`attempt:${retryAttempts}:${input}`);
});

export const retryWorkflow = defineWorkflow({
  factoryId: 'retry_workflow',
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(RetryA)
      .sink('mid', (value) => Buffer.from(`res:${value}`, 'utf8'))
      .into(RetryFlaky)
      .sink('out');
  },
});

// -- the durable loop family ------------------------------------------------
//
// chat_loop_workflow is the durable-loop + suspension shape
// (workflow_loop.rs:8). One iteration per inbox item: TURN commits the turn to
// Cell state, so a resumed iteration is visible in the WAL, and REPLY emits
// the answer to the sink. Between turns the loop finds an empty inbox at a
// committed boundary and PARKS — the instance is evicted and rehydrated by the
// next message. The turn whose payload is the crash turn BLOCKS while the
// release variable is unset, so a SIGKILL lands with the earlier iterations
// durable and that one in flight. Keying the block on the payload rather than
// on an invocation count keeps it deterministic across the park/rehydrate
// cycles, which re-run the driver but not the turns. The turn `bye` closes the
// session: TURN emits it like any other turn but does not record it, and ends
// the instance with the number of turns its state holds once that iteration
// commits.
//
// stateless_loop_workflow is the same loop MINUS its stateful first
// tasklet: ChatReply alone holds no per-Cell state, so nothing writes an
// Cell WAL and the instance's task-journal reaches the driver only if it
// rides the bind itself.
//
// chat_rewind_workflow is the loop whose replying tasklet can be undone. The
// undo handle lives in Cell state, because a rewind's compensation is
// rebuilt from the journal and is handed NO retained outputs, so the hook
// reads the last thing it said, retracts it and pops it.

const CRASH_TURN = 'mid-crash';
const CLOSING_TURN = 'bye';

const recordTurn = durableStep('chat_turn', {
  // Keyed by the turn, so the record count is the number of DISTINCT turns
  // that ran — a re-run of a committed iteration would show up.
  change: (input) => input,
  gate: (input) => input === CRASH_TURN,
}).definition;

const ChatTurn = defineTasklet({
  ...recordTurn,
  run: (input, ctx) => {
    if (input !== CLOSING_TURN) return recordTurn.run(input, ctx);
    ctx.emit(input);
    ctx.complete(String(ctx.state.items.length));
  },
});

const ChatReply = statelessStep('chat_reply', (input, ctx) => ctx.emit(`reply:${input}`));

const ChatReplyUndoable = defineTasklet({
  name: 'chat_reply_undoable',
  initialState: () => ({ said: [] }),
  applyChange: (state, change) => {
    if (change.said === undefined) state.said.pop();
    else state.said.push(change.said);
  },
  run: (input, ctx) => {
    ctx.change({ said: input });
    ctx.emit(`reply:${input}`);
  },
  compensate: (_outputs, ctx) => {
    const turn = ctx.state.said.at(-1);
    if (turn === undefined) return;
    ctx.change({ retracted: true });
    ctx.emit(`retract:${turn}`);
  },
});

// The duration backstop keeps a stray instance, one that neither `bye` nor a
// stop ended, from outliving the test run.
const loopKnobs = { loopMode: 'per_input', maxDurationMs: 600_000 };

export const chatLoopWorkflow = defineWorkflow({
  factoryId: 'chat_loop_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(ChatTurn).named('mid').into(ChatReply).sink('out');
  },
});

export const statelessLoopWorkflow = defineWorkflow({
  factoryId: 'stateless_loop_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(ChatReply).sink('out');
  },
});

// The chat loop with the block moved AFTER the root's commit: TURN commits its
// turn, REPLY blocks on the turn `reply-crash` while the release env is unset,
// so a SIGKILL lands between a root's `task_committed` and the pass's
// `iter_committed` (workflow_loop.rs).
const REPLY_CRASH_TURN = 'reply-crash';

const BlockingReply = statelessStep('blocking_reply', async (input, ctx) => {
  if (input === REPLY_CRASH_TURN) await blockUntilReleased();
  ctx.emit(`reply:${input}`);
});

export const replyBlockLoopWorkflow = defineWorkflow({
  factoryId: 'reply_block_loop_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(ChatTurn).named('mid').into(BlockingReply).sink('out');
  },
});

// -- spawn_requester --------------------------------------------------------
//
// On each input it spawns `stateless_loop_workflow` once with no id and once
// under the input as its id, and prints both results, because a handle's answer
// is observable nowhere but in the tasklet that read it (workflow_loop.rs).

// A spawn handle's answer on stdout, as `spawn_result <label> <id> <outcome>`.
async function reportSpawn(label, handle) {
  try {
    const { id, outcome } = await handle.wait();
    console.log(`spawn_result ${label} ${id} ${outcome}`);
  } catch (error) {
    console.log(`spawn_result ${label} error ${error.message}`);
  }
}

const SpawnRequester = defineTasklet({
  name: 'spawn_requester',
  run: async (input, ctx) => {
    const factory = statelessLoopWorkflow.factoryId;
    const handles = [
      ['unnamed', ctx.spawnWorkflow(factory, input)],
      ['named', ctx.spawnWorkflow(factory, input, { id: input })],
    ];
    for (const [label, handle] of handles) await reportSpawn(label, handle);
  },
});

export const spawnRequester = defineOperative({
  factoryId: 'spawn_requester',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(SpawnRequester).sink('out');
  },
});

// -- spawn_once_requester / typed_once_workflow -------------------------------
//
// The KB-381 shape: a stream tasklet spawns a `once` workflow whose entry
// channel decodes the payload's type. The spawned instance prints what its
// entry decoded and completes with it, because the runner's stdout and the
// completion record are the two places the scenario can read.

const OncePlaced = statelessStep('once_placed', (ticket, ctx) => {
  console.log(`once_ran ${ticket.text}`);
  ctx.emit(`placed:${ticket.text}`);
  ctx.complete(ticket.text);
});

export const typedOnceWorkflow = defineWorkflow({
  factoryId: 'typed_once_workflow',
  channels: (c) => {
    c.input('order', { codec: 'json' }).into(OncePlaced).sink('placed');
  },
});

const SpawnOnceRequester = defineTasklet({
  name: 'spawn_once_requester',
  run: async (input, ctx) =>
    reportSpawn('once', ctx.spawnWorkflow(typedOnceWorkflow.factoryId, { text: input })),
});

export const spawnOnceRequester = defineOperative({
  factoryId: 'spawn_once_requester',
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(SpawnOnceRequester).sink('out');
  },
});

export const chatRewindWorkflow = defineWorkflow({
  factoryId: 'chat_rewind_workflow',
  ...loopKnobs,
  channels: (c) => {
    c.input('in', { codec: 'text' })
      .into(ChatTurn)
      .named('mid')
      .into(ChatReplyUndoable)
      .sink('out');
  },
});

// -- the bench chains -------------------------------------------------------
//
// What `mix bench.workflow --scenario chain` drives (workflow_loop.rs:232): K
// durable pass-through steps in a line, the payload unchanged, so the sink
// receives the seed input and the bench reads its own seq and send timestamp
// back. Each chain position needs its own tasklet, so the sixteen are
// generated rather than written out; the edge names follow the reference,
// which spells them from the step TYPE rather than from the tasklet name.

const benchSteps = Array.from({ length: 16 }, (_, at) => durableStep(`bench_s${at}`));

const benchChain = (factoryId, steps, persistent) =>
  defineWorkflow({
    factoryId,
    persistent,
    channels: (c) => {
      let stream = c.input('in', { codec: 'text' }).into(benchSteps[0]);
      for (let at = 1; at < steps; at += 1) {
        stream = stream.named(`edge_BenchS${at}`).into(benchSteps[at]);
      }
      stream.sink('out');
    },
  });

export const benchChain1 = benchChain('bench_chain_1', 1, true);
export const benchChain4 = benchChain('bench_chain_4', 4, true);
export const benchChain16 = benchChain('bench_chain_16', 16, true);
export const benchChainOptout4 = benchChain('bench_chain_optout_4', 4, false);

// -- bench_loop_workflow ----------------------------------------------------
//
// The long-lived loop the `turns` scenario drives (workflow_loop.rs:359): the
// chat loop's shape with a BOUNDED per-turn state, so a million-turn soak
// snapshots the same few bytes at every iteration, and a wake deadline for the
// timer-wake measurements.

const WAKE_AFTER_ENV = 'FLEZHA_BENCH_WAKE_AFTER_MS';

const BenchTurn = defineTasklet({
  name: 'bench_turn',
  initialState: () => ({ turns: 0, last: '' }),
  applyChange: (state, seen) => {
    state.turns += 1;
    state.last = seen.turn;
  },
  run: (input, ctx) => {
    ctx.change({ turn: input });
    ctx.emit(input);
  },
});

export const benchLoopWorkflow = defineWorkflow({
  factoryId: 'bench_loop_workflow',
  loopMode: 'per_input',
  // A day, not the chat loop's ten minutes: a soak outlives 600 s.
  maxDurationMs: 24 * 3_600_000,
  wakeAfterMs: Number.parseInt(process.env[WAKE_AFTER_ENV] ?? '', 10) || undefined,
  channels: (c) => {
    c.input('in', { codec: 'text' }).into(BenchTurn).named('mid').into(ChatReply).sink('out');
  },
});
