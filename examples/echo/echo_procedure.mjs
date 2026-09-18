import { defineProcedure } from '@flezha/sdk';

/**
 * A processing stage of the operative's graph, run once per message. This one
 * prefixes the input, and what `run` returns is emitted.
 *
 * `name` is the procedure's stable identity. State is filed under it, so
 * renaming it later declares a different procedure rather than renaming this
 * one. With no `initialState` and no `applyChange`, nothing is kept between
 * messages and nothing is journaled.
 *
 * Where the result goes is not decided here. The operative decides that, so the
 * graph can be rewired without touching this procedure.
 */
export const EchoProcedure = defineProcedure({
  name: 'echo',
  run: (input) => `echo: ${input}`,
});
