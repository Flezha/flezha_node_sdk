/**
 * The latest-document router `routeLatest` expands to.
 *
 * A dynamically keyed procedure cannot receive a broadcast: inside a
 * partitioned member a computed key is admitted against that member's key
 * range, so fanning a document out over a remembered key set would emit keys
 * this member may no longer own. The router delivers on a key's own next
 * traffic item instead, so every key it emits is one that traffic just proved
 * this member owns. The price is one item of latency per key.
 *
 * The expansion is a merge into this single-entity procedure, whose state
 * rides the ordinary change journal: no runtime feature of its own.
 */

import { encodeValue } from './codec.js';
import type { ProcedureDefinition, RoutedDocument } from './types.js';

/** A document off the document stream, or the key one traffic item carried. */
export type RouterInput = { document: unknown } | { key: string };

export interface RouterState {
  document: unknown;
  /** 0 until a document is retained; every replacement advances it. */
  version: number;
  /** The version each key has received. */
  delivered: Record<string, number>;
}

export type RouterChange = { retain: unknown } | { delivered: string; version: number };

/** Equal as the class codec writes them, which is what a restart would compare. */
function sameDocument(left: unknown, right: unknown): boolean {
  return Buffer.compare(encodeValue(left), encodeValue(right)) === 0;
}

export const latestRouter: ProcedureDefinition<
  RouterInput,
  RoutedDocument<unknown>,
  RouterState,
  RouterChange
> = {
  name: 'route_latest',
  initialState: () => ({ document: null, version: 0, delivered: {} }),
  // The key is the tested side of both unions: the codec omits an undefined
  // property, so a document or a retained value can arrive without its field.
  applyChange: (state, change) => {
    if ('delivered' in change) {
      state.delivered[change.delivered] = change.version;
    } else {
      state.document = change.retain;
      state.version += 1;
    }
  },
  run: (input, ctx) => {
    const { document, version, delivered } = ctx.state;
    if (!('key' in input)) {
      if (version === 0 || !sameDocument(document, input.document)) {
        ctx.change({ retain: input.document });
      }
      return;
    }
    if (version === 0 || (delivered[input.key] ?? 0) >= version) return;
    // Emit first: a failed emit rides the channel's retry ladder, and recording
    // the delivery before it would make the retry silent.
    ctx.emit({ key: input.key, document });
    ctx.change({ delivered: input.key, version });
  },
};
