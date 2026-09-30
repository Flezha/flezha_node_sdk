/**
 * A graph's call to another operator: where it goes, and the outcome that
 * comes back on the caller's Response channel.
 */

import { channelDecoder, channelEncoder } from './codec.js';
import { FlezhaError, type ErrorFields } from './errors.js';
import type { ChannelCodec } from './types.js';

/**
 * The operator a `call` reaches: an instance (or a partition group, whose
 * member the request's key chooses) by its id, or an instance of a factory —
 * after a `keyBy`, the one whose id is the request's key, started with the
 * request unless `create` is false; without one, a new bounded instance per
 * call. `channel` names the callee's entry channel; its first when absent.
 */
export type CallTarget =
  | string
  | { target: string; channel?: string }
  | { factory: string; create?: boolean; channel?: string };

export interface CallOptions {
  /** How long the caller waits at most, in milliseconds; `call.timeout` after it. */
  deadlineMs: number;
  /**
   * One codec for both directions: the request is written with it as `sink`
   * writes, the answer read with it as `input` reads. JSON when absent.
   */
  codec?: ChannelCodec;
}

/**
 * The outcome of one call: the request as the call end wrote it, decoded
 * back, then either the callee's answer or the error that ended the call
 * (`call.timeout` among them), the callee's instance id and whether this call
 * started it.
 */
export type Response<Req, Ans> = {
  request: Req;
  target: string;
  created: boolean;
} & ({ ok: true; value: Ans } | { ok: false; error: FlezhaError });

/** The `call` link the core reads, closing the chain it ends. */
export interface CallLink {
  link: 'call';
  target?: string;
  factory?: string;
  deadline_ms: number;
  create: boolean;
  channel?: string;
  response: string;
}

/** The link a `call` records, its Response channel named `response`. */
export function callLink(target: CallTarget, deadlineMs: number, response: string): CallLink {
  const to = typeof target === 'string' ? { target } : target;
  return {
    link: 'call',
    ...('target' in to ? { target: to.target } : { factory: to.factory }),
    deadline_ms: deadlineMs,
    create: 'create' in to ? to.create !== false : true,
    ...(to.channel === undefined ? {} : { channel: to.channel }),
    response,
  };
}

/** The envelope the controller writes on a Response channel for each outcome. */
interface Envelope {
  target: string;
  created: boolean;
  request: string;
  value?: string;
  error?: ErrorFields;
}

/** The request encoder and the Response channel's decoder for one call's codec. */
export function callCodec(
  codec: ChannelCodec | undefined,
  channel: { name: string },
): [(value: unknown) => Uint8Array, (bytes: Uint8Array) => Response<unknown, unknown>] {
  const decode = channelDecoder(codec ?? 'json', channel);
  const decodeResponse = (bytes: Uint8Array): Response<unknown, unknown> => {
    const envelope = JSON.parse(Buffer.from(bytes).toString('utf8')) as Envelope;
    const head = {
      request: decode(Buffer.from(envelope.request, 'base64')),
      target: envelope.target,
      created: envelope.created === true,
    };
    if (envelope.error !== undefined) {
      const error = FlezhaError.fromDocument(JSON.stringify({ error: envelope.error }));
      if (error === null) {
        throw new TypeError('Response channel: the envelope carries no error document');
      }
      return { ...head, ok: false, error };
    }
    if (typeof envelope.value !== 'string') {
      throw new TypeError('Response channel: the envelope carries neither a value nor an error');
    }
    return { ...head, ok: true, value: decode(Buffer.from(envelope.value, 'base64')) };
  };
  return [channelEncoder(codec ?? 'json', channel), decodeResponse];
}
