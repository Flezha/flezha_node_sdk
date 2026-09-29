/**
 * Cell identity bytes and the replay-stable identifier derived from them.
 *
 * Both algorithms are fixed by the SDK contract rather than by a library, so
 * every SDK derives the same value from the same cell: the identity is
 * length-prefixed `instance ‖ tasklet ‖ key`, and the identifier is FNV-1a
 * over `identity ‖ change seq ‖ per-invocation ordinal`.
 */

import { flezhaError } from './errors.js';

const FNV128_OFFSET_BASIS = 0x6c62272e07bb014262b821756295c58dn;
const FNV128_PRIME = 0x0000000001000000000000000000013bn;
const MASK128 = (1n << 128n) - 1n;

function fnv1a128(seed: bigint, bytes: Uint8Array): bigint {
  let hash = seed;
  for (const byte of bytes) {
    hash = (hash ^ BigInt(byte)) & MASK128;
    hash = (hash * FNV128_PRIME) & MASK128;
  }
  return hash;
}

/**
 * `[len instance][instance][len tasklet][tasklet][u16 len key][key]`,
 * big-endian lengths. The same layout every SDK writes, so an identifier
 * derived here matches one derived there for the same cell.
 */
export function cellIdentity(instanceId: string, taskletName: string, cellKey: string): Uint8Array {
  const instance = Buffer.from(instanceId, 'utf8');
  const tasklet = Buffer.from(taskletName, 'utf8');
  const key = Buffer.from(cellKey, 'utf8');
  for (const [part, value, bytes, limit] of [
    ['operator id', instanceId, instance, 255],
    ['tasklet name', taskletName, tasklet, 255],
    ['cell key', cellKey, key, 0xffff],
  ] as const) {
    if (bytes.length > limit)
      throw flezhaError('runtime.identity_too_long', { part, value, limit });
  }
  const out = Buffer.alloc(1 + instance.length + 1 + tasklet.length + 2 + key.length);
  let at = 0;
  out.writeUInt8(instance.length, at);
  at += 1;
  instance.copy(out, at);
  at += instance.length;
  out.writeUInt8(tasklet.length, at);
  at += 1;
  tasklet.copy(out, at);
  at += tasklet.length;
  out.writeUInt16BE(key.length, at);
  at += 2;
  key.copy(out, at);
  return out;
}

function u64Bytes(value: bigint): Uint8Array {
  const out = Buffer.alloc(8);
  out.writeBigUInt64BE(BigInt.asUintN(64, value));
  return out;
}

function u32Bytes(value: number): Uint8Array {
  const out = Buffer.alloc(4);
  out.writeUInt32BE(value >>> 0);
  return out;
}

/**
 * Thirty-two lowercase hex characters. Repeated calls in one invocation
 * differ; the n-th call of a replayed invocation equals the n-th call of the
 * original, which is what makes it the replacement for a random UUID.
 */
export function stableId(identity: Uint8Array, changeSeq: bigint, ordinal: number): string {
  let hash = fnv1a128(FNV128_OFFSET_BASIS, identity);
  hash = fnv1a128(hash, u64Bytes(changeSeq));
  hash = fnv1a128(hash, u32Bytes(ordinal));
  return hash.toString(16).padStart(32, '0');
}
