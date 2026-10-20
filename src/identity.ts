/**
 * The replay-stable identifier of a cell.
 *
 * The algorithm is fixed by the SDK contract rather than by a library, so
 * every SDK derives the same value from the same cell: FNV-1a over
 * `identity ‖ change seq ‖ per-invocation ordinal`, where the identity is the
 * bytes the core hands with the cell's first activation.
 */

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
