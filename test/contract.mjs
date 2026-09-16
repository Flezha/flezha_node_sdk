// The contract set this package is proven against.
//
// `test/fixtures/contract.txt` names every member (the declaration keys, the
// knob parse rule, the reference topologies, the channel codecs, the C ABI
// surface), and the release publishes them as one archive, so a package
// developed outside the core's repository vendors them at `contract/` instead
// of walking to a tree it has not got.
//
// This resolves the directory those repository-relative paths hang off, once,
// in the order every SDK uses: `$FLEZHA_CONTRACT_DIR`, else `contract/` at the
// package root, else the nearest enclosing tree that carries the manifest.
// Walking up rather than counting `..` segments is what lets the same
// resolution work at both layouts this package lives in: under `sdk/node` here
// and at the root of its own repository.
//
// Resolving nowhere throws, naming the three. It never skips.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** The member every candidate directory is recognised by. */
const MANIFEST = 'test/fixtures/contract.txt';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

/**
 * The nearest directory at or above `from` that carries `marker`.
 *
 * Both of this suite's resolvers ask the same question: the contract set looks
 * for its manifest, the test app for its `conformance.json`. Asking it by
 * walking rather than by counting `..` segments is what lets one answer serve
 * both layouts this package lives in, under `sdk/node` in the core's tree and
 * at the root of its own repository.
 */
export function ascendTo(from, marker) {
  for (let at = from; ; at = dirname(at)) {
    if (existsSync(resolve(at, marker))) return at;
    if (dirname(at) === at) return null;
  }
}

function candidates() {
  return [
    process.env.FLEZHA_CONTRACT_DIR || null,
    resolve(packageRoot, 'contract'),
    ascendTo(packageRoot, MANIFEST),
  ];
}

let cached = null;

/** The directory the contract set's repository-relative paths hang off. */
export function contractRoot() {
  if (cached) return cached;
  const tried = candidates();
  const found = tried.find((at) => at && existsSync(resolve(at, MANIFEST)));
  if (found) return (cached = found);
  const unresolved = [
    '$FLEZHA_CONTRACT_DIR (unset)',
    '',
    `no directory above ${packageRoot} carries ${MANIFEST}`,
  ];
  throw new Error(
    'the contract set is nowhere this suite looked:\n  ' +
      tried.map((at, i) => at ?? unresolved[i]).join('\n  ') +
      `\nUnpack flezha-fixtures-<version>.tar.gz at ${packageRoot}/contract, ` +
      'or set FLEZHA_CONTRACT_DIR.',
  );
}

/** One member of the set, as an absolute path. */
export function member(relative) {
  const path = resolve(contractRoot(), relative);
  if (!existsSync(path)) {
    throw new Error(`${relative} is a member of the contract set and is not in ${contractRoot()}`);
  }
  return path;
}

/** One member of the set, read as UTF-8 text. */
export function read(relative) {
  return readFileSync(member(relative), 'utf8');
}
