#!/usr/bin/env node
// Fetch the three things this package needs from one core release: the native
// addon, the fake controller, and the contract set.
//
// This package is proven against a core it does not contain. A release of the
// core publishes every one of those as an asset with its SHA-256 in one
// manifest, so obtaining them is a download and a checksum rather than a
// checkout and a Rust toolchain. That is the whole reason the SDK can live
// here: its unit suite, its local-mode suite and its smoke tier all run
// against binaries someone else built, at a version this package names.
//
//   node scripts/fetch-core.mjs --from <dir|url> [--version 0.1.0]
//
// `--from` is a staged release directory (what `scripts/build-dist.sh
// --stage-release` writes in the core's tree) or the base URL a GitHub release
// serves its assets from. Everything it installs is build output and is
// ignored by git.

import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const valueOf = (name, fallback = null) => {
  const at = args.indexOf(name);
  return at === -1 ? fallback : args[at + 1];
};

const from = valueOf('--from');
const version = valueOf('--version', '0.1.0');
const scratch = resolve(root, '.core/download');

const refuse = (message) => {
  console.error(`fetch-core: ${message}`);
  process.exit(1);
};

if (!from) refuse('--from <dir|url> is required');

// The platform a core release names its per-platform assets by. It mirrors the
// directory column of the core's `scripts/native_triples.sh`; this package is
// outside that tree and cannot read it, so the asset name is the contract.
const CORE_PLATFORMS = {
  'linux-x64': 'linux-x64',
  'linux-arm64': 'linux-arm64',
  'darwin-x64': 'macos-x64',
  'darwin-arm64': 'macos-arm64',
  'win32-x64': 'windows-x64',
};

const host = `${process.platform}-${process.arch}`;
const corePlatform = CORE_PLATFORMS[host];
if (!corePlatform) refuse(`no core release asset is published for ${host}`);

const { NATIVE_PLATFORMS } = await import(pathToFileURL(resolve(root, 'dist/esm/platforms.js')))
  .catch(() => refuse('dist/esm/platforms.js does not exist; build the package first: npm run build'));
const platform = NATIVE_PLATFORMS.find(
  (entry) => entry.os === process.platform && entry.cpu === process.arch,
);
if (!platform) refuse(`this package publishes no binary for ${host}`);

const isUrl = /^https?:\/\//.test(from);

/** One asset's bytes, from a directory or a release URL. */
async function asset(name) {
  if (!isUrl) {
    const path = resolve(from, name);
    if (!existsSync(path)) refuse(`${path} is not there`);
    return readFileSync(path);
  }
  const response = await fetch(`${from.replace(/\/$/, '')}/${name}`);
  if (!response.ok) refuse(`${name}: ${response.status} ${response.statusText}`);
  return Buffer.from(await response.arrayBuffer());
}

/** The manifest's digest for every asset, so nothing is used unverified. */
const digests = new Map(
  (await asset('SHA256SUMS.txt'))
    .toString('utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => {
      const [digest, name] = line.trim().split(/\s+/);
      return [name, digest];
    }),
);

/** One asset, checked against the manifest and written to `into`. */
async function verified(name, into) {
  const bytes = await asset(name);
  const expected = digests.get(name);
  if (!expected) refuse(`${name} is not in SHA256SUMS.txt; it was not part of this release`);
  const got = createHash('sha256').update(bytes).digest('hex');
  if (got !== expected) refuse(`${name} does not match its manifest digest`);
  mkdirSync(dirname(into), { recursive: true });
  writeFileSync(into, bytes);
  console.log(`  verified ${name}`);
  return into;
}

const tar = (archive, into, ...flags) => {
  mkdirSync(into, { recursive: true });
  execFileSync('tar', ['-xzf', archive, '-C', into, ...flags], { stdio: 'inherit' });
};

rmSync(scratch, { recursive: true, force: true });
mkdirSync(scratch, { recursive: true });
console.log(`fetch-core: taking the ${version} core from ${from}`);

// 1. The addon, out of this platform's package tarball. The tarball is what a
//    registry serves, so what the smoke tier loads is what an installer gets.
const tarball = await verified(
  `flezha-sdk-${platform.suffix}-${version}.tgz`,
  resolve(scratch, 'addon.tgz'),
);
tar(tarball, scratch, '--strip-components=1', 'package/flz_core_ffi.node');
mkdirSync(resolve(root, 'native'), { recursive: true });
copyFileSync(resolve(scratch, 'flz_core_ffi.node'), resolve(root, 'native/flz_core_ffi.node'));
console.log('  -> native/flz_core_ffi.node');

// 2. The fake controller, which the smoke tier runs in place of a controller.
const controller = await verified(
  `flz-fake-controller-${corePlatform}${process.platform === 'win32' ? '.exe' : ''}`,
  resolve(root, '.core/flz-fake-controller'),
);
execFileSync('chmod', ['+x', controller]);
console.log('  -> .core/flz-fake-controller');

// 3. The contract set, which every fixture-reading test resolves against.
const fixtures = await verified(
  `flezha-fixtures-${version}.tar.gz`,
  resolve(scratch, 'fixtures.tar.gz'),
);
rmSync(resolve(root, 'contract'), { recursive: true, force: true });
tar(fixtures, resolve(root, 'contract'));
console.log('  -> contract/');

rmSync(scratch, { recursive: true, force: true });
console.log('fetch-core: done');
