import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { locateContract } from './contract.mjs';

function carryManifest(tree) {
  mkdirSync(join(tree, 'test/fixtures'), { recursive: true });
  writeFileSync(join(tree, 'test/fixtures/contract.txt'), '');
}

test('an in-tree contract/ copy does not shadow the checkout the package sits in', (t) => {
  const checkout = mkdtempSync(join(tmpdir(), 'flezha-contract-'));
  t.after(() => rmSync(checkout, { recursive: true, force: true }));
  const pkg = join(checkout, 'sdk/node');
  carryManifest(checkout);
  carryManifest(join(pkg, 'contract'));

  assert.equal(locateContract(pkg, undefined), checkout);

  rmSync(join(checkout, 'test'), { recursive: true });
  assert.equal(locateContract(pkg, undefined), join(pkg, 'contract'));
});
