import { mkdirSync, rmdirSync, rmSync, symlinkSync, unlinkSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

// The starter example's test, run against this checkout. The starter names the
// SDK by its package name, as a project of its own does, so a link in its
// node_modules resolves that name to this package while its modules load. The
// link goes once they have: it would make the package a folder of itself, which
// a recursive walk of the tree never finishes.
const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const link = join(root, 'examples/starter/node_modules/@flezha/sdk');
mkdirSync(dirname(link), { recursive: true });
rmSync(link, { recursive: true, force: true });
symlinkSync(root, link, 'dir');
try {
  await import('../examples/starter/count.test.mjs');
} finally {
  unlinkSync(link);
  for (const dir of [dirname(link), dirname(dirname(link))]) {
    try {
      rmdirSync(dir); // only when nothing else is in it
    } catch {}
  }
}
