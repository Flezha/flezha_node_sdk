// Where this package's conformance test app is.
//
// The app declares the fixture classes `conformance.json` names for each
// capability, and the local-mode suite runs the same ones rather than a second
// set declared beside them: a class that drifts from the one end-to-end drives
// would prove nothing about the SDK under test.
//
// Two layouts, found rather than counted: `test_app/` beside the package once
// this package has a repository of its own, and `test/apps/node_runner` in the
// enclosing tree while it sits inside the core's.

import { existsSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { ascendTo } from './contract.mjs';

/** Where the app sits in the core's tree, relative to any ancestor. */
const IN_TREE = 'test/apps/node_runner';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

function locate() {
  const beside = resolve(packageRoot, 'test_app');
  if (existsSync(resolve(beside, 'conformance.json'))) return beside;
  const tree = ascendTo(packageRoot, `${IN_TREE}/conformance.json`);
  return tree && resolve(tree, IN_TREE);
}

const located = locate();

/** The test app's directory. */
export function testAppRoot() {
  if (!located) {
    throw new Error(
      `no conformance test app beside ${packageRoot} (test_app/) or in any tree above it ` +
        `(${IN_TREE}); the local-mode suite runs the classes it declares`,
    );
  }
  return located;
}

/** One module of the test app, imported. */
export function testAppModule(file) {
  return import(pathToFileURL(resolve(testAppRoot(), file)).href);
}

/** The app's manifest, `conformance.json`. */
export function manifest() {
  return JSON.parse(readFileSync(resolve(testAppRoot(), 'conformance.json'), 'utf8'));
}
