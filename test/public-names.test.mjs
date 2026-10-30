// Every public concept spelled as `test/fixtures/sdk_public_names.txt` spells it.
//
// A Node cell names an export of the package entry, or of a subpath entry
// written before a slash (`testing/TaskletTestHarness`), then `.member` for a
// member of that export: a method, a static, or a field of an exported type.
// The built declarations are read through the TypeScript checker, so a type
// the entry exports only as a type is held as surely as a class. A cell
// starting with `!` names something that must not be public.

import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import test, { after } from 'node:test';

import { API, ModifierFlags, SymbolFlags } from 'typescript/unstable/sync';

import { read } from './contract.mjs';

const COLUMN = 4; // concept|python|ruby|php|node|elixir|java|rust
const ENTRIES = { '': 'index', testing: 'testing' };

function cells() {
  return read('test/fixtures/sdk_public_names.txt')
    .split('\n')
    .filter((line) => line.trim() !== '' && !line.startsWith('#'))
    .flatMap((line) => {
      const row = line.split('|');
      return row[COLUMN].split(/\s+/)
        .filter((path) => path !== '' && path !== '-')
        .map((path) => [row[0], path]);
    });
}

const declarations = Object.fromEntries(
  Object.entries(ENTRIES).map(([entry, file]) => [
    entry,
    fileURLToPath(new URL(`../dist/types/${file}.d.ts`, import.meta.url)),
  ]),
);
// The declarations belong to no tsconfig, so they open as one inferred project.
const api = new API({ cwd: fileURLToPath(new URL('..', import.meta.url)) });
const project = api
  .updateSnapshot({ openFiles: Object.values(declarations) })
  .getDefaultProjectForFile(declarations['']);
const { checker, program } = project;
after(() => api.close());

function exportsOf(entry) {
  const module = checker.getSymbolAtLocation(program.getSourceFile(declarations[entry]));
  return new Map(checker.getExportsOfModule(module).map((symbol) => [symbol.name, symbol]));
}

const exported = Object.fromEntries(Object.keys(ENTRIES).map((entry) => [entry, exportsOf(entry)]));

const declarationOf = (symbol) => (symbol.valueDeclaration ?? symbol.declarations?.[0])?.resolve();

function isPrivate(symbol) {
  const flags = declarationOf(symbol)?.modifierFlags ?? 0;
  return (flags & (ModifierFlags.Private | ModifierFlags.Protected)) !== 0;
}

function memberOf(symbol, name) {
  const instance = checker.getDeclaredTypeOfSymbol(symbol);
  const statics = checker.getTypeOfSymbolAtLocation(symbol, declarationOf(symbol));
  return checker.getPropertyOfType(instance, name) ?? checker.getPropertyOfType(statics, name);
}

function resolves(path) {
  const [entry, rest] = path.includes('/') ? path.split('/') : ['', path];
  const [name, ...members] = rest.split('.');
  let symbol = exported[entry]?.get(name);
  for (const member of members) {
    if (!symbol) return false;
    if (symbol.flags & SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
    symbol = memberOf(symbol, member);
    if (symbol && isPrivate(symbol)) return false;
  }
  return symbol !== undefined;
}

test('every concept has its one public spelling', () => {
  const wrong = cells().flatMap(([concept, path]) => {
    const name = path.replace(/^!/, '');
    const hidden = name !== path;
    return resolves(name) === hidden
      ? [`${concept}: \`${name}\` is ${hidden ? 'public' : 'missing'}`]
      : [];
  });
  assert.deepEqual(wrong, []);
});
