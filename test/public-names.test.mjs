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
import test from 'node:test';

import ts from 'typescript';

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
const program = ts.createProgram(Object.values(declarations), { noEmit: true });
const checker = program.getTypeChecker();

function exportsOf(entry) {
  const module = checker.getSymbolAtLocation(program.getSourceFile(declarations[entry]));
  return new Map(checker.getExportsOfModule(module).map((symbol) => [symbol.name, symbol]));
}

const exported = Object.fromEntries(Object.keys(ENTRIES).map((entry) => [entry, exportsOf(entry)]));

function isPrivate(symbol) {
  const declaration = symbol.valueDeclaration ?? symbol.declarations?.[0];
  if (!declaration) return false;
  const flags = ts.getCombinedModifierFlags(declaration);
  return (flags & (ts.ModifierFlags.Private | ts.ModifierFlags.Protected)) !== 0;
}

function memberOf(symbol, name) {
  const declaration = symbol.valueDeclaration ?? symbol.declarations?.[0];
  const instance = checker.getDeclaredTypeOfSymbol(symbol);
  const statics = checker.getTypeOfSymbolAtLocation(symbol, declaration);
  return checker.getPropertyOfType(instance, name) ?? checker.getPropertyOfType(statics, name);
}

function resolves(path) {
  const [entry, rest] = path.includes('/') ? path.split('/') : ['', path];
  const [name, ...members] = rest.split('.');
  let symbol = exported[entry]?.get(name);
  for (const member of members) {
    if (!symbol) return false;
    if (symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
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
