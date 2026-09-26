# Contributing

Thanks for your interest. This is a small, dependency-light Node/TypeScript
project, and it stays pleasant to work on if changes stay focused.

## Before you start

Read the [README](README.md) and, in particular, its warning about
self-botting. The project's value depends on the extraction logic being correct:
a bad dedup or resume path silently corrupts someone's archive, and the damage
is only visible much later.

## Getting set up

```bash
npm install
npm run build      # tsc -> dist/
npm test           # 132 tests, ~8s
npm run dev -- status
```

`npm test` and `npm run build` are the two commands CI runs. Both must pass
before a pull request is merged.

You do not need a real Discord token to work on this. The test suite is fully
offline: it drives the storage, resume, and shard layers directly against
temporary directories.

## Ground rules

- **Keep the dependency count low.** The only runtime dependency is `sql.js`,
  used for the small metadata database. A PR that adds a dependency needs a
  reason in the description.
- **Do not weaken a test to make it pass.** If a test is wrong, fix the test and
  say why in the commit body. If the code is wrong, fix the code.
- **Match the existing style.** Two-space indent, single quotes, semicolons,
  `.js` extensions on relative imports (NodeNext resolution). Run `npx tsc
  --noEmit` before pushing.
- **Write comments for *why*, not *what*.** Most of this code has a comment only
  where the reasoning is non-obvious. Please keep it that way; a comment that
  restates the line below it will be asked to go.

## Tests

New behaviour needs a test. Bug fixes need a test that fails before the fix and
passes after. The suite uses the built-in Node test runner via `tsx`:

```bash
npm test                                  # everything
npx tsx --test test/writer-lock.test.ts  # one file
```

Tests write to a temporary directory and must not touch the repo's own `data/`.
If a test needs to assert on a torn write, a stale lock, or a simulated crash,
simulate it rather than mocking the filesystem.

## Pull requests

1. Branch from `master`.
2. Keep the change focused. Unrelated refactors in a bugfix PR make review
   harder than the fix itself.
3. Write a commit message that explains the change and, more importantly, why it
   was needed. `fix: not it` is not a description.
4. Open the PR against `master` and describe the behaviour change and how you
   verified it. Screenshots are welcome for CLI output changes.
5. CI runs the build and the test suite on Node 18, 20, and 22.

## Reporting bugs

Open an issue with the command you ran, the relevant part of your `.env` with
**all values redacted**, the output, and what you expected instead. Please do not
include a real token; see [SECURITY.md](SECURITY.md).

## Security

Do not open a public issue for a security problem. Follow
[SECURITY.md](SECURITY.md).

## Code of conduct

Participation is governed by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Licence

By contributing you agree that your contributions are licensed under the
[MIT License](LICENSE).
