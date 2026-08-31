# Maintain the muteCat fork

The private product branch is `mutecat/main`. Local `main` is a clean mirror of `upstream/main`; fork
changes belong only on `mutecat/main` or branches based on it.

From a clean `mutecat/main`, run:

```sh
pnpm fork:update
```

The command fetches `upstream/main`, refuses a divergent local `main`, fast-forwards the local branch,
then merges `main` into `mutecat/main` and runs `pnpm fork:check`. It never pushes, rebases,
force-updates a branch, or resolves conflicts. If the merge conflicts, resolve or abort it manually;
the check does not run until the merge succeeds.

`pnpm fork:check` validates the fork workflow wiring, applies the existing Vite+ formatting check only
to fork policy and maintenance files, and runs the explicit focused test list in
`scripts/fork-check-tests.json`. Add a stable test file to that manifest when fork-specific behavior
gains a new test seam. Full CI still runs independently on both `main` and `mutecat/main`.

The fork selects GitHub-hosted runners for that CI because upstream's Blacksmith pool is private.
The workflow keeps the original Blacksmith labels for runs in the upstream repository.
