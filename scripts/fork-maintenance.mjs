import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

const repoRoot = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");
const vp = NodePath.join(repoRoot, "node_modules", "vite-plus", "bin", "vp");
const command = process.argv[2];

const run = (program, args, options = {}) => {
  const result = NodeChildProcess.spawnSync(program, args, {
    cwd: repoRoot,
    encoding: "utf8",
    stdio: options.capture ? "pipe" : "inherit",
    shell: options.shell ?? false,
  });

  if (result.error) throw result.error;
  if (result.status !== 0 && !options.allowFailure) process.exit(result.status ?? 1);
  return result;
};

const gitOutput = (...args) => run("git", args, { capture: true }).stdout.trim();

const fail = (message) => {
  console.error(`fork maintenance: ${message}`);
  process.exit(1);
};

const checkPolicyFiles = () => {
  const manifest = JSON.parse(NodeFS.readFileSync(NodePath.join(repoRoot, "package.json"), "utf8"));
  if (manifest.scripts?.["fork:check"] !== "node scripts/fork-maintenance.mjs check") {
    fail("package.json must expose the canonical fork:check command");
  }
  if (manifest.scripts?.["fork:update"] !== "node scripts/fork-maintenance.mjs update") {
    fail("package.json must expose the canonical fork:update command");
  }

  const workflow = NodeFS.readFileSync(NodePath.join(repoRoot, ".github/workflows/ci.yml"), "utf8");
  if (!/^\s{6}- mutecat\/main$/mu.test(workflow)) {
    fail("CI must run on pushes to mutecat/main");
  }
  if (!/^\s{2}fork_check:$/mu.test(workflow) || !/^\s+run: vp run fork:check$/mu.test(workflow)) {
    fail("CI must contain the separate fork:check job");
  }
  if (
    !workflow.includes("github.repository == 'muteCat3/t3code'") ||
    !workflow.includes("'ubuntu-24.04'") ||
    !workflow.includes("'macos-15'")
  ) {
    fail("CI must select GitHub-hosted runners for the muteCat fork");
  }
};

const check = () => {
  checkPolicyFiles();

  const testManifest = "scripts/fork-check-tests.json";
  const testFiles = JSON.parse(NodeFS.readFileSync(NodePath.join(repoRoot, testManifest), "utf8"));
  if (!Array.isArray(testFiles) || testFiles.length === 0) {
    fail(`${testManifest} must contain at least one focused test file`);
  }
  for (const testFile of testFiles) {
    if (typeof testFile !== "string" || !NodeFS.existsSync(NodePath.join(repoRoot, testFile))) {
      fail(`${testManifest} references a missing test: ${String(testFile)}`);
    }
  }

  run(process.execPath, [
    vp,
    "fmt",
    "--check",
    "package.json",
    "scripts/fork-maintenance.mjs",
    testManifest,
    ".github/workflows/ci.yml",
    "AGENTS.md",
    "T3CODE.md",
    "docs/README.md",
    "docs/user/agent-orchestration.md",
    "docs/internals/agent-orchestration.md",
    "docs/internals/ci.md",
    "docs/operations/fork-maintenance.md",
  ]);
  run(process.execPath, [vp, "test", "run", ...testFiles]);
};

const update = () => {
  const branch = gitOutput("branch", "--show-current");
  if (branch !== "mutecat/main") {
    fail(`fork:update must run from mutecat/main (current branch: ${branch || "detached HEAD"})`);
  }
  if (gitOutput("status", "--porcelain=v1") !== "") {
    fail("fork:update requires a clean worktree and index");
  }

  const remote = run("git", ["remote", "get-url", "upstream"], {
    allowFailure: true,
    capture: true,
  });
  if (remote.status !== 0) fail("the upstream remote is not configured");

  run("git", ["fetch", "upstream", "main:refs/remotes/upstream/main"]);

  const localMain = run("git", ["show-ref", "--verify", "--quiet", "refs/heads/main"], {
    allowFailure: true,
  });
  if (localMain.status !== 0) fail("the local main branch does not exist");

  const fastForward = run("git", ["merge-base", "--is-ancestor", "main", "upstream/main"], {
    allowFailure: true,
  });
  if (fastForward.status !== 0) {
    fail("local main has diverged from upstream/main; refusing to rewrite it");
  }

  const oldMain = gitOutput("rev-parse", "refs/heads/main");
  const upstreamMain = gitOutput("rev-parse", "refs/remotes/upstream/main");
  run("git", ["update-ref", "refs/heads/main", upstreamMain, oldMain]);
  run("git", ["merge", "--no-edit", "main"]);

  run(process.execPath, [vp, "run", "fork:check"]);
};

if (command === "check") check();
else if (command === "update") update();
else fail("expected `check` or `update`");
