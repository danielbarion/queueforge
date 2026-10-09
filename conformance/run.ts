/**
 * Run the conformance suite against each broker and print a feature table.
 *
 *   bun run.ts                      every target
 *   bun run.ts rust bun             only these
 *   bun run.ts --report             reprint from the last results/
 *
 * A feature row passes on a broker when every test titled "<row> :: ..."
 * passes there. RabbitMQ is run too: a test RabbitMQ fails is a wrong test,
 * not a gap.
 */
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fixtures } from "./fixtures.ts";
import { start, TARGETS, type Target } from "./targets.ts";

const HERE = import.meta.dir;
const RESULTS = join(HERE, "results");

type Outcome = "pass" | "fail" | "skip";
export type Results = Record<string, Outcome>;
type RunResult = { results: Results; exitCode: number };

/** Rows RabbitMQ 4.3 itself does not pass without a non-bundled plugin. */
const RABBIT_EXPECTED_FAIL = new Set(["Delayed messages"]);

export function parseJunit(xml: string): Results {
  // Reject truncated or malformed reports instead of treating an empty parse as success.
  const tags = /<!--[\s\S]*?-->|<\?[\s\S]*?\?>|<!\[CDATA\[[\s\S]*?\]\]>|<\/?[A-Za-z_][\w:.-]*(?:\s+[A-Za-z_][\w:.-]*\s*=\s*(?:"[^"]*"|'[^']*'))*\s*\/?>/g;
  const stack: string[] = [];
  let end = 0;
  let roots = 0;
  for (const match of xml.matchAll(tags)) {
    const between = xml.slice(end, match.index);
    if (between.includes("<") || (!stack.length && between.trim())) throw new Error("malformed JUnit report");
    const tag = match[0];
    end = match.index + tag.length;
    if (tag.startsWith("<!") || tag.startsWith("<?")) continue;
    const name = /^<\/?([\w:.-]+)/.exec(tag)![1]!;
    if (tag.startsWith("</")) {
      if (stack.pop() !== name) throw new Error("malformed JUnit report");
    } else {
      if (!stack.length) {
        if (++roots !== 1 || !["testsuites", "testsuite"].includes(name)) throw new Error("malformed JUnit report");
      }
      if ((name === "testsuite" || name === "testsuites") && /\berrors=["'][1-9]\d*["']/.test(tag)) {
        throw new Error("JUnit report contains suite errors");
      }
      if (!tag.endsWith("/>")) stack.push(name);
    }
  }
  if (stack.length || roots !== 1 || xml.slice(end).trim()) throw new Error("malformed JUnit report");
  const out: Results = {};
  const re = /<testcase\b([^>]*?)(\/>|>([\s\S]*?)<\/testcase>)/g;
  for (const m of xml.matchAll(re)) {
    const attrs = m[1] ?? "";
    const nameMatch = /\bname=(?:"([^"]*)"|'([^']*)')/.exec(attrs);
    const name = nameMatch?.[1] ?? nameMatch?.[2] ?? "";
    const body = m[3] ?? "";
    const decoded = name.replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&apos;/g, "'");
    if (!decoded) throw new Error("JUnit testcase has no name");
    out[decoded] = /<failure|<error/.test(body) ? "fail" : /<skipped/.test(body) ? "skip" : "pass";
  }
  if (!Object.keys(out).length) throw new Error("JUnit report contains no tests");
  return out;
}

export function runFailed(target: Target, { results, exitCode }: RunResult): boolean {
  const failed = Object.entries(results).filter(([, outcome]) => outcome === "fail");
  const unexpected = failed.some(([name]) => target !== "rabbitmq" || !RABBIT_EXPECTED_FAIL.has(name.split(" :: ")[0]!));
  // Bun uses status 1 for test failures; signals/other errors cannot be excused.
  return unexpected || (exitCode !== 0 && !(exitCode === 1 && failed.length > 0 && !unexpected));
}

async function runTarget(target: Target, filter: string | undefined, resultsDir = RESULTS): Promise<RunResult> {
  process.stdout.write(`\n== ${target}: starting\n`);
  const outfile = join(resultsDir, `${target}${filter ? ".filtered" : ""}.xml`);
  rmSync(outfile, { force: true });
  const broker = await start(target);
  try {
    const extra = filter ? ["-t", filter] : [];
    // Async, so the fixture servers in this process (JWKS) keep answering.
    const proc = Bun.spawn(["bun", "test", "./tests", "--timeout", "15000", "--reporter=junit", `--reporter-outfile=${outfile}`, ...extra], {
      cwd: HERE,
      env: { ...process.env, ...broker.env },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    const exitCode = await proc.exited;
    const out = `${stdout}\n${stderr}`;
    if (process.env.QF_VERBOSE) process.stdout.write(out);
    const tail = out.split("\n").filter((l) => /^\s*\d+ (pass|fail)|^Ran /.test(l));
    process.stdout.write(`== ${target}: ${tail.join(" ")}\n`);
    if (!existsSync(outfile)) throw new Error(`test process exited ${exitCode} without a JUnit report`);
    return { results: parseJunit(readFileSync(outfile, "utf8")), exitCode };
  } finally {
    await broker.stop();
  }
}

function report(all: Partial<Record<Target, Results>>, resultsDir = RESULTS) {
  const targets = TARGETS.filter((t) => all[t]);
  const tests = new Set<string>();
  for (const t of targets) for (const name of Object.keys(all[t]!)) tests.add(name);
  const rows = new Map<string, string[]>();
  for (const name of tests) {
    const [feature] = name.split(" :: ");
    if (!feature) continue;
    rows.set(feature, [...(rows.get(feature) ?? []), name]);
  }
  const level = (t: Target, names: string[]) => {
    const r = all[t]!;
    const passed = names.filter((n) => r[n] === "pass").length;
    return passed === names.length ? "y" : passed === 0 ? "n" : "p";
  };
  const width = Math.max(...[...rows.keys()].map((k) => k.length), 7);
  const lines = [`${"feature".padEnd(width)}  tests  ${targets.map((t) => t.padEnd(8)).join("")}`];
  const summary: Record<string, Record<string, string>> = {};
  for (const [feature, names] of [...rows].sort((a, b) => a[0].localeCompare(b[0]))) {
    summary[feature] = {};
    const cells = targets.map((t) => {
      const l = level(t, names);
      summary[feature]![t] = l;
      return l.padEnd(8);
    });
    lines.push(`${feature.padEnd(width)}  ${String(names.length).padStart(5)}  ${cells.join("")}`);
  }
  const totals = targets.map((t) => {
    const r = all[t]!;
    const pass = Object.values(r).filter((v) => v === "pass").length;
    return `${t} ${pass}/${Object.keys(r).length}`;
  });
  console.log(`\n${lines.join("\n")}\n\ntests passed: ${totals.join(", ")}`);
  writeFileSync(join(resultsDir, "summary.json"), JSON.stringify(summary, null, 2));
  const failures = targets.flatMap((t) =>
    Object.entries(all[t]!)
      .filter(([n, v]) => v === "fail" && !(t === "rabbitmq" && RABBIT_EXPECTED_FAIL.has(n.split(" :: ")[0]!)))
      .map(([n]) => `  ${t}: ${n}`),
  );
  if (failures.length) console.log(`\nfailures:\n${failures.join("\n")}`);
}

type Dependencies = {
  resultsDir?: string;
  runTarget?: (target: Target, filter: string | undefined, resultsDir: string) => Promise<RunResult>;
  stopFixtures?: () => Promise<void>;
};

export async function main(argv = process.argv.slice(2), deps: Dependencies = {}): Promise<number> {
  const resultsDir = deps.resultsDir ?? RESULTS;
  mkdirSync(resultsDir, { recursive: true });
  const filterArg = argv.find((a) => a.startsWith("--filter="));
  const filter = filterArg?.slice("--filter=".length);
  const args = argv.filter((a) => !a.startsWith("--filter="));
  const all: Partial<Record<Target, Results>> = {};
  let failed = false;
  const attempted = new Set<Target>();
  if (!args.includes("--report")) {
    const chosen = (args.length ? args : TARGETS) as Target[];
    for (const t of chosen) {
      if (!TARGETS.includes(t)) throw new Error(`unknown target ${t}`);
      attempted.add(t);
      try {
        const result = await (deps.runTarget ?? runTarget)(t, filter, resultsDir);
        failed = runFailed(t, result) || failed;
        // Filtered runs must not replace the full feature matrix.
        all[t] = filter ? {} : result.results;
      } catch (err) {
        failed = true;
        console.error(`== ${t}: ${(err as Error).message}`);
      }
    }
    try {
      await (deps.stopFixtures ?? (async () => { (await fixtures()).stop(); }))();
    } catch (err) {
      failed = true;
      console.error(`== fixtures: ${(err as Error).message}`);
    }
  }
  // Historical results only belong to targets not attempted in this run.
  for (const t of TARGETS) {
    const file = join(resultsDir, `${t}.xml`);
    if (!attempted.has(t) && existsSync(file)) {
      try { all[t] = parseJunit(readFileSync(file, "utf8")); }
      catch (err) {
        failed = true;
        console.error(`== ${t}: ${(err as Error).message}`);
      }
    }
  }
  report(all, resultsDir);
  // --report remains informational; historical assertion failures don't fail it.
  return failed ? 1 : 0;
}

if (import.meta.main) process.exitCode = await main();
