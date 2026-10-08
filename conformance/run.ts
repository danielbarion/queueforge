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
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fixtures } from "./fixtures.ts";
import { start, TARGETS, type Target } from "./targets.ts";

const HERE = import.meta.dir;
const RESULTS = join(HERE, "results");

type Outcome = "pass" | "fail" | "skip";
type Results = Record<string, Outcome>;

/** Rows RabbitMQ 4.3 itself does not pass without a non-bundled plugin. */
const RABBIT_EXPECTED_FAIL = new Set(["Delayed messages"]);

function parseJunit(xml: string): Results {
  const out: Results = {};
  const re = /<testcase\b([^>]*?)(\/>|>([\s\S]*?)<\/testcase>)/g;
  for (const m of xml.matchAll(re)) {
    const attrs = m[1] ?? "";
    const name = /\bname="([^"]*)"/.exec(attrs)?.[1] ?? "";
    const body = m[3] ?? "";
    const decoded = name.replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&apos;/g, "'");
    out[decoded] = /<failure|<error/.test(body) ? "fail" : /<skipped/.test(body) ? "skip" : "pass";
  }
  return out;
}

async function runTarget(target: Target, filter: string | undefined): Promise<Results> {
  process.stdout.write(`\n== ${target}: starting\n`);
  const broker = await start(target);
  try {
    const outfile = join(RESULTS, `${target}.xml`);
    const extra = filter ? ["-t", filter] : [];
    // Async, so the fixture servers in this process (JWKS) keep answering.
    const proc = Bun.spawn(["bun", "test", "./tests", "--timeout", "15000", "--reporter=junit", `--reporter-outfile=${outfile}`, ...extra], {
      cwd: HERE,
      env: { ...process.env, ...broker.env },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    await proc.exited;
    const out = `${stdout}\n${stderr}`;
    if (process.env.QF_VERBOSE) process.stdout.write(out);
    const tail = out.split("\n").filter((l) => /^\s*\d+ (pass|fail)|^Ran /.test(l));
    process.stdout.write(`== ${target}: ${tail.join(" ")}\n`);
    return existsSync(outfile) && !filter ? parseJunit(readFileSync(outfile, "utf8")) : {};
  } finally {
    await broker.stop();
  }
}

function report(all: Partial<Record<Target, Results>>) {
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
  writeFileSync(join(RESULTS, "summary.json"), JSON.stringify(summary, null, 2));
  const failures = targets.flatMap((t) =>
    Object.entries(all[t]!)
      .filter(([n, v]) => v === "fail" && !(t === "rabbitmq" && RABBIT_EXPECTED_FAIL.has(n.split(" :: ")[0]!)))
      .map(([n]) => `  ${t}: ${n}`),
  );
  if (failures.length) console.log(`\nfailures:\n${failures.join("\n")}`);
}

mkdirSync(RESULTS, { recursive: true });
const argv = process.argv.slice(2);
const filterArg = argv.find((a) => a.startsWith("--filter="));
const filter = filterArg?.slice("--filter=".length);
const args = argv.filter((a) => !a.startsWith("--filter="));
const all: Partial<Record<Target, Results>> = {};
if (args.includes("--report")) {
  for (const t of TARGETS) {
    const file = join(RESULTS, `${t}.xml`);
    if (existsSync(file)) all[t] = parseJunit(readFileSync(file, "utf8"));
  }
} else {
  const chosen = (args.length ? args : TARGETS) as Target[];
  for (const t of chosen) {
    if (!TARGETS.includes(t)) throw new Error(`unknown target ${t}`);
    try {
      all[t] = await runTarget(t, filter);
    } catch (err) {
      console.error(`== ${t}: ${(err as Error).message}`);
    }
  }
  (await fixtures()).stop();
  // Keep earlier results for targets not run this time.
  for (const t of TARGETS) {
    const file = join(RESULTS, `${t}.xml`);
    if (!all[t] && existsSync(file)) all[t] = parseJunit(readFileSync(file, "utf8"));
  }
}
report(all);
