// The entry point. Only `config.ts`, `cores.ts` and `supervise.ts` load before
// the cores check, so a cores-mode supervisor stays a small process.
import { parseConfig } from "./config.ts";
import { grantedCores } from "./cores.ts";
import { supervise } from "./supervise.ts";

const args = process.argv.slice(2);
let configPath = "";
let dev = false;
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--config") configPath = args[++i] ?? "";
  if (args[i] === "--dev-bootstrap") dev = true;
}
if (!configPath) {
  console.error("usage: queueforge --config <file> [--dev-bootstrap]");
  process.exit(2);
}
const cfg = parseConfig(await Bun.file(configPath).text());
if ((cfg.tls || cfg.amqps || cfg.mqtts || cfg.stomps || cfg.streamTls) && (!cfg.tlsCert || !cfg.tlsKey)) {
  console.error("[tls] enabled or listeners.amqps needs cert_path and key_path (PEM)");
  process.exit(2);
}
const tls = cfg.tls ? { cert: cfg.tlsCert!, key: cfg.tlsKey!, ca: cfg.tlsCa } : null;
// The per-core parent hands each accepted socket, TLS or not, to a child.
if (process.env.QUEUEFORGE_CHILD !== "1" && grantedCores() > 1) {
  await supervise(configPath, dev, cfg);
  process.exit(0);
}
const { serve } = await import("./serve.ts");
await serve(cfg, dev, tls);
