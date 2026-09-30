# QueueForge management UI

React 18 + Vite + TypeScript SPA embedded into `queueforge-mgmt` via `rust-embed`.

## Develop against a running broker

```bash
# Terminal 1 — broker with management on :15672
cargo run -p queueforge-broker -- --config configs/queueforge.example.toml --dev-bootstrap

# Terminal 2 — Vite dev server with API proxy
cd ui
npm install
npm run dev
# open http://127.0.0.1:5173
```

Vite proxies `/api`, `/healthz`, and `/readyz` to `http://127.0.0.1:15672`
(see `vite.config.ts`). Session cookies stay same-origin on the Vite host.

Default bootstrap user: `admin` / `devpassword12`.

## Production build (embedded assets)

```bash
cd ui
npm install
npm run build   # writes ui/dist
```

`queueforge-mgmt` embeds `ui/dist` at compile time. Rebuild the UI before
`cargo build` when you change frontend sources. Commit `ui/dist` so CI and
environments without Node can still compile the broker.
