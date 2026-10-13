import postgres from "postgres";

let client: ReturnType<typeof postgres> | null = null;
let ready: Promise<void> | null = null;

export function databaseConfigured(): boolean {
  return Boolean(process.env.DATABASE_URL);
}

export function sql() {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error("DATABASE_URL is not set");
  client ??= postgres(url, { max: 5, idle_timeout: 20, connect_timeout: 10 });
  return client;
}

/** Creates the console tables. Safe to call on every request. */
export function migrate(): Promise<void> {
  ready ??= (async () => {
    const db = sql();
    await db`
      create table if not exists operators (
        id uuid primary key,
        username text not null unique,
        password_hash text not null,
        created_at timestamptz not null default now()
      )`;
    await db`
      create table if not exists sessions (
        id text primary key,
        operator_id uuid not null references operators (id) on delete cascade,
        expires_at timestamptz not null
      )`;
    await db`
      create table if not exists brokers (
        id uuid primary key,
        operator_id uuid not null references operators (id) on delete cascade,
        name text not null,
        url text not null,
        kind text not null,
        selected boolean not null default false,
        created_at timestamptz not null default now()
      )`;
    await db`
      create table if not exists broker_credentials (
        session_id text not null references sessions (id) on delete cascade,
        broker_url text not null,
        kind text not null,
        value text not null,
        primary key (session_id, broker_url)
      )`;
  })();
  return ready;
}
