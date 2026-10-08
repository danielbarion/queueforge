/**
 * Authentication backends after the internal user store, as RabbitMQ chains
 * them: OAuth 2.0 tokens, then LDAP.
 *
 * OAuth 2.0: the password is an RS256 JWT. Its key comes from the JWKS URL
 * by `kid`; `exp` must be in the future and `aud` must name the resource
 * server id. Scopes use RabbitMQ's form, `<id>.<perm>:<vhost>/<resource>`
 * (perm is configure, write or read; `*` is a wildcard) and
 * `<id>.tag:<tag>`.
 *
 * LDAP: a simple bind as the DN from `user_dn_pattern`. A member of
 * `admin_group` gets the administrator tag, everyone else management; vhost
 * and resource access are granted, as RabbitMQ's default queries do.
 *
 * A successful login records a {@link Principal} under the login name, which
 * the permission checks consult for names not in the internal store. Two
 * logins with one name but different tokens share it: the latest wins.
 */
import { createPublicKey, verify as verifySignature, type JsonWebKeyInput } from "node:crypto";
import { readFileSync } from "node:fs";
import { Client } from "ldapts";

export type OauthConfig = { resourceServerId: string; jwksUrl: string; jwksCaPath: string | null };
export type LdapConfig = {
  server: string;
  port: number;
  userDnPattern: string;
  adminGroup: string | null;
  bindDn: string | null;
  bindPassword: string | null;
};

export type Scope = { kind: "configure" | "write" | "read"; vhost: RegExp; resource: RegExp; routingKey: RegExp | null };

export type Principal = {
  source: "oauth" | "ldap";
  tags: string[];
  /** OAuth scopes; null grants everything (LDAP). */
  scopes: Scope[] | null;
  /** Epoch ms after which the login is no longer valid. */
  expiresAt: number | null;
};

/** A RabbitMQ scope wildcard as an anchored regular expression. */
function wildcard(pattern: string): RegExp {
  let text = pattern;
  try {
    text = decodeURIComponent(pattern);
  } catch {
    /* a stray % is literal */
  }
  return new RegExp(`^${text.split("*").map((p) => p.replace(/[.+?^${}()|[\]\\]/g, "\\$&")).join(".*")}$`);
}

/** Parse token scopes for one resource server id. Others are ignored. */
export function parseScopes(scopes: string[], resourceServerId: string): { scopes: Scope[]; tags: string[] } {
  const out: Scope[] = [];
  const tags: string[] = [];
  const prefix = `${resourceServerId}.`;
  for (const raw of scopes) {
    if (!raw.startsWith(prefix)) continue;
    const s = raw.slice(prefix.length);
    if (s.startsWith("tag:")) {
      tags.push(s.slice(4));
      continue;
    }
    const m = /^(configure|write|read):([^/]+)\/([^/]+)(?:\/(.+))?$/.exec(s);
    if (!m) continue;
    out.push({ kind: m[1] as Scope["kind"], vhost: wildcard(m[2]!), resource: wildcard(m[3]!), routingKey: m[4] ? wildcard(m[4]) : null });
  }
  return { scopes: out, tags };
}

export function principalAllows(p: Principal, vhost: string, kind: Scope["kind"], resource: string, routingKey?: string): boolean {
  if (p.expiresAt != null && Date.now() >= p.expiresAt) return false;
  if (!p.scopes) return true;
  return p.scopes.some(
    (s) => s.kind === kind && s.vhost.test(vhost) && s.resource.test(resource) && (routingKey == null || s.routingKey == null || s.routingKey.test(routingKey)),
  );
}

export function principalHasVhost(p: Principal, vhost: string): boolean {
  if (p.expiresAt != null && Date.now() >= p.expiresAt) return false;
  return !p.scopes || p.scopes.some((s) => s.vhost.test(vhost));
}

const b64 = (s: string) => Buffer.from(s, "base64url");

type Jwk = JsonWebKeyInput["key"] & { kid?: string };

export class OauthBackend {
  private keys = new Map<string, Jwk>();
  private fetchedAt = 0;
  constructor(private readonly cfg: OauthConfig) {}

  /** Fetch the JWKS, at most once every 10 s unless a `kid` is missing. */
  private async key(kid: string): Promise<Jwk | null> {
    const fresh = Date.now() - this.fetchedAt < 10_000;
    if (!this.keys.has(kid) || !fresh) {
      try {
        const ca = this.cfg.jwksCaPath ? readFileSync(this.cfg.jwksCaPath, "utf8") : undefined;
        const res = await fetch(this.cfg.jwksUrl, { tls: ca ? { ca } : undefined, signal: AbortSignal.timeout(5000) } as RequestInit);
        if (res.ok) {
          const body = (await res.json()) as { keys?: Jwk[] };
          this.keys = new Map((body.keys ?? []).filter((k) => k.kid).map((k) => [k.kid!, k]));
          this.fetchedAt = Date.now();
        }
      } catch {
        /* keep the keys already known */
      }
    }
    return this.keys.get(kid) ?? null;
  }

  /** Validate a token. Returns the principal and the token's subject, or null. */
  async login(token: string): Promise<{ principal: Principal; sub: string } | null> {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    let header: { alg?: string; kid?: string };
    let claims: Record<string, unknown>;
    try {
      header = JSON.parse(b64(parts[0]!).toString("utf8"));
      claims = JSON.parse(b64(parts[1]!).toString("utf8"));
    } catch {
      return null;
    }
    if (header.alg !== "RS256" || !header.kid) return null;
    const jwk = await this.key(header.kid);
    if (!jwk) return null;
    let ok = false;
    try {
      ok = verifySignature("RSA-SHA256", Buffer.from(`${parts[0]}.${parts[1]}`), createPublicKey({ key: jwk, format: "jwk" }), b64(parts[2]!));
    } catch {
      ok = false;
    }
    if (!ok) return null;
    const now = Date.now() / 1000;
    if (typeof claims.exp !== "number" || claims.exp <= now) return null;
    if (typeof claims.nbf === "number" && claims.nbf > now + 60) return null;
    const aud = Array.isArray(claims.aud) ? claims.aud : claims.aud == null ? [] : [claims.aud];
    if (!aud.includes(this.cfg.resourceServerId)) return null;
    const scope = typeof claims.scope === "string" ? claims.scope.split(" ") : Array.isArray(claims.scope) ? (claims.scope as string[]) : [];
    const { scopes, tags } = parseScopes(scope, this.cfg.resourceServerId);
    const sub = String(claims.sub ?? claims.client_id ?? "");
    return { principal: { source: "oauth", tags, scopes, expiresAt: claims.exp * 1000 }, sub };
  }
}

/** Escape a value for an LDAP filter (RFC 4515). */
function filterValue(s: string): string {
  return s.replace(/[\\*()\0]/g, (c) => `\\${c.charCodeAt(0).toString(16).padStart(2, "0")}`);
}

/** Escape a value for a DN attribute (RFC 4514). */
function dnValue(s: string): string {
  return s.replace(/[,+"\\<>;=]/g, (c) => `\\${c}`).replace(/^[ #]/, (c) => `\\${c}`);
}

export class LdapBackend {
  constructor(private readonly cfg: LdapConfig) {}

  private client() {
    return new Client({ url: `ldap://${this.cfg.server}:${this.cfg.port}`, timeout: 5000, connectTimeout: 5000 });
  }

  async login(user: string, password: string): Promise<Principal | null> {
    // An empty password would be an anonymous bind, which always succeeds.
    if (!user || !password) return null;
    const dn = this.cfg.userDnPattern.replaceAll("${username}", dnValue(user));
    const c = this.client();
    try {
      await c.bind(dn, password);
    } catch {
      await c.unbind().catch(() => {});
      return null;
    }
    const tags = ["management"];
    if (this.cfg.adminGroup) {
      try {
        if (this.cfg.bindDn) {
          await c.unbind().catch(() => {});
          await c.bind(this.cfg.bindDn, this.cfg.bindPassword ?? "");
        }
        const { searchEntries } = await c.search(this.cfg.adminGroup, { scope: "base", filter: `(member=${filterValue(dn)})`, attributes: ["cn"] });
        if (searchEntries.length) tags.unshift("administrator");
      } catch {
        /* no group: not an administrator */
      }
    }
    await c.unbind().catch(() => {});
    return { source: "ldap", tags, scopes: null, expiresAt: null };
  }
}
