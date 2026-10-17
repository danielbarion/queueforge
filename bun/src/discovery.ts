/**
 * Cloud peer discovery: `[cluster] discovery = "k8s" | "consul" | "etcd" | "aws"`,
 * next to the DNS discovery in config.ts. Each backend lists the cluster
 * members' addresses (`host:port`); consul and etcd also register this node
 * and keep its registration alive. serve.ts resolves the list at startup and
 * re-checks it on an interval, joining new members through the same
 * `installMembers` path DNS discovery uses.
 *
 * The keys follow RabbitMQ's peer discovery plugins (see docs/raft.md).
 */
import { createHash, createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import type { ConfigValue, Member } from "./config.ts";

export type DiscoveryKeys = Record<string, ConfigValue | undefined>;

export interface DiscoveryBackend {
  /** Register this node, where the backend has a registry. */
  start(): Promise<void>;
  /** Member addresses, each `host:port`. */
  list(): Promise<string[]>;
  /** Remove this node's registration and stop its keepalive. */
  stop(): Promise<void>;
}

function str(keys: DiscoveryKeys, k: string, d: string): string {
  const v = keys[k];
  return typeof v === "string" && v !== "" ? v : typeof v === "number" ? String(v) : d;
}

function int(keys: DiscoveryKeys, k: string, d: number): number {
  const n = Number(keys[k]);
  return keys[k] != null && Number.isFinite(n) ? n : d;
}

function bool(keys: DiscoveryKeys, k: string, d: boolean): boolean {
  const v = keys[k];
  return v === true || v === "true" ? true : v === false || v === "false" ? false : d;
}

function readOptional(path: string): string | null {
  try {
    return readFileSync(path, "utf8");
  } catch {
    return null;
  }
}

function hostPort(host: string, port: number): string {
  return host.includes(":") && !host.startsWith("[") ? `[${host}]:${port}` : `${host}:${port}`;
}

/**
 * Members from discovered addresses plus this node, deduplicated and sorted,
 * in the same shape as {@link import("./config.ts").dnsMembers}.
 */
export function membersFrom(addrs: string[], self: Member): Member[] {
  const found: Member[] = addrs.map((addr) => ({ id: addr, addr }));
  if (!found.some((m) => m.id === self.id)) found.push(self);
  const seen = new Set<string>();
  return found.filter((m) => !seen.has(m.id) && seen.add(m.id)).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

const SA = "/var/run/secrets/kubernetes.io/serviceaccount";

/** Kubernetes: the addresses of the service's Endpoints object. */
export class K8sDiscovery implements DiscoveryBackend {
  constructor(
    private readonly keys: DiscoveryKeys,
    private readonly memberPort: number,
    private readonly env: Record<string, string | undefined> = process.env,
  ) {}

  async start() {}
  async stop() {}

  async list(): Promise<string[]> {
    const k = this.keys;
    const host = str(k, "k8s_host", this.env.KUBERNETES_SERVICE_HOST || "kubernetes.default.svc");
    const port = int(k, "k8s_port", Number(this.env.KUBERNETES_SERVICE_PORT) || 443);
    const scheme = str(k, "k8s_scheme", "https");
    const token = readOptional(str(k, "k8s_token_path", `${SA}/token`))?.trim();
    const ca = readOptional(str(k, "k8s_ca_path", `${SA}/ca.crt`));
    const namespace = readOptional(str(k, "k8s_namespace_path", `${SA}/namespace`))?.trim() || "default";
    const service = str(k, "k8s_service_name", "queueforge");
    const byHostname = str(k, "k8s_address_type", "ip") === "hostname";
    const suffix = str(k, "k8s_hostname_suffix", "");
    const url = `${scheme}://${hostPort(host, port)}/api/v1/namespaces/${encodeURIComponent(namespace)}/endpoints/${encodeURIComponent(service)}`;
    const res = await fetch(url, {
      headers: token ? { authorization: `Bearer ${token}` } : {},
      signal: AbortSignal.timeout(5000),
      ...(ca && scheme === "https" ? { tls: { ca } } : {}),
    } as RequestInit);
    if (!res.ok) throw new Error(`k8s endpoints ${res.status}`);
    const body = (await res.json()) as { subsets?: Array<{ addresses?: Array<{ ip?: string; hostname?: string }>; notReadyAddresses?: Array<{ ip?: string; hostname?: string }> }> };
    const out: string[] = [];
    for (const subset of body.subsets ?? []) {
      for (const a of [...(subset.addresses ?? []), ...(subset.notReadyAddresses ?? [])]) {
        const name = byHostname ? (a.hostname ? `${a.hostname}${suffix}` : null) : a.ip;
        if (name) out.push(hostPort(name, this.memberPort));
      }
    }
    return out;
  }
}

/** Consul: register this node with a TTL check and list the passing instances. */
export class ConsulDiscovery implements DiscoveryBackend {
  private timer: Timer | null = null;
  private readonly id: string;

  constructor(
    private readonly keys: DiscoveryKeys,
    private readonly selfHost: string,
    private readonly memberPort: number,
  ) {
    this.id = `${str(keys, "consul_svc", "queueforge")}:${hostPort(this.address(), memberPort)}`;
  }

  private base() {
    return `${str(this.keys, "consul_scheme", "http")}://${hostPort(str(this.keys, "consul_host", "localhost"), int(this.keys, "consul_port", 8500))}`;
  }

  private address() {
    return str(this.keys, "consul_svc_addr", this.selfHost);
  }

  private headers(): Record<string, string> {
    const token = str(this.keys, "consul_acl_token", "");
    return token ? { "x-consul-token": token, "content-type": "application/json" } : { "content-type": "application/json" };
  }

  async start() {
    const ttl = int(this.keys, "consul_svc_ttl", 30);
    const res = await fetch(`${this.base()}/v1/agent/service/register`, {
      method: "PUT",
      headers: this.headers(),
      body: JSON.stringify({
        ID: this.id,
        Name: str(this.keys, "consul_svc", "queueforge"),
        Address: this.address(),
        Port: this.memberPort,
        Check: { TTL: `${ttl}s`, Status: "passing" },
      }),
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`consul register ${res.status}`);
    await this.pass();
    // Pass the TTL check well inside its interval.
    this.timer = setInterval(() => void this.pass().catch(() => {}), Math.max(500, (ttl * 1000) / 2));
    this.timer.unref?.();
  }

  private async pass() {
    await fetch(`${this.base()}/v1/agent/check/pass/service:${encodeURIComponent(this.id)}`, { method: "PUT", headers: this.headers(), signal: AbortSignal.timeout(5000) });
  }

  async list(): Promise<string[]> {
    const svc = str(this.keys, "consul_svc", "queueforge");
    const res = await fetch(`${this.base()}/v1/health/service/${encodeURIComponent(svc)}?passing=true`, { headers: this.headers(), signal: AbortSignal.timeout(5000) });
    if (!res.ok) throw new Error(`consul health ${res.status}`);
    const rows = (await res.json()) as Array<{ Service?: { Address?: string; Port?: number }; Node?: { Address?: string } }>;
    return rows.flatMap((r) => {
      const host = r.Service?.Address || r.Node?.Address;
      return host ? [hostPort(host, r.Service?.Port || this.memberPort)] : [];
    });
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await fetch(`${this.base()}/v1/agent/service/deregister/${encodeURIComponent(this.id)}`, { method: "PUT", headers: this.headers(), signal: AbortSignal.timeout(5000) }).catch(() => {});
  }
}

const b64 = (s: string) => Buffer.from(s).toString("base64");

/** etcd v3 (JSON gateway): a leased key per node under the cluster's prefix. */
export class EtcdDiscovery implements DiscoveryBackend {
  private lease: string | null = null;
  private timer: Timer | null = null;
  private token: string | null = null;

  constructor(
    private readonly keys: DiscoveryKeys,
    private readonly self: string,
  ) {}

  private endpoints(): string[] {
    const v = this.keys.etcd_endpoints;
    const list = Array.isArray(v) ? v.map(String) : typeof v === "string" ? v.split(",") : ["http://127.0.0.1:2379"];
    return list.map((e) => (e.includes("://") ? e : `http://${e}`).replace(/\/$/, ""));
  }

  /** `/<prefix>/discovery/<cluster>/nodes/` */
  dir(): string {
    return `/${str(this.keys, "etcd_key_prefix", "queueforge")}/discovery/${str(this.keys, "etcd_cluster_name", "default")}/nodes/`;
  }

  private async post(path: string, body: unknown): Promise<Record<string, unknown>> {
    let last: unknown = null;
    for (const endpoint of this.endpoints()) {
      try {
        const res = await fetch(`${endpoint}${path}`, {
          method: "POST",
          headers: { "content-type": "application/json", ...(this.token ? { authorization: this.token } : {}) },
          body: JSON.stringify(body),
          signal: AbortSignal.timeout(5000),
        });
        if (!res.ok) throw new Error(`etcd ${path} ${res.status}`);
        return (await res.json()) as Record<string, unknown>;
      } catch (err) {
        last = err;
      }
    }
    throw last instanceof Error ? last : new Error("no etcd endpoint");
  }

  async start() {
    const user = str(this.keys, "etcd_username", "");
    if (user) {
      const auth = await this.post("/v3/auth/authenticate", { name: user, password: str(this.keys, "etcd_password", "") });
      this.token = typeof auth.token === "string" ? auth.token : null;
    }
    const ttl = int(this.keys, "etcd_node_ttl", 30);
    const grant = await this.post("/v3/lease/grant", { TTL: ttl });
    this.lease = String(grant.ID ?? "");
    if (!this.lease) throw new Error("etcd lease grant gave no ID");
    await this.post("/v3/kv/put", { key: b64(this.dir() + this.self), value: b64(this.self), lease: this.lease });
    this.timer = setInterval(() => void this.post("/v3/lease/keepalive", { ID: this.lease }).catch(() => {}), Math.max(500, (ttl * 1000) / 3));
    this.timer.unref?.();
  }

  async list(): Promise<string[]> {
    const dir = this.dir();
    const end = Buffer.from(dir);
    end[end.length - 1]!++;
    const res = await this.post("/v3/kv/range", { key: b64(dir), range_end: end.toString("base64") });
    const kvs = (res.kvs ?? []) as Array<{ key?: string }>;
    return kvs.map((kv) => Buffer.from(kv.key ?? "", "base64").toString().slice(dir.length)).filter((a) => a !== "");
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (this.lease) await this.post("/v3/lease/revoke", { ID: this.lease }).catch(() => {});
    this.lease = null;
  }
}

export type AwsCredentials = { accessKeyId: string; secretAccessKey: string; sessionToken?: string };

/** RFC 3986 percent-encoding, as SigV4 wants it. */
export function awsEncode(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

const sha256 = (s: string | Buffer) => createHash("sha256").update(s).digest("hex");
const hmac = (key: string | Buffer, s: string) => createHmac("sha256", key).update(s).digest();

/** The SigV4 signing key for one date, region and service. */
export function sigV4Key(secret: string, date: string, region: string, service: string): Buffer {
  return hmac(hmac(hmac(hmac(`AWS4${secret}`, date), region), service), "aws4_request");
}

/**
 * Sign one request with AWS Signature Version 4.
 *
 * @param req Method, URL and headers (lower or mixed case; `host` is added).
 * @param amzDate `YYYYMMDDTHHMMSSZ`.
 * @returns The headers to send, `authorization` and `x-amz-date` included.
 */
export function sigV4(
  req: { method: string; url: string; headers?: Record<string, string>; body?: string },
  creds: AwsCredentials,
  region: string,
  service: string,
  amzDate: string,
): Record<string, string> {
  const u = new URL(req.url);
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(req.headers ?? {})) headers[k.toLowerCase()] = v;
  headers.host ??= u.host;
  headers["x-amz-date"] = amzDate;
  if (creds.sessionToken) headers["x-amz-security-token"] = creds.sessionToken;
  const names = Object.keys(headers).sort();
  const canonicalHeaders = names.map((n) => `${n}:${headers[n]!.trim().replace(/\s+/g, " ")}\n`).join("");
  const signedHeaders = names.join(";");
  const query = [...u.searchParams.entries()]
    .map(([k, v]) => [awsEncode(k), awsEncode(v)] as const)
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join("&");
  const path = u.pathname.split("/").map((seg) => awsEncode(decodeURIComponent(seg))).join("/") || "/";
  const canonical = [req.method.toUpperCase(), path, query, canonicalHeaders, signedHeaders, sha256(req.body ?? "")].join("\n");
  const date = amzDate.slice(0, 8);
  const scope = `${date}/${region}/${service}/aws4_request`;
  const toSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256(canonical)].join("\n");
  const signature = createHmac("sha256", sigV4Key(creds.secretAccessKey, date, region, service)).update(toSign).digest("hex");
  headers.authorization = `AWS4-HMAC-SHA256 Credential=${creds.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  return headers;
}

function xmlTag(block: string, tag: string): string | null {
  const m = new RegExp(`<${tag}>([^<]*)</${tag}>`).exec(block);
  return m ? m[1]!.trim() : null;
}

/** Each instance's address from a DescribeInstances response. */
export function instanceAddresses(xml: string, usePrivateIp: boolean): string[] {
  const out: string[] = [];
  const starts: number[] = [];
  const re = /<instanceId>/g;
  for (let m = re.exec(xml); m; m = re.exec(xml)) starts.push(m.index);
  for (let i = 0; i < starts.length; i++) {
    const block = xml.slice(starts[i], starts[i + 1] ?? xml.length);
    // The instance's own fields come before its network interfaces.
    const own = block.split("<networkInterfaceSet>")[0]!;
    const addr = usePrivateIp ? xmlTag(own, "privateIpAddress") : xmlTag(own, "privateDnsName") || xmlTag(own, "ipAddress");
    if (addr) out.push(addr);
  }
  return [...new Set(out)];
}

/** AWS: running EC2 instances carrying the configured tags. */
export class AwsDiscovery implements DiscoveryBackend {
  constructor(
    private readonly keys: DiscoveryKeys,
    private readonly memberPort: number,
    private readonly env: Record<string, string | undefined> = process.env,
    private readonly now: () => Date = () => new Date(),
  ) {}

  async start() {}
  async stop() {}

  private region(): string {
    return str(this.keys, "aws_region", this.env.AWS_REGION || this.env.AWS_DEFAULT_REGION || "us-east-1");
  }

  /** Config keys, then the environment, then the instance role over IMDSv2. */
  async credentials(): Promise<AwsCredentials> {
    const id = str(this.keys, "aws_access_key_id", this.env.AWS_ACCESS_KEY_ID ?? "");
    const secret = str(this.keys, "aws_secret_access_key", this.env.AWS_SECRET_ACCESS_KEY ?? "");
    if (id && secret) return { accessKeyId: id, secretAccessKey: secret, ...(this.env.AWS_SESSION_TOKEN && !this.keys.aws_access_key_id ? { sessionToken: this.env.AWS_SESSION_TOKEN } : {}) };
    const imds = (this.env.AWS_EC2_METADATA_SERVICE_ENDPOINT || "http://169.254.169.254").replace(/\/$/, "");
    const tokenRes = await fetch(`${imds}/latest/api/token`, { method: "PUT", headers: { "x-aws-ec2-metadata-token-ttl-seconds": "21600" }, signal: AbortSignal.timeout(2000) });
    const token = await tokenRes.text();
    const h = { "x-aws-ec2-metadata-token": token };
    const role = (await (await fetch(`${imds}/latest/meta-data/iam/security-credentials/`, { headers: h, signal: AbortSignal.timeout(2000) })).text()).split("\n")[0]!.trim();
    const c = (await (await fetch(`${imds}/latest/meta-data/iam/security-credentials/${role}`, { headers: h, signal: AbortSignal.timeout(2000) })).json()) as { AccessKeyId: string; SecretAccessKey: string; Token?: string };
    return { accessKeyId: c.AccessKeyId, secretAccessKey: c.SecretAccessKey, ...(c.Token ? { sessionToken: c.Token } : {}) };
  }

  /** The DescribeInstances query for the tag filters and running instances. */
  query(nextToken?: string): string {
    const params: Array<[string, string]> = [["Action", "DescribeInstances"], ["Version", "2016-11-15"]];
    const tags = this.keys.aws_ec2_tags;
    let n = 1;
    if (tags && typeof tags === "object" && !Array.isArray(tags)) {
      for (const [k, v] of Object.entries(tags)) {
        params.push([`Filter.${n}.Name`, `tag:${k}`], [`Filter.${n}.Value.1`, String(v)]);
        n++;
      }
    }
    params.push([`Filter.${n}.Name`, "instance-state-name"], [`Filter.${n}.Value.1`, "running"]);
    if (nextToken) params.push(["NextToken", nextToken]);
    return params.map(([k, v]) => `${awsEncode(k)}=${awsEncode(v)}`).join("&");
  }

  async list(): Promise<string[]> {
    const region = this.region();
    const endpoint = str(this.keys, "aws_endpoint", `https://ec2.${region}.amazonaws.com/`).replace(/\/?$/, "/");
    const creds = await this.credentials();
    const usePrivate = bool(this.keys, "aws_use_private_ip", true);
    const out: string[] = [];
    let next: string | undefined;
    for (let page = 0; page < 20; page++) {
      const url = `${endpoint}?${this.query(next)}`;
      const amzDate = this.now().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}/, "");
      const headers = sigV4({ method: "GET", url }, creds, region, "ec2", amzDate);
      const res = await fetch(url, { headers, signal: AbortSignal.timeout(5000) });
      const xml = await res.text();
      if (!res.ok) throw new Error(`ec2 DescribeInstances ${res.status}: ${xml.slice(0, 200)}`);
      out.push(...instanceAddresses(xml, usePrivate).map((a) => hostPort(a, this.memberPort)));
      next = xmlTag(xml.slice(xml.lastIndexOf("</reservationSet>") >= 0 ? xml.lastIndexOf("</reservationSet>") : 0), "nextToken") ?? undefined;
      if (!next) break;
    }
    return out;
  }
}

/** The backend for a `[cluster] discovery` value, or null for none / dns. */
export function discoveryBackend(kind: string, keys: DiscoveryKeys, self: { host: string; addr: string }, memberPort: number): DiscoveryBackend | null {
  if (kind === "k8s") return new K8sDiscovery(keys, memberPort);
  if (kind === "consul") return new ConsulDiscovery(keys, self.host, memberPort);
  if (kind === "etcd") return new EtcdDiscovery(keys, self.addr);
  if (kind === "aws") return new AwsDiscovery(keys, memberPort);
  return null;
}
