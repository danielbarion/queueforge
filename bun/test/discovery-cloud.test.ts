import { afterAll, expect, test } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AwsDiscovery, ConsulDiscovery, EtcdDiscovery, instanceAddresses, K8sDiscovery, membersFrom, sigV4, sigV4Key } from "../src/discovery.ts";

const servers: Array<{ stop: (force?: boolean) => unknown }> = [];
afterAll(() => {
  for (const s of servers) s.stop(true);
});

const EXAMPLE = { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" };

// AWS's published SigV4 test suite (aws-sig-v4-test-suite, as in awslabs/aws-c-auth).
test("SigV4 matches the published get-vanilla vector", () => {
  const h = sigV4({ method: "GET", url: "https://example.amazonaws.com/" }, EXAMPLE, "us-east-1", "service", "20150830T123600Z");
  expect(h.authorization).toBe(
    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
  );
});

test("SigV4 matches the published get-vanilla-query-order-key-case vector", () => {
  const h = sigV4({ method: "GET", url: "https://example.amazonaws.com/?Param2=value2&Param1=value1" }, EXAMPLE, "us-east-1", "service", "20150830T123600Z");
  expect(h.authorization).toEndWith("Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500");
});

test("SigV4 signing key derivation is deterministic per day, region and service", () => {
  const a = sigV4Key(EXAMPLE.secretAccessKey, "20150830", "us-east-1", "ec2").toString("hex");
  expect(a).toHaveLength(64);
  expect(sigV4Key(EXAMPLE.secretAccessKey, "20150830", "us-east-1", "ec2").toString("hex")).toBe(a);
  expect(sigV4Key(EXAMPLE.secretAccessKey, "20150831", "us-east-1", "ec2").toString("hex")).not.toBe(a);
});

test("membersFrom adds this node and sorts", () => {
  const self = { id: "10.0.0.2:25672", addr: "10.0.0.2:25672" };
  expect(membersFrom(["10.0.0.3:25672", "10.0.0.1:25672", "10.0.0.3:25672"], self).map((m) => m.id)).toEqual(["10.0.0.1:25672", "10.0.0.2:25672", "10.0.0.3:25672"]);
});

test("k8s lists the service's endpoint addresses with the bearer token", async () => {
  let auth = "";
  let path = "";
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req) {
      auth = req.headers.get("authorization") ?? "";
      path = new URL(req.url).pathname;
      return Response.json({
        subsets: [{ addresses: [{ ip: "10.1.0.4", hostname: "qf-0" }, { ip: "10.1.0.5", hostname: "qf-1" }], notReadyAddresses: [{ ip: "10.1.0.6", hostname: "qf-2" }] }],
      });
    },
  });
  servers.push(server);
  const dir = mkdtempSync(join(tmpdir(), "qf-k8s-"));
  writeFileSync(join(dir, "token"), "tok-123\n");
  writeFileSync(join(dir, "namespace"), "mq\n");
  const keys = {
    k8s_host: "127.0.0.1",
    k8s_port: server.port,
    k8s_scheme: "http",
    k8s_token_path: join(dir, "token"),
    k8s_namespace_path: join(dir, "namespace"),
    k8s_ca_path: join(dir, "missing.crt"),
    k8s_service_name: "rabbit",
  };
  expect(await new K8sDiscovery(keys, 25672, {}).list()).toEqual(["10.1.0.4:25672", "10.1.0.5:25672", "10.1.0.6:25672"]);
  expect(auth).toBe("Bearer tok-123");
  expect(path).toBe("/api/v1/namespaces/mq/endpoints/rabbit");
  const byName = new K8sDiscovery({ ...keys, k8s_address_type: "hostname", k8s_hostname_suffix: ".rabbit.mq.svc" }, 25672, {});
  expect(await byName.list()).toEqual(["qf-0.rabbit.mq.svc:25672", "qf-1.rabbit.mq.svc:25672", "qf-2.rabbit.mq.svc:25672"]);
});

test("consul registers with a TTL check, passes it, lists passing services and deregisters", async () => {
  const services = new Map<string, { Address: string; Port: number; Name: string }>();
  const passes: string[] = [];
  const tokens = new Set<string>();
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const url = new URL(req.url);
      tokens.add(req.headers.get("x-consul-token") ?? "");
      if (req.method === "PUT" && url.pathname === "/v1/agent/service/register") {
        const body = (await req.json()) as { ID: string; Name: string; Address: string; Port: number; Check: { TTL: string } };
        expect(body.Check.TTL).toBe("2s");
        services.set(body.ID, body);
        return new Response("");
      }
      if (req.method === "PUT" && url.pathname.startsWith("/v1/agent/check/pass/service:")) {
        passes.push(decodeURIComponent(url.pathname.slice("/v1/agent/check/pass/service:".length)));
        return new Response("");
      }
      if (req.method === "PUT" && url.pathname.startsWith("/v1/agent/service/deregister/")) {
        services.delete(decodeURIComponent(url.pathname.slice("/v1/agent/service/deregister/".length)));
        return new Response("");
      }
      if (req.method === "GET" && url.pathname === "/v1/health/service/qf") {
        expect(url.searchParams.get("passing")).toBe("true");
        return Response.json([
          ...[...services.values()].map((s) => ({ Service: { Address: s.Address, Port: s.Port }, Node: { Address: "192.0.2.1" } })),
          { Service: { Address: "", Port: 25672 }, Node: { Address: "10.2.0.9" } },
        ]);
      }
      return new Response("no", { status: 404 });
    },
  });
  servers.push(server);
  const keys = { consul_host: "127.0.0.1", consul_port: server.port, consul_svc: "qf", consul_svc_ttl: 2, consul_acl_token: "acl-1", consul_svc_addr: "10.2.0.1" };
  const d = new ConsulDiscovery(keys, "0.0.0.0", 25672);
  await d.start();
  expect([...services.values()].map((s) => [s.Name, s.Address, s.Port])).toEqual([["qf", "10.2.0.1", 25672]]);
  expect((await d.list()).sort()).toEqual(["10.2.0.1:25672", "10.2.0.9:25672"]);
  await Bun.sleep(1200);
  expect(passes.length).toBeGreaterThanOrEqual(2);
  await d.stop();
  expect(services.size).toBe(0);
  expect([...tokens]).toEqual(["acl-1"]);
});

test("etcd grants a lease, puts this node's key, keeps it alive and lists the prefix", async () => {
  const kv = new Map<string, string>();
  let keepalives = 0;
  let revoked = "";
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const path = new URL(req.url).pathname;
      const body = (await req.json()) as Record<string, string | number>;
      if (path === "/v3/lease/grant") return Response.json({ ID: "7587", TTL: String(body.TTL) });
      if (path === "/v3/kv/put") {
        expect(body.lease).toBe("7587");
        kv.set(Buffer.from(String(body.key), "base64").toString(), Buffer.from(String(body.value), "base64").toString());
        return Response.json({});
      }
      if (path === "/v3/lease/keepalive") {
        keepalives++;
        return Response.json({ result: { ID: body.ID } });
      }
      if (path === "/v3/lease/revoke") {
        revoked = String(body.ID);
        return Response.json({});
      }
      if (path === "/v3/kv/range") {
        const from = Buffer.from(String(body.key), "base64").toString();
        const to = Buffer.from(String(body.range_end), "base64").toString();
        const kvs = [...kv.keys()].filter((k) => k >= from && k < to).map((k) => ({ key: Buffer.from(k).toString("base64") }));
        return Response.json({ kvs, count: String(kvs.length) });
      }
      return new Response("no", { status: 404 });
    },
  });
  servers.push(server);
  // Another member's key, and one from another cluster that must not show up.
  kv.set("/qf/discovery/c1/nodes/10.3.0.2:25672", "x");
  kv.set("/qf/discovery/c2/nodes/10.9.9.9:25672", "x");
  const d = new EtcdDiscovery({ etcd_endpoints: ["http://127.0.0.1:1", `http://127.0.0.1:${server.port}`], etcd_key_prefix: "qf", etcd_cluster_name: "c1", etcd_node_ttl: 3 }, "10.3.0.1:25672");
  await d.start();
  expect(kv.get("/qf/discovery/c1/nodes/10.3.0.1:25672")).toBe("10.3.0.1:25672");
  expect((await d.list()).sort()).toEqual(["10.3.0.1:25672", "10.3.0.2:25672"]);
  await Bun.sleep(1200);
  expect(keepalives).toBeGreaterThanOrEqual(1);
  await d.stop();
  expect(revoked).toBe("7587");
});

const DESCRIBE = `<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <requestId>r-1</requestId>
  <reservationSet>
    <item>
      <instancesSet>
        <item>
          <instanceId>i-1</instanceId>
          <privateDnsName>ip-10-4-0-1.ec2.internal</privateDnsName>
          <privateIpAddress>10.4.0.1</privateIpAddress>
          <ipAddress>203.0.113.1</ipAddress>
          <networkInterfaceSet><item><privateIpAddress>10.4.9.9</privateIpAddress></item></networkInterfaceSet>
        </item>
        <item>
          <instanceId>i-2</instanceId>
          <privateDnsName>ip-10-4-0-2.ec2.internal</privateDnsName>
          <privateIpAddress>10.4.0.2</privateIpAddress>
        </item>
      </instancesSet>
    </item>
  </reservationSet>
</DescribeInstancesResponse>`;

test("EC2 DescribeInstances addresses: private IPs, or private DNS names", () => {
  expect(instanceAddresses(DESCRIBE, true)).toEqual(["10.4.0.1", "10.4.0.2"]);
  expect(instanceAddresses(DESCRIBE, false)).toEqual(["ip-10-4-0-1.ec2.internal", "ip-10-4-0-2.ec2.internal"]);
});

test("aws signs DescribeInstances with the tag and running filters", async () => {
  let seen: URL | null = null;
  let auth = "";
  let token = "";
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req) {
      seen = new URL(req.url);
      auth = req.headers.get("authorization") ?? "";
      token = req.headers.get("x-amz-security-token") ?? "";
      return new Response(DESCRIBE, { headers: { "content-type": "text/xml" } });
    },
  });
  servers.push(server);
  const now = () => new Date("2026-10-17T10:00:00Z");
  const d = new AwsDiscovery(
    { aws_region: "eu-west-1", aws_endpoint: `http://127.0.0.1:${server.port}/`, aws_ec2_tags: { service: "queueforge" } },
    25672,
    { AWS_ACCESS_KEY_ID: "AKIDEXAMPLE", AWS_SECRET_ACCESS_KEY: EXAMPLE.secretAccessKey, AWS_SESSION_TOKEN: "sess" },
    now,
  );
  expect(await d.list()).toEqual(["10.4.0.1:25672", "10.4.0.2:25672"]);
  const q = seen!.searchParams;
  expect(q.get("Action")).toBe("DescribeInstances");
  expect(q.get("Version")).toBe("2016-11-15");
  expect(q.get("Filter.1.Name")).toBe("tag:service");
  expect(q.get("Filter.1.Value.1")).toBe("queueforge");
  expect(q.get("Filter.2.Name")).toBe("instance-state-name");
  expect(q.get("Filter.2.Value.1")).toBe("running");
  expect(token).toBe("sess");
  expect(auth).toStartWith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261017/eu-west-1/ec2/aws4_request, SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=");
  // The signature is what sigV4 computes for that exact request.
  const expected = sigV4({ method: "GET", url: seen!.toString() }, { ...EXAMPLE, sessionToken: "sess" }, "eu-west-1", "ec2", "20261017T100000Z").authorization;
  expect(auth).toBe(expected);
});

test("aws takes the instance role's credentials from IMDSv2", async () => {
  let gotToken = "";
  const imds = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req) {
      const path = new URL(req.url).pathname;
      if (req.method === "PUT" && path === "/latest/api/token") return new Response("imds-token");
      gotToken = req.headers.get("x-aws-ec2-metadata-token") ?? "";
      if (path === "/latest/meta-data/iam/security-credentials/") return new Response("qf-role\n");
      if (path === "/latest/meta-data/iam/security-credentials/qf-role") return Response.json({ AccessKeyId: "ASIAROLE", SecretAccessKey: "s3cr3t", Token: "role-session" });
      return new Response("no", { status: 404 });
    },
  });
  servers.push(imds);
  const d = new AwsDiscovery({}, 25672, { AWS_EC2_METADATA_SERVICE_ENDPOINT: `http://127.0.0.1:${imds.port}` });
  expect(await d.credentials()).toEqual({ accessKeyId: "ASIAROLE", secretAccessKey: "s3cr3t", sessionToken: "role-session" });
  expect(gotToken).toBe("imds-token");
});

test("a broker with discovery = consul registers, starts with the discovered members, and deregisters on SIGTERM", async () => {
  const registered = new Map<string, { Address: string; Port: number }>();
  let deregistered = false;
  const consul = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const u = new URL(req.url);
      if (u.pathname === "/v1/agent/service/register") {
        const b = (await req.json()) as { ID: string; Address: string; Port: number };
        registered.set(b.ID, b);
        return new Response("");
      }
      if (u.pathname.startsWith("/v1/agent/service/deregister/")) {
        deregistered = true;
        return new Response("");
      }
      if (u.pathname.startsWith("/v1/agent/check/pass/")) return new Response("");
      if (u.pathname === "/v1/health/service/queueforge") return Response.json([...registered.values()].map((s) => ({ Service: s })));
      return new Response("", { status: 404 });
    },
  });
  servers.push(consul);
  const dir = mkdtempSync(join(tmpdir(), "qf-consul-"));
  const cfg = join(dir, "qf.toml");
  writeFileSync(cfg, `[listeners]
amqp = "127.0.0.1:38180"
management = "127.0.0.1:38181"
metrics = "127.0.0.1:38182"
[data]
dir = "${dir}/data"
[cluster]
listen = "127.0.0.1:38185"
discovery = "consul"
consul_host = "127.0.0.1"
consul_port = ${consul.port}
consul_svc_ttl = 4
`);
  const { spawn } = await import("node:child_process");
  const p = spawn("bun", ["src/main.ts", "--config", cfg, "--dev-bootstrap"], { cwd: join(import.meta.dir, ".."), stdio: "ignore" });
  try {
    let ready = false;
    for (let i = 0; i < 100 && !ready; i++) {
      ready = await fetch("http://127.0.0.1:38182/readyz").then((r) => r.ok, () => false);
      if (!ready) await Bun.sleep(100);
    }
    expect(ready).toBe(true);
    expect([...registered.values()]).toEqual([expect.objectContaining({ Address: "127.0.0.1", Port: 38185 })]);
    const nodes = (await (await fetch("http://127.0.0.1:38181/api/nodes", { headers: { authorization: `Basic ${btoa("admin:devpassword12")}` } })).json()) as Array<{ name: string }>;
    expect(nodes.map((n) => n.name)).toEqual(["127.0.0.1:38185"]);
  } finally {
    const exited = new Promise((r) => p.once("exit", r));
    p.kill("SIGTERM");
    await Promise.race([exited, Bun.sleep(3000).then(() => p.kill("SIGKILL"))]);
  }
  expect(deregistered).toBe(true);
}, 20000);
