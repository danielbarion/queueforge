import { expect, test } from "bun:test";
import { parseConfig, parseValue } from "../src/config.ts";

test("parseValue reads strings, numbers, booleans, arrays and inline tables", () => {
  expect(parseValue(`"a # not a comment"`)).toBe("a # not a comment");
  expect(parseValue("1_800_000")).toBe(1800000);
  expect(parseValue("true")).toBe(true);
  expect(parseValue(`["http://127.0.0.1:2379", "http://h2:2379"]`)).toEqual(["http://127.0.0.1:2379", "http://h2:2379"]);
  expect(parseValue(`{ in_group = "cn=admins,ou=groups,dc=qf", attribute = "uniqueMember" }`)).toEqual({ in_group: "cn=admins,ou=groups,dc=qf", attribute: "uniqueMember" });
  expect(parseValue(`{ constant = false }`)).toEqual({ constant: false });
  expect(parseValue(`{ service = "mq", "aws:env" = "prod" }`)).toEqual({ service: "mq", "aws:env": "prod" });
});

test("the conformance config's LDAP resource access, consumer timeout and cluster keys", () => {
  const cfg = parseConfig(`
consumer_timeout = 900000
[auth.ldap]
server = "127.0.0.1"
port = 3389
user_dn_pattern = "uid=\${username},ou=people,dc=qf,dc=test"
admin_group = "cn=qf-admins,ou=groups,dc=qf,dc=test"
bind_dn = "cn=admin,dc=qf,dc=test"
bind_password = "secret" # trailing comment

# RabbitMQ's resource_access_query: only qf-admins may configure.
[auth.ldap.resource_access]
configure = { in_group = "cn=qf-admins,ou=groups,dc=qf,dc=test" }
write = { constant = true }
read = { constant = true }
topic = { in_group = "cn=t,dc=qf", attribute = "uniqueMember" }

[cluster]
discovery = "etcd"
etcd_endpoints = [
  "http://127.0.0.1:2379", # first
  "http://127.0.0.1:2380",
]
aws_ec2_tags = { service = "queueforge", env = "test" }
etcd_node_ttl = 15
`);
  expect(cfg.consumerTimeout).toBe(900000);
  expect(cfg.ldap?.bindPassword).toBe("secret");
  expect(cfg.ldap?.port).toBe(3389);
  expect(cfg.ldap?.resourceAccess).toEqual({
    configure: { inGroup: "cn=qf-admins,ou=groups,dc=qf,dc=test", attribute: "member" },
    write: { constant: true },
    read: { constant: true },
    topic: { inGroup: "cn=t,dc=qf", attribute: "uniqueMember" },
  });
  expect(cfg.discovery).toBe("etcd");
  expect(cfg.clusterKeys?.etcd_endpoints).toEqual(["http://127.0.0.1:2379", "http://127.0.0.1:2380"]);
  expect(cfg.clusterKeys?.aws_ec2_tags).toEqual({ service: "queueforge", env: "test" });
  expect(cfg.clusterKeys?.etcd_node_ttl).toBe(15);
});

test("consumer_timeout under [limits] and the default", () => {
  expect(parseConfig("[limits]\nconsumer_timeout = 5000\n").consumerTimeout).toBe(5000);
  expect(parseConfig("").consumerTimeout).toBe(1_800_000);
});

test("members with inline tables still parse", () => {
  const cfg = parseConfig(`[cluster]
node_id = "a"
members = [
  { id = "a", addr = "127.0.0.1:1" },
  { id = "b", addr = "127.0.0.1:2" },
]
`);
  expect(cfg.members).toEqual([
    { id: "a", addr: "127.0.0.1:1" },
    { id: "b", addr: "127.0.0.1:2" },
  ]);
});
