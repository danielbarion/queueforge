/**
 * Fixtures the authentication tests share across targets: a test PKI, a
 * JWKS server for OAuth 2.0 tokens, and an OpenLDAP server.
 *
 * They start once per run. RabbitMQ in Docker reaches the JWKS server and
 * LDAP through host.docker.internal; QueueForge brokers through 127.0.0.1.
 */
import { spawnSync } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

export type Fixtures = {
  /** ca.pem, server.pem/.key, client.pem/.key (CN conf-cert-user), nobody.pem/.key (CN with no user), stranger.pem/.key (another CA). */
  pki: string;
  /** PEM private key that signs test tokens, and its JWKS key id. */
  oauthKey: string;
  oauthKid: string;
  jwksPort: number;
  ldapPort: number;
  stop: () => void;
};

export const LDAP_BASE = "dc=qf,dc=test";
export const LDAP_ADMIN_GROUP = `cn=qf-admins,ou=groups,${LDAP_BASE}`;
export const LDAP_USER_DN = `uid=\${username},ou=people,${LDAP_BASE}`;
/** The directory admin, used for group lookups (RabbitMQ's other_bind). */
export const LDAP_BIND_DN = `cn=admin,${LDAP_BASE}`;
export const LDAP_BIND_PASSWORD = "ldap-admin-pw";

let running: Fixtures | null = null;

async function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const server = createServer();
    server.once("error", fail);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => done(port));
    });
  });
}

function openssl(dir: string, args: string[]) {
  const r = spawnSync("openssl", args, { cwd: dir, encoding: "utf8" });
  if (r.status !== 0) throw new Error(`openssl ${args.join(" ")}: ${r.stderr}`);
}

function makePki(dir: string) {
  writeFileSync(join(dir, "san.ext"), "subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1\n");
  openssl(dir, ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=qf-test-ca"]);
  openssl(dir, ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost"]);
  openssl(dir, ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "server.pem", "-days", "2", "-extfile", "san.ext"]);
  openssl(dir, ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "client.key", "-out", "client.csr", "-subj", "/CN=conf-cert-user/O=QF"]);
  openssl(dir, ["x509", "-req", "-in", "client.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "client.pem", "-days", "2"]);
  // Signed by the test CA, for a CN with no broker user.
  openssl(dir, ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", "nobody.key", "-out", "nobody.csr", "-subj", "/CN=conf-no-such-user"]);
  openssl(dir, ["x509", "-req", "-in", "nobody.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "nobody.pem", "-days", "2"]);
  // A certificate from a CA the brokers do not trust.
  openssl(dir, ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "stranger.key", "-out", "stranger.pem", "-days", "2", "-subj", "/CN=conf-cert-user"]);
  // Readable by the RabbitMQ container's user.
  spawnSync("chmod", ["-R", "a+rX", dir]);
}

const LDIF = `dn: ou=people,${LDAP_BASE}
objectClass: organizationalUnit
ou: people

dn: ou=groups,${LDAP_BASE}
objectClass: organizationalUnit
ou: groups

dn: uid=ldap-admin,ou=people,${LDAP_BASE}
objectClass: inetOrgPerson
uid: ldap-admin
cn: LDAP Admin
sn: Admin
userPassword: ldap-secret

dn: uid=ldap-plain,ou=people,${LDAP_BASE}
objectClass: inetOrgPerson
uid: ldap-plain
cn: LDAP Plain
sn: Plain
userPassword: ldap-secret

dn: ${LDAP_ADMIN_GROUP}
objectClass: groupOfNames
cn: qf-admins
member: uid=ldap-admin,ou=people,${LDAP_BASE}
`;

async function startLdap(dir: string): Promise<{ port: number; stop: () => void }> {
  const port = await freePort();
  const name = `qf-conf-ldap-${process.pid}`;
  spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  writeFileSync(join(dir, "seed.ldif"), LDIF);
  const run = spawnSync(
    "docker",
    [
      "run", "-d", "--name", name,
      "-e", "LDAP_ORGANISATION=QueueForge",
      "-e", "LDAP_DOMAIN=qf.test",
      "-e", `LDAP_ADMIN_PASSWORD=${LDAP_BIND_PASSWORD}`,
      "-e", "LDAP_TLS=false",
      "-p", `127.0.0.1:${port}:389`,
      "-v", `${dir}/seed.ldif:/container/service/slapd/assets/config/bootstrap/ldif/custom/50-seed.ldif`,
      "osixia/openldap:1.5.0", "--copy-service",
    ],
    { encoding: "utf8" },
  );
  if (run.status !== 0) throw new Error(`ldap: ${run.stderr}`);
  const stop = () => void spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  // Ready once the seeded user binds.
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const r = spawnSync(
      "docker",
      ["exec", name, "ldapwhoami", "-x", "-H", "ldap://127.0.0.1", "-D", `uid=ldap-admin,ou=people,${LDAP_BASE}`, "-w", "ldap-secret"],
      { encoding: "utf8" },
    );
    if (r.status === 0) return { port, stop };
    await Bun.sleep(500);
  }
  stop();
  throw new Error("ldap did not come up");
}

/** Start the shared fixtures, once. */
export async function fixtures(): Promise<Fixtures> {
  if (running) return running;
  const pki = mkdtempSync(join(tmpdir(), "qf-conf-pki-"));
  makePki(pki);
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const oauthKid = "qf-conformance";
  const oauthKey = join(pki, "oauth.key");
  writeFileSync(oauthKey, privateKey.export({ type: "pkcs8", format: "pem" }));
  const jwk = { ...publicKey.export({ format: "jwk" }), kid: oauthKid, use: "sig", alg: "RS256" };
  const jwksPort = await freePort();
  // JWKS must be served over HTTPS (RabbitMQ refuses http); the test CA signs it.
  const jwks = Bun.serve({
    hostname: "127.0.0.1",
    port: jwksPort,
    tls: { cert: Bun.file(join(pki, "server.pem")), key: Bun.file(join(pki, "server.key")) },
    fetch: () => Response.json({ keys: [jwk] }),
  });
  const ldap = await startLdap(pki);
  running = {
    pki,
    oauthKey,
    oauthKid,
    jwksPort,
    ldapPort: ldap.port,
    stop: () => {
      jwks.stop(true);
      ldap.stop();
      rmSync(pki, { recursive: true, force: true });
      running = null;
    },
  };
  return running;
}
