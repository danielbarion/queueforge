<?php
declare(strict_types=1);

/**
 * Cloud peer discovery (Kubernetes, Consul, etcd, AWS) against a local mock
 * HTTP server, the AWS Signature Version 4 published test vectors, and a
 * broker that registers with Consul, passes its TTL check, picks up a member
 * that appears later and deregisters on shutdown.
 */

require_once __DIR__ . '/lib/Harness.php';
require_once dirname(__DIR__) . '/src/Discovery.php';

$mockPort = Harness::freePort();
$dir = sys_get_temp_dir() . '/qf-discovery-' . bin2hex(random_bytes(4));
mkdir($dir);
$state = "$dir/state.json";
$mock = proc_open([PHP_BINARY, '-S', "127.0.0.1:$mockPort", __DIR__ . '/lib/discovery-mock.php'], [1 => ['file', "$dir/mock.out", 'w'], 2 => ['file', "$dir/mock.err", 'w']], $pipes, null, ['MOCK_STATE' => $state] + getenv());
Harness::waitListening($mockPort, $dir);
$base = "http://127.0.0.1:$mockPort";
$mockState = static fn (): array => is_file($state) ? (json_decode((string) file_get_contents($state), true) ?: []) : [];
$requests = static fn (): array => array_map(static fn ($l) => json_decode($l, true), array_filter(explode("\n", (string) @file_get_contents("$state.log"))));
$ids = static fn (array $config): array => array_column($config['members'], 'id');

Harness::guard('AWS SigV4 published test vectors', static function (): void {
    // aws-sig-v4-test-suite get-vanilla.
    $h = Discovery::sigv4('GET', 'https://example.amazonaws.com/', [], '', 'us-east-1', 'service', 'AKIDEXAMPLE', 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY', null, '20150830T123600Z');
    Harness::eq('get-vanilla', 'AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31', $h['authorization']);
    // The IAM ListUsers example from the Signature Version 4 documentation.
    $h = Discovery::sigv4('GET', 'https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08', ['Content-Type' => 'application/x-www-form-urlencoded; charset=utf-8'], '', 'us-east-1', 'iam', 'AKIDEXAMPLE', 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY', null, '20150830T123600Z');
    Harness::eq('IAM ListUsers', 'AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7', $h['authorization']);
    $h = Discovery::sigv4('GET', 'https://example.amazonaws.com/', [], '', 'us-east-1', 'service', 'AKIDEXAMPLE', 'secret', 'session-token', '20150830T123600Z');
    Harness::ok('a session token is signed', ($h['x-amz-security-token'] ?? '') === 'session-token' && str_contains($h['authorization'], 'SignedHeaders=host;x-amz-date;x-amz-security-token'));
});

Harness::guard('TOML values for discovery keys', static function (): void {
    Harness::eq('list', ['http://a:2379', 'http://b:2379'], Discovery::tomlValue('["http://a:2379", "http://b:2379"]'));
    Harness::eq('inline table', ['role' => 'mq', 'env' => 'prod'], Discovery::tomlValue('{ role = "mq", env = "prod" }'));
    Harness::eq('integer and boolean', [30, false], [Discovery::tomlValue('30'), Discovery::tomlValue('false')]);
});

Harness::guard('Kubernetes endpoints', static function () use ($mockPort, $dir, $ids, $requests): void {
    file_put_contents("$dir/token", "k8s-test-token\n");
    file_put_contents("$dir/namespace", 'qf-ns');
    $opts = ['k8s_host' => '127.0.0.1', 'k8s_port' => $mockPort, 'k8s_scheme' => 'http', 'k8s_token_path' => "$dir/token", 'k8s_namespace_path' => "$dir/namespace", 'k8s_service_name' => 'qf-svc'];
    $config = Discovery::resolve(['discovery' => 'k8s', 'cluster' => '10.0.0.1:25672', 'node_id' => '', 'discovery_opts' => $opts]);
    Harness::eq('ready and not-ready addresses are members', ['10.0.0.1:25672', '10.0.0.2:25672', '10.0.0.3:25672'], $ids($config));
    Harness::eq('this node is the member at its listen address', '10.0.0.1:25672', $config['node_id']);
    $last = array_values(array_filter($requests(), static fn ($r) => str_contains($r['uri'], '/endpoints/')));
    Harness::eq('the bearer token is sent', 'Bearer k8s-test-token', end($last)['headers']['authorization'] ?? null);
    $config = Discovery::resolve(['discovery' => 'k8s', 'cluster' => 'qf-0.qf.svc:25672', 'dns_port' => 25673, 'discovery_opts' => $opts + ['k8s_address_type' => 'hostname', 'k8s_hostname_suffix' => '.qf.svc']]);
    Harness::eq('hostname addresses with the suffix, on dns_port', ['qf-0.qf.svc:25672', 'qf-0.qf.svc:25673', 'qf-1.qf.svc:25673', 'qf-2.qf.svc:25673'], $ids($config));
    $refused = false;
    try { Discovery::resolve(['discovery' => 'k8s', 'cluster' => '10.0.0.1:25672', 'discovery_opts' => ['k8s_token_path' => "$dir/none"] + $opts]); } catch (RuntimeException) { $refused = true; }
    Harness::ok('an API error fails startup', $refused);
});

Harness::guard('Consul', static function () use ($base, $mockPort, $ids, $mockState, $requests): void {
    $opts = ['consul_host' => '127.0.0.1', 'consul_port' => $mockPort, 'consul_acl_token' => 'consul-acl', 'consul_svc' => 'qf', 'consul_svc_ttl' => 5];
    $config = Discovery::resolve(['discovery' => 'consul', 'cluster' => '127.0.0.5:25672', 'discovery_opts' => $opts]);
    Harness::eq('registered self and the existing peer', ['10.1.0.9:25672', '127.0.0.5:25672'], $ids($config));
    $svc = $mockState()['consul']['qf:127.0.0.5:25672']['svc'] ?? null;
    Harness::eq('registration body', ['ID' => 'qf:127.0.0.5:25672', 'Name' => 'qf', 'Address' => '127.0.0.5', 'Port' => 25672, 'TTL' => '5s'], $svc === null ? null : ['ID' => $svc['ID'], 'Name' => $svc['Name'], 'Address' => $svc['Address'], 'Port' => $svc['Port'], 'TTL' => $svc['Check']['TTL']]);
    Harness::ok('the TTL check is passed', ($mockState()['consulPasses'] ?? 0) >= 1);
    Harness::ok('every request carries X-Consul-Token', array_filter($requests(), static fn ($r) => str_starts_with($r['uri'], '/v1/') && ($r['headers']['x-consul-token'] ?? '') !== 'consul-acl') === []);
    Discovery::consulDeregister(Discovery::consulAgent($opts, '127.0.0.5:25672', '127.0.0.5:25672'));
    Harness::eq('deregistered', ['qf:127.0.0.5:25672'], $mockState()['consulDeregistered'] ?? []);
});

Harness::guard('etcd v3', static function () use ($base, $ids, $mockState): void {
    $opts = ['etcd_endpoints' => ['http://127.0.0.1:1', $base], 'etcd_key_prefix' => 'qf', 'etcd_cluster_name' => 'blue', 'etcd_node_ttl' => 20, 'etcd_username' => 'qf', 'etcd_password' => 'etcd-pw'];
    $config = Discovery::resolve(['discovery' => 'etcd', 'cluster' => '127.0.0.6:25672', 'discovery_opts' => $opts]);
    Harness::eq('members from the cluster prefix only, after a dead endpoint', ['10.2.0.7:25672', '127.0.0.6:25672'], $ids($config));
    $s = $mockState();
    Harness::eq('a lease with the node TTL', [20], $s['leases'] ?? null);
    Harness::eq('this node key under the lease', ['value' => '127.0.0.6:25672', 'lease' => '7587862070069563666'], $s['etcd']['/qf/discovery/blue/nodes/127.0.0.6:25672'] ?? null);
    Harness::eq('the lease id is kept for keepalives', '7587862070069563666', $config['discovery_lease'] ?? null);
    Discovery::etcdKeepalive($opts, '127.0.0.6:25672', $config['discovery_lease']);
    Harness::eq('keepalive', 1, $mockState()['keepalives'] ?? 0);
    Harness::eq('range end of a prefix', '/a/c', Discovery::rangeEnd('/a/b'));
});

Harness::guard('AWS EC2 tags', static function () use ($base, $ids, $mockState, $requests): void {
    $opts = ['aws_region' => 'eu-west-1', 'aws_access_key_id' => 'AKIDTEST', 'aws_secret_access_key' => 'secret', 'aws_endpoint' => $base, 'aws_ec2_tags' => ['role' => 'mq', 'env' => 'prod']];
    $config = Discovery::resolve(['discovery' => 'aws', 'cluster' => '10.4.0.1:25672', 'discovery_opts' => $opts]);
    Harness::eq('private addresses of tagged running instances', ['10.4.0.1:25672', '10.4.0.2:25672'], $ids($config));
    $query = [];
    $last = array_values(array_filter($requests(), static fn ($r) => str_contains($r['uri'], 'DescribeInstances')));
    parse_str((string) parse_url(end($last)['uri'], PHP_URL_QUERY), $query);
    Harness::eq('filters: tags then running', ['tag:role', 'mq', 'tag:env', 'prod', 'instance-state-name', 'running', '2016-11-15'], [$query['Filter_1_Name'] ?? null, $query['Filter_1_Value_1'] ?? null, $query['Filter_2_Name'] ?? null, $query['Filter_2_Value_1'] ?? null, $query['Filter_3_Name'] ?? null, $query['Filter_3_Value_1'] ?? null, $query['Version'] ?? null]);
    $config = Discovery::resolve(['discovery' => 'aws', 'cluster' => '10.4.0.1:25672', 'discovery_opts' => ['aws_use_private_ip' => false, 'aws_access_key_id' => '', 'aws_imds_endpoint' => $base] + $opts]);
    Harness::eq('public addresses, falling back to the private DNS name', ['10.4.0.1:25672', '54.0.0.1:25672', 'ip-10-4-0-2.internal:25672'], $ids($config));
    Harness::eq('instance-role credentials from IMDSv2 sign the request', 'ASIAROLE', end($mockState()['awsKeys']));
    $signed = array_values(array_filter($requests(), static fn ($r) => str_contains($r['uri'], 'DescribeInstances')));
    Harness::eq('the role session token is sent', 'role-session', end($signed)['headers']['x-amz-security-token'] ?? null);
});

Harness::guard('a broker on Consul discovery', static function () use ($mockPort, $mockState, $state): void {
    $cluster = Harness::freePort(); $mgmt = Harness::freePort();
    $data = $mockState(); $data['consul'] = []; $data['consulDeregistered'] = []; $data['consulPasses'] = 0; $data['consulExtra'] = [];
    file_put_contents($state, json_encode($data));
    $broker = Harness::broker(["management = \"127.0.0.1:$mgmt\""], 10, '', [], []);
    Harness::stop($broker);
    // Same broker directory, now with discovery in its config.
    $config = (string) file_get_contents($broker['dir'] . '/config.toml');
    $config .= "\n[cluster]\nlisten = \"127.0.0.1:$cluster\"\ndiscovery = \"consul\"\nconsul_host = \"127.0.0.1\"\nconsul_port = $mockPort\nconsul_acl_token = \"consul-acl\"\nconsul_svc = \"qf\"\nconsul_svc_ttl = 2\ndiscovery_interval = 1\n";
    file_put_contents($broker['dir'] . '/config.toml', $config);
    @unlink($broker['dir'] . '/data/members.json');
    $broker = Harness::launch($broker['dir'], $broker['port']);
    Harness::waitListening($mgmt, $broker['dir']);
    $id = "qf:127.0.0.1:$cluster";
    Harness::ok('registered at startup', isset($mockState()['consul'][$id]));
    $passes = $mockState()['consulPasses'] ?? 0;
    Harness::ok('the TTL check keeps passing', Harness::until(static fn () => ($mockState()['consulPasses'] ?? 0) >= $passes + 2, 4.0));
    $data = $mockState(); $data['consulExtra'] = [['node' => '127.0.0.1', 'port' => 1]];
    file_put_contents($state, json_encode($data));
    $joined = Harness::until(static function () use ($mgmt): bool {
        return in_array('127.0.0.1:1', array_column(Harness::http($mgmt, 'GET', '/api/nodes')['json'] ?? [], 'name'), true);
    }, 5.0);
    Harness::ok('a member that appears later is added', $joined);
    Harness::stop($broker);
    usleep(300000);
    Harness::eq('deregistered on shutdown', [$id], $mockState()['consulDeregistered'] ?? []);
});

proc_terminate($mock); proc_close($mock);
Harness::done();
