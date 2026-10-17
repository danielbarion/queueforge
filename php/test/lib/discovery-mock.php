<?php
declare(strict_types=1);

/**
 * Mock Kubernetes, Consul, etcd v3, EC2 and IMDSv2 endpoints for
 * discovery.test.php, run under `php -S`. Every request is appended to
 * MOCK_STATE.log as JSON; Consul and etcd keep their registrations in
 * MOCK_STATE so a later list sees them.
 */

$state = (string) getenv('MOCK_STATE');
$method = $_SERVER['REQUEST_METHOD'];
$uri = $_SERVER['REQUEST_URI'];
$path = (string) parse_url($uri, PHP_URL_PATH);
$headers = array_change_key_case(getallheaders(), CASE_LOWER);
$body = (string) file_get_contents('php://input');
$fp = fopen("$state.lock", 'c'); flock($fp, LOCK_EX);
$data = is_file($state) ? (json_decode((string) file_get_contents($state), true) ?: []) : [];
file_put_contents("$state.log", json_encode(['method' => $method, 'uri' => $uri, 'headers' => $headers, 'body' => $body]) . "\n", FILE_APPEND);
$reply = static function (int $status, mixed $out, string $type = 'application/json') use (&$data, $state, $fp): never {
    file_put_contents($state, json_encode($data));
    flock($fp, LOCK_UN);
    http_response_code($status);
    header("Content-Type: $type");
    echo is_string($out) ? $out : json_encode($out);
    exit;
};

// Kubernetes endpoints.
if ($method === 'GET' && $path === '/api/v1/namespaces/qf-ns/endpoints/qf-svc') {
    if (($headers['authorization'] ?? '') !== 'Bearer k8s-test-token') $reply(401, ['message' => 'unauthorized']);
    $reply(200, ['kind' => 'Endpoints', 'subsets' => [[
        'addresses' => [['ip' => '10.0.0.1', 'hostname' => 'qf-0'], ['ip' => '10.0.0.2', 'hostname' => 'qf-1']],
        'notReadyAddresses' => [['ip' => '10.0.0.3', 'hostname' => 'qf-2']],
        'ports' => [['port' => 25672]],
    ]]]);
}

// Consul agent and health API.
if (str_starts_with($path, '/v1/')) {
    if (($headers['x-consul-token'] ?? '') !== 'consul-acl') $reply(403, 'ACL not found', 'text/plain');
    if ($method === 'PUT' && $path === '/v1/agent/service/register') {
        $svc = json_decode($body, true);
        $data['consul'][$svc['ID']] = ['svc' => $svc, 'passing' => false];
        $reply(200, '');
    }
    if ($method === 'PUT' && preg_match('#^/v1/agent/check/pass/service:(.+)$#', rawurldecode($path), $m)) {
        if (!isset($data['consul'][$m[1]])) $reply(404, 'unknown check', 'text/plain');
        $data['consul'][$m[1]]['passing'] = true;
        $data['consulPasses'] = ($data['consulPasses'] ?? 0) + 1;
        $reply(200, '');
    }
    if ($method === 'PUT' && preg_match('#^/v1/agent/service/deregister/(.+)$#', rawurldecode($path), $m)) {
        unset($data['consul'][$m[1]]);
        $data['consulDeregistered'][] = $m[1];
        $reply(200, '');
    }
    if ($method === 'GET' && preg_match('#^/v1/health/service/([^/]+)$#', $path, $m)) {
        $rows = [];
        foreach ($data['consul'] ?? [] as $row) {
            if ($row['svc']['Name'] !== $m[1] || (($_GET['passing'] ?? '') === 'true' && !$row['passing'])) continue;
            $rows[] = ['Node' => ['Address' => '10.9.9.9'], 'Service' => ['Address' => $row['svc']['Address'], 'Port' => $row['svc']['Port']]];
        }
        // A peer that registered without a service address uses the node address.
        foreach ($data['consulExtra'] ?? [['node' => '10.1.0.9', 'port' => 25672]] as $extra) $rows[] = ['Node' => ['Address' => $extra['node']], 'Service' => ['Address' => '', 'Port' => $extra['port']]];
        $reply(200, $rows);
    }
}

// etcd v3 JSON gateway.
if ($method === 'POST' && str_starts_with($path, '/v3/')) {
    $req = json_decode($body, true) ?: [];
    if ($path === '/v3/auth/authenticate') $reply(($req['name'] ?? '') === 'qf' && ($req['password'] ?? '') === 'etcd-pw' ? 200 : 401, ['token' => 'etcd-token']);
    if (($headers['authorization'] ?? '') !== 'etcd-token') $reply(401, ['error' => 'auth']);
    if ($path === '/v3/lease/grant') { $data['leases'][] = $req['TTL']; $reply(200, ['ID' => '7587862070069563666', 'TTL' => (string) $req['TTL']]); }
    if ($path === '/v3/lease/keepalive') { $data['keepalives'] = ($data['keepalives'] ?? 0) + 1; $reply(200, ['result' => ['ID' => $req['ID'], 'TTL' => '30']]); }
    if ($path === '/v3/kv/put') { $data['etcd'][base64_decode($req['key'])] = ['value' => base64_decode($req['value']), 'lease' => $req['lease'] ?? null]; $reply(200, ['header' => []]); }
    if ($path === '/v3/kv/range') {
        $from = base64_decode($req['key']); $to = base64_decode($req['range_end']);
        $kvs = [];
        $all = ($data['etcd'] ?? []) + ['/qf/discovery/blue/nodes/10.2.0.7:25672' => ['value' => '10.2.0.7:25672'], '/qf/discovery/green/nodes/10.3.0.1:25672' => ['value' => 'x']];
        foreach ($all as $key => $row) if (strcmp($key, $from) >= 0 && strcmp($key, $to) < 0) $kvs[] = ['key' => base64_encode((string) $key), 'value' => base64_encode($row['value'])];
        $reply(200, ['kvs' => $kvs, 'count' => (string) count($kvs)]);
    }
}

// IMDSv2 and EC2 DescribeInstances.
if ($method === 'PUT' && $path === '/latest/api/token') $reply(($headers['x-aws-ec2-metadata-token-ttl-seconds'] ?? '') !== '' ? 200 : 400, 'imds-token', 'text/plain');
if ($method === 'GET' && str_starts_with($path, '/latest/meta-data/iam/security-credentials/')) {
    if (($headers['x-aws-ec2-metadata-token'] ?? '') !== 'imds-token') $reply(401, 'no token', 'text/plain');
    if ($path === '/latest/meta-data/iam/security-credentials/') $reply(200, "qf-role\n", 'text/plain');
    $reply(200, ['Code' => 'Success', 'AccessKeyId' => 'ASIAROLE', 'SecretAccessKey' => 'role-secret', 'Token' => 'role-session']);
}
if ($method === 'GET' && $path === '/' && ($_GET['Action'] ?? '') === 'DescribeInstances') {
    $auth = $headers['authorization'] ?? '';
    $ok = preg_match('#^AWS4-HMAC-SHA256 Credential=([^/]+)/\d{8}/eu-west-1/ec2/aws4_request, SignedHeaders=([^,]+), Signature=[0-9a-f]{64}$#', $auth, $m) === 1;
    if (!$ok) $reply(403, '<Response><Errors><Error><Code>AuthFailure</Code></Error></Errors></Response>', 'text/xml');
    $data['awsKeys'][] = $m[1];
    $xml = '<?xml version="1.0" encoding="UTF-8"?><DescribeInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>r</requestId><reservationSet>'
        . '<item><reservationId>r-1</reservationId><instancesSet><item><instanceId>i-1</instanceId><privateDnsName>ip-10-4-0-1.internal</privateDnsName><privateIpAddress>10.4.0.1</privateIpAddress><ipAddress>54.0.0.1</ipAddress>'
        . '<networkInterfaceSet><item><privateIpAddress>10.4.9.9</privateIpAddress></item></networkInterfaceSet></item>'
        . '<item><instanceId>i-2</instanceId><privateDnsName>ip-10-4-0-2.internal</privateDnsName><privateIpAddress>10.4.0.2</privateIpAddress></item></instancesSet></item>'
        . '</reservationSet></DescribeInstancesResponse>';
    $reply(200, $xml, 'text/xml');
}
$reply(404, ['error' => "no mock for $method $uri"]);
