<?php
declare(strict_types=1);

/**
 * A Consul agent for discovery-nonblocking.test.php, run under `php -S`.
 * Once MOCK_STATE.slow exists every request takes 3 seconds. Each request
 * is logged to MOCK_STATE.log with its start and end time.
 */

$state = (string) getenv('MOCK_STATE');
$method = $_SERVER['REQUEST_METHOD'];
$path = (string) parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$start = microtime(true);
$slow = is_file("$state.slow");
if ($slow) sleep(3);
$body = '';
if ($method === 'GET' && str_starts_with($path, '/v1/health/service/')) {
    $body = json_encode([['Node' => ['Address' => '127.0.0.9'], 'Service' => ['Address' => '127.0.0.9', 'Port' => 25672]]]);
}
file_put_contents("$state.log", json_encode(['method' => $method, 'path' => $path, 'slow' => $slow, 'start' => $start, 'end' => microtime(true)]) . "\n", FILE_APPEND);
header('Content-Type: application/json');
echo $body;
