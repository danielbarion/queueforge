#!/usr/bin/env php
<?php
// Broker Test: Verify topic permissions are enforced on publish/bind.

require_once __DIR__ . '/../src/Routing.php';
require_once __DIR__ . '/../src/Features.php';
require_once __DIR__ . '/../src/Policy.php';
require_once __DIR__ . '/../src/Auth.php';
require_once __DIR__ . '/../src/Store.php';
require_once __DIR__ . '/../src/Broker.php';

$testId = 'bf-' . getmypid();
$brokerPath = '/tmp/' . $testId . '.json';
$usersPath = '/tmp/' . $testId . '-users.json';
@unlink($brokerPath);
@unlink($usersPath);

echo "Broker Topic Permission Test\n";
echo str_repeat('=', 40) . "\n\n";

$broker = new Broker(new Store($brokerPath), $usersPath);

// Step 1: Declare queue and exchange without permissions
echo "[1] Declare test setup:\n";
try {
    $r = $broker->declareQueue('test-q');
    echo "    ✓ Queue 'test-q' created\n";
} catch (\Exception $e) {
    echo "    ✗ Queue declare failed: {$e->getMessage()}\n";
    exit(1);
}

$exID = uniqid('ex-');
try {
    $broker->declareExchange($exID, 'topic', true);
    echo "    ✓ Exchange '$exID' (topic) created\n";
} catch (\Exception $e) {
    echo "    ✗ Exchange declare failed: {$e->getMessage()}\n";
    exit(1);
}

// Step 2: Publish without permission - check if topicWriteAllowed is enforced
echo "\n[2] Test publish to topic (checking if permissions checked):\n";
$testId = 0;
try {
    $result = $broker->publish($testId, 1, 0, $exID, 'pattern.*', '{"data":"test"}', 0);
    echo "    ✓ Publish succeeded: {$result}\n";
} catch (\Exception $e) {
    echo "    ✗ Publish rejected: code {$e->getCode()} - {$e->getMessage()}\n";
}

// Step 3: Verify topic bind permission enforcement
echo "\n[3] Test bind to exchange:\n";
try {
    $broker->declareQueue('binding-q');
    $result = $broker->publish(0, 2, 1, $exID, 'pattern.*', '"binding"', 0);
    echo "    Publish: mode={$result}\n";
} catch (\Exception $e) {
    echo "    Publish rejected: code {$e->getCode()}\n";
}

echo str_repeat('-', 40) . "\n";
echo "Test complete.\n";
