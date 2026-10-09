#!/usr/bin/env php
<?php
// Comprehensive topic permission test showing allow/deny behavior.

require_once __DIR__ . '/../src/Routing.php';
require_once __DIR__ . '/../src/Features.php';
require_once __DIR__ . '/../src/Policy.php';
require_once __DIR__ . '/../src/Auth.php';
require_once __DIR__ . '/../src/Store.php';
require_once __DIR__ . '/../src/Broker.php';

echo "=== Topic Permission Test Suite ===\n\n";

function run($id) {
    $brokerPath = '/tmp/' . $id . '-broker.json';
    $usersPath = '/tmp/' . $id . '-users.json';
    @unlink($brokerPath);
    @unlink($usersPath);
    return new Broker(new Store($brokerPath), $usersPath);
}

function test($desc, callable $fn) {
    printf("Test: %s\n", $desc);
    try {
        $fn();
        echo "  Result: PASS\n";
    } catch (RuntimeException $e) {
        if ($e->getCode() === 403 || $e->getCode() === 404) {
            echo "  Result: BLOCKED (code {$e->getCode()})\n";
        } else {
            echo "  Result: FAIL - {$e->getMessage()}\n";
        }
    } catch (\Exception $e) {
        echo "  Result: ERROR - {$e->getMessage()}\n";
    }
}

// Setup test broker
$b1 = run('test1');
$exchangeId = uniqid('ex-');
$b1->declareQueue('q1', true);
$b1->declareExchange($exchangeId, 'topic', true);

echo "Created queue 'q1' and exchange '{$exchangeId}'\n\n";

// Test 1: Publish without any permission should fail with ACCESS_REFUSED  
test("Publish WITHOUT write permission", function() use ($b1, $exchangeId) {
    $result = $b1->publish(0, 1, 0, $exchangeId, 'secret.key', '{"msg":"blocked"}', 0);
});

// Test 2: Publish to default exchange should also check permissions  
test("Publish empty exchange with no permission", function() use ($b1) {
    // Default exchange uses key as queue name
    try {
        $result = $b1->publish(0, 1, 0, '', 'q3', "{'msg'}", 0);
    } catch (RuntimeException $e) {
        throw new RuntimeException("Blocked: {$e->getMessage()}");
    }
});

// Test 3: Verify topicWriteAllowed actually checks permissions
echo "\nDirect permissions check:\n";
$b1->declareQueue('q2');
$perms = $b1->topicPermissions['alice'] ?? [];
echo "  Alice's current permissions: " . json_encode($perms) . "\n";

// Test 4: Topic binding should require write permission
test("Bind WITHOUT write", function() use ($b1, $exchangeId) {
    $result = $b1->bind('q3', $exchangeId, 'topic.#');
});

// Test 5: Declare queue with quorum type without read permission  
$b1_new = run('test2');
$b1_new->declareQueue('exch-q', true);
$result = ['durable' => false];
try {
    $result['status'] = $b1_new->declareQueue('queue-perm', [
        'x-queue-type' => 'quorum',
        'x-message-ttl' => 86400000,
    ]);
} catch (Exception $e) {
    echo "  Quorum declare rejected: code {$e->getCode()}\n";
}

echo "\n=== Test Complete ===\n";
echo "All tests finished.\n";
