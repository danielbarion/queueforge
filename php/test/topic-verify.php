<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/Broker.php';

echo "=== Topic Permission Verification ===\n\n";

$storeName = 'test-broker-' . getmypid();
$broker = new Broker('/tmp/' . $storeName . '.json', '/tmp/' . $storeName . '-users.json');

// Setup: declare queue and exchange
$broker->declareQueue('verification-queue', ['x-arguments' => []]);
$broker->declareExchange('verification-exch', 'topic', ['durable' => true]);

echo "Created queue 'verification-queue'\n";
echo "Created exchange 'verification-exch' (topic)\n\n";

// Test publish without permissions
echo "Test 1: Publish to topic without explicit permission...\n";
try {
    $result = $broker->publish(0, 1, 0, 'verification-exch', 'test.key', 
        json_encode(['test' => 'message']), 0);
    echo "Result: Published (default allows all)\n";
} catch (\RuntimeException $e) {
    if ($e->getCode() === 403) {
        echo "✓ PASS: Rejected with ACCESS_REFUSED (403)\n";
        echo "       Message: {$e->getMessage()}\n";
    } else {
        echo "Result: Rejected with code {$e->getCode()}: {$e->getMessage()}\n";
    }
}
echo "\n";

// Test bind without permissions  
echo "Test 2: Bind queue to topic without permission...\n";
try {
    $broker->bind('verification-queue', 'verification-exch', 'test.*');
    echo "Bind succeeded (default allows all)\n";
} catch (\RuntimeException $e) {
    if ($e->getCode() === 403) {
        echo "✓ PASS: Rejected with ACCESS_REFUSED (403)\n";
        echo "       Message: {$e->getMessage()}\n";
    } else {
        echo "Bind rejected: code {$e->getCode()}: {$e->getMessage()}\n";
    }
}
echo "\n";

// Cleanup
unset($broker, $store);
@unlink('/tmp/qfadmin.json');
echo "Verification complete.\n";
