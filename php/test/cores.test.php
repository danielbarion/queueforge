<?php
declare(strict_types=1);

/** How many PHP processes a cgroup may run. One CPU stays one process. */

require_once __DIR__ . '/lib/Harness.php';
require_once dirname(__DIR__) . '/src/Cores.php';

Harness::guard('cgroup cores', static function (): void {
    Harness::eq('a range', 4, Cores::countCpuset('0-3'));
    Harness::eq('one cpu', 1, Cores::countCpuset('0'));
    Harness::eq('a list', 4, Cores::countCpuset('0,2,4-5'));
    Harness::eq('quota of one', 1, Cores::quotaCpus('100000 100000'));
    Harness::eq('quota of four', 4, Cores::quotaCpus('400000 100000'));
    Harness::eq('unlimited quota', null, Cores::quotaCpus('max 100000'));
    Harness::eq('tighter quota', 2, Cores::fromCgroup('800000 100000', '0-1'));
    Harness::eq('tighter cpuset', 4, Cores::fromCgroup('800000 100000', '0-3'));
    Harness::eq('nothing means one', 1, Cores::fromCgroup(null, null));
});

Harness::guard('child plans', static function (): void {
    $plans = Cores::plans(4, '/var/lib/queueforge');
    Harness::eq('four ids', ['n0', 'n1', 'n2', 'n3'], array_column($plans, 'id'));
    Harness::eq('management only on the first', ['n0'], array_values(array_map(
        static fn (array $plan): string => $plan['id'],
        array_filter($plans, static fn (array $plan): bool => $plan['mgmt']),
    )));
    $dirs = array_column($plans, 'dir');
    Harness::eq('distinct directories', 4, count(array_unique($dirs)));
});

Harness::done();
