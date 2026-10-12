<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
// Replay reads the whole log before paging, so a restart needs more than the default.
ini_set('memory_limit','-1');
foreach(['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker'] as $class) require_once dirname(__DIR__).'/src/'.$class.'.php';

// Persistent classic bodies past Broker::RESIDENT_BYTES are read back from the
// log when delivered, as Bun pages its backlog. 150 MiB are published; 80 are
// taken before a compaction, 5 after it, and the rest after a restart.
Harness::guard('classic backlog past the resident limit is paged', static function (): void {
    $dir=sys_get_temp_dir().'/qf-classic-page-'.bin2hex(random_bytes(6));
    $mib=1024*1024;
    $count=150;
    $body=static fn(int $i): string => str_repeat(chr(65+$i%26),$mib-4).pack('N',$i);
    $paged=static fn(Broker $b): int => count(array_filter($b->msgs, static fn(array $m): bool => $m['body']===null));
    $open=static function () use ($dir): Broker { $b=new Broker(new Store("$dir/messages.log"),"$dir/users.json"); return $b; };

    $broker=$open();
    $broker->declareQueue('backlog');
    $before=memory_get_usage();
    for($i=0;$i<$count;$i++){
        $broker->publish(1,1,0,'','backlog',$body($i),2);
        if($i%16===15)$broker->flushDurable();
    }
    $broker->flushDurable();
    $grown=memory_get_usage()-$before;
    Harness::ok('most of the backlog is paged out',$paged($broker)>=$count-intdiv(Broker::RESIDENT_BYTES,$mib)-1);
    Harness::ok('resident bodies stay near the limit ('.intdiv($grown,$mib).' MiB)',$grown<Broker::RESIDENT_BYTES+32*$mib);

    $inOrder=true;
    for($i=0;$i<80;$i++) if($broker->pullBody('backlog')!==$body($i)) $inOrder=false;
    Harness::ok('the first 80 come back in order with their bodies',$inOrder);
    $broker->flushDurable();
    $broker->store->sinceCompact=256;
    Harness::ok('the log compacts with paged bodies in it',$broker->maybeCompact());
    $inOrder=true;
    for($i=80;$i<85;$i++) if($broker->pullBody('backlog')!==$body($i)) $inOrder=false;
    Harness::ok('paged bodies read back after the compaction',$inOrder);
    $broker->flushDurable();
    unset($broker);

    $restart=$open();
    Harness::eq('a restart recovers the rest',$count-85,$restart->readyCount('backlog'));
    Harness::ok('a restart pages the recovered backlog again',$paged($restart)>0);
    $inOrder=true;
    for($i=85;$i<$count;$i++) if($restart->pullBody('backlog')!==$body($i)) $inOrder=false;
    Harness::ok('the rest come back in order after the restart',$inOrder);
    Harness::eq('nothing is left',null,$restart->pullBody('backlog'));
    exec('rm -rf '.escapeshellarg($dir));
});
Harness::done();
