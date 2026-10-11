<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
// A restart replays the whole log before paging, and a portable snapshot
// holds every body base64-encoded, so this test needs more than the default.
ini_set('memory_limit','-1');
foreach(['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Streams'] as $class) require_once dirname(__DIR__).'/src/'.$class.'.php';

Harness::guard('records past the memory budget are read back from the log', static function (): void {
    $dir=sys_get_temp_dir().'/qf-stream-page-'.bin2hex(random_bytes(6));
    $stream=new Streams($dir);
    $mib=1024*1024;
    $count=intdiv(Streams::MEMORY_BYTES,$mib)+16;
    $body=static fn(int $i): string => str_repeat(chr(65+$i%26),$mib);
    for($i=0;$i<$count;$i++){$stream->append($body($i),['n'=>$i],null,'p',$i+1,null,null,null,'ex','k'.$i,false);if($i%8===7&&$i<$count-1)$stream->sync();}
    Harness::eq('an unsynced record is not readable yet',[],$stream->read($count-1,1));
    $stream->sync();
    $first=$stream->read(0,1)[0]??[];
    Harness::eq('the oldest body reads back',$body(0),$first['body']??null);
    Harness::eq('its headers read back',['n'=>0],$first['headers']??null);
    Harness::eq('its routing key reads back','k0',$first['key']??null);
    Harness::eq('the newest body is intact',$body($count-1),$stream->read($count-1,1)[0]['body']??null);
    Harness::eq('a range spans paged and held records',$count,count($stream->read(0,$count)));
    $restart=new Streams($dir);
    Harness::eq('a restart reads the oldest body',$body(0),$restart->read(0,1)[0]['body']??null);
    Harness::eq('a restart keeps the producer sequence',$count,$restart->sequence('p'));
    $replica=new Streams($dir.'-replica');$replica->install($restart->snapshot());
    Harness::eq('a snapshot carries paged bodies',$body(1),$replica->read(1,1)[0]['body']??null);
});
