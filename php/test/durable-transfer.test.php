<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
foreach(['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Streams'] as $f)require_once __DIR__."/../src/$f.php";
$dir=sys_get_temp_dir().'/qf-transfer-'.bin2hex(random_bytes(5));$brokers=[];$clusters=[];$wire=[];$ids=['a','b','c'];
function transferPump(array $clusters,array $brokers,array &$wire,int $steps=30):void{for($i=0;$i<$steps;$i++){foreach($clusters as $c)$c->tick();$batch=$wire;$wire=[];foreach($batch as[$from,$to,$line]){$reply=$clusters[$to]->handleLine(trim($line));if($reply!==null)$wire[]=[$to,$from,$reply];}foreach($brokers as$b)$b->flushDurable();}}
function transferWait(callable $ready,array $clusters,array $brokers,array &$wire):void{for($i=0;$i<80&&!$ready();$i++){transferPump($clusters,$brokers,$wire,5);usleep(10000);}}
try {
    foreach($ids as$id){$b=new Broker(new Store("$dir/$id/messages.log"),"$dir/$id/users.json");$b->members=array_map(fn($id)=>['id'=>$id,'addr'=>'unused'],$ids);$b->nodeId=$id;
        $b->declareQueue('source',['x-dead-letter-exchange'=>'','x-dead-letter-routing-key'=>'target','x-dead-letter-strategy'=>'at-least-once']);$b->declareQueue('target',['x-queue-type'=>'quorum']);$b->declareQueue('stream',['x-queue-type'=>'stream']);$b->queues['stream']['raftGroup']=RaftNode::queueGroup('/','stream');$brokers[$id]=$b;$clusters[$id]=new Cluster($b,$id);
        $clusters[$id]->raft=new RaftNode($id,"$dir/$id/raft",$ids,fn($to,$msg)=>$clusters[$id]->peers[$to]['write'](json_encode(['v'=>1,'op'=>'raft','from'=>$id,'payload'=>$msg])."\n"),fn($g,$e)=>$b->applyRaft($g,$e['kind'],$e['data'],$e['i']),fn($g,$s)=>$b->installRaftState($g,$s),fn($g)=>$b->raftState($g),fn()=> $b->refreshRole());$clusters[$id]->registerQueueGroup($b->queues['stream']['raftGroup'],$id==='a');
    }
    foreach($clusters as$id=>$c)foreach($ids as$peer)if($peer!==$id)$c->attach($peer,function($line)use(&$wire,$id,$peer){$wire[]=[$id,$peer,$line];});
    $source=$brokers['a'];$source->enqueueLocal('source','original','body','','source',true);$id=$source->getReady('source');$source->deadLetter($id,'rejected');$source->flushDurable();
    Harness::ok('at-least-once source retained while destination awaits majority',isset($source->msgs[$id]));Harness::eq('retained source survives durable replay',1,count($source->store->replay()));Harness::eq('destination is not delivered before commit',0,$source->readyCount('target'));
    // A second transfer attempt of this same source must not propose another destination copy.
    $source->deadLetter($id,'rejected');$r=new ReflectionProperty(RaftNode::class,'groups');foreach($r->getValue($clusters['a']->raft)as$row)$row['core']->startTimer(0);
    transferWait(fn()=>!isset($source->msgs[$id]),$clusters,$brokers,$wire);Harness::ok('confirmed destination allows source removal',!isset($source->msgs[$id]));transferWait(fn()=>count(array_filter($brokers['b']->msgs,fn($m)=>$m['queue']==='target'))===1,$clusters,$brokers,$wire);foreach($brokers as$node=>$b)Harness::eq("exactly one destination copy on $node",1,count(array_filter($b->msgs,fn($m)=>$m['queue']==='target')));
    $source->declareQueue('broken',['x-queue-type'=>'quorum']);$source->queues['broken']['raftGroup']='q:missing';$source->declareQueue('retry',['x-dead-letter-exchange'=>'','x-dead-letter-routing-key'=>'broken','x-dead-letter-strategy'=>'at-least-once']);$source->enqueueLocal('retry','retry-id','retry-body','','retry',true);$retry=$source->getReady('retry');
    // An unroutable exchange must not remove an at-least-once source.
    $source->queues['retry']['args']['dlx']='missing-exchange';$source->deadLetter($retry,'rejected');$source->flushDurable();Harness::ok('unroutable destination retains source',isset($source->msgs[$retry]));
    $source->declareQueue('reject-stream',['x-queue-type'=>'stream']);$source->queues['reject-stream']['raftGroup']='q:missing';$calls=0;$source->streamAppendAsync('reject-stream','bad',[],null,'ref',1,function()use(&$calls){$calls++;});Harness::eq('proposal rejection completes exactly once',1,$calls);
    $source->streamAppendAsync('reject-stream','retry',[],null,'ref',1,function()use(&$calls){$calls++;});Harness::eq('rejected reservation is cleared for producer retry',2,$calls);
    $completed=[];$append=function(int $seq)use($source,&$completed):void{$source->streamAppendAsync('stream',"body-$seq",[],null,'publisher',$seq,function($ok,$offset)use(&$completed,$seq){$completed[]=[$seq,$ok,$offset];});};
    $append(7);$append(7);$append(8);$append(8);transferWait(fn()=>count($completed)===4,$clusters,$brokers,$wire);Harness::eq('in-flight retry callbacks all complete',4,count($completed));Harness::eq('in-flight sequence reservations append only distinct sequences',2,$source->streamNext('stream'));Harness::eq('repeated sequence returns same committed offset',[[7,true,0],[7,true,0],[8,true,1],[8,true,1]],$completed);
    $append(7);Harness::eq('durable retry does not append older sequence',2,$source->streamNext('stream'));
    $reentrant=0;$source->streamAppendAsync('stream','body-9',[],null,'publisher',9,function($ok)use($source,&$reentrant){$reentrant++;$source->streamAppendAsync('stream','body-10',[],null,'publisher',10,function()use(&$reentrant){$reentrant++;});});transferWait(fn()=>$reentrant===2,$clusters,$brokers,$wire);Harness::eq('reentrant completion drains exactly once',2,$reentrant);Harness::eq('reentrant callbacks do not propose twice',4,$source->streamNext('stream'));
    foreach($brokers as$b)$b->declareQueue('quorum-source',['x-queue-type'=>'quorum','x-dead-letter-exchange'=>'','x-dead-letter-routing-key'=>'target','x-dead-letter-strategy'=>'at-least-once']);
    $published=false;$source->publishAsync(1,1,'','quorum-source','quorum-body',2,0,[],null,null,function($ok)use(&$published){$published=$ok;});transferWait(fn()=>$published,$clusters,$brokers,$wire);
    $qid=$source->getReady('quorum-source');Harness::ok('quorum source available on leader',$qid!==null);$source->deadLetter($qid,'rejected');Harness::ok('quorum source retained before destination commit',isset($source->msgs[$qid]));
    transferWait(fn()=>count(array_filter($brokers['b']->msgs,fn($m)=>$m['queue']==='quorum-source'))===0&&!isset($source->msgs[$qid]),$clusters,$brokers,$wire);
    foreach($brokers as$node=>$b)Harness::eq("quorum dead-letter source removal replicated $node",0,count(array_filter($b->msgs,fn($m)=>$m['queue']==='quorum-source')));
    $state=$source->raftState('meta');$source->installRaftState('meta',$state);Harness::ok('canonical snapshot preserves default exchange',array_key_exists('',$source->exchanges));
    $local=new Broker(new Store("$dir/local/messages.log"),"$dir/local/users.json");$brokers['local']=$local;
    $local->declareQueue('bounded',['x-queue-type'=>'stream','x-max-length-bytes'=>4]);
    $local->streamAppend('bounded','aaa',[],null,'producer',1);$local->streamAppend('bounded','bbb',[],null,'producer',2);
    Harness::eq('Broker wires byte retention to stream',1,$local->streamFirst('bounded'));
    Harness::eq('retention preserves producer dedup state',2,$local->streamPublisherSequence('bounded','producer'));
    $local->declareQueue('aged',['x-queue-type'=>'stream','x-max-age'=>'1s']);
    foreach([1,2]as$i)$local->applyRaft('aged-group','sappend',['vhost'=>'/','queue'=>'aged','ts'=>(int)(microtime(true)*1000)-2000,'body_b64'=>base64_encode("old-$i")],$i);
    Harness::eq('Broker wires declared age retention to stream',1,$local->streamFirst('aged'));
    Harness::eq('retention keeps newest entry',1,count($local->streamRead('aged',0,10)));
    $definition=['max-priority'=>7,'federation-upstream-set'=>'all'];
    $local->applyRaft('meta','policy',['name'=>'preserved','vhost'=>'/','pattern'=>'^absent$','definition'=>$definition],0);
    $local->bootstrap('snapshot-password');$metadata=$local->raftState('meta');
    Harness::eq('user snapshot exports Rust password hash alias',$metadata['users'][0]['hash'],$metadata['users'][0]['password_hash']);
    foreach($metadata['users']as&$user)unset($user['hash']);unset($user);
    $local->installRaftState('meta',$metadata);
    Harness::eq('snapshot retains unmapped policy definition',$definition,$local->policies['/']['preserved']['definition']);
    Harness::eq('Rust user snapshot remains authenticated',true,$local->verify('admin','snapshot-password'));
    $aged=array_values(array_filter($metadata['queues'],fn($q)=>$q['name']==='aged'))[0];
    Harness::eq('stream metadata exports canonical age milliseconds',1000,$aged['args']['max_age_ms']);
    Harness::done();
}finally{foreach($brokers as$b)if(is_resource($b->store->fp))fclose($b->store->fp);$it=new RecursiveIteratorIterator(new RecursiveDirectoryIterator($dir,FilesystemIterator::SKIP_DOTS),RecursiveIteratorIterator::CHILD_FIRST);foreach($it as$p)$p->isDir()?rmdir($p->getPathname()):unlink($p->getPathname());rmdir($dir);}
