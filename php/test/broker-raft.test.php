<?php
require_once __DIR__ . '/lib/Harness.php';
foreach(['Routing','Features','Policy','Codec','Auth','Store','Raft','RaftNode','Cluster','Broker','Streams'] as $f)require_once dirname(__DIR__) . "/src/$f.php";
$dir='/tmp/qf-broker-raft-'.bin2hex(random_bytes(5));$brokers=[];$clusters=[];$wire=[];$ids=['a','b','c'];
foreach($ids as $id){$b=new Broker(new Store("$dir/$id/messages.log"),"$dir/$id/users.json");$b->members=array_map(fn($id)=>['id'=>$id,'addr'=>'unused'],$ids);$b->nodeId=$id;$b->declareQueue('q',['x-queue-type'=>'quorum']);$brokers[$id]=$b;$c=new Cluster($b,$id);$clusters[$id]=$c;$c->markFreshRaft();}
foreach($clusters as $id=>$c)foreach($ids as $peer)if($peer!==$id)$c->attach($peer,function($line)use(&$wire,$id,$peer){$wire[]=[$id,$peer,$line];});
foreach($clusters as $id=>$c)foreach($ids as $peer)if($peer!==$id)$wire[]=[$id,$peer,json_encode($c->hello())];
function pumpB(&$wire,$clusters,$brokers,$steps=25){for($i=0;$i<$steps;$i++){foreach($clusters as $c)$c->tick();$batch=$wire;$wire=[];foreach($batch as [$from,$to,$line]){$reply=$clusters[$to]->handleLine(trim($line));if($reply!==null)$wire[]=[$to,$from,$reply];}foreach($brokers as $b)$b->flushDurable();}}
pumpB($wire,$clusters,$brokers);foreach($clusters as $id=>$c)Harness::ok("fresh cluster auto enables $id",$c->raftEnabled());
$r=new ReflectionProperty(RaftNode::class,'groups');foreach($r->getValue($clusters['a']->raft) as $row)$row['core']->startTimer(0);pumpB($wire,$clusters,$brokers,100);
$leader=null;foreach($clusters as $id=>$c)if($c->queueLeader('/','q')===$id)$leader=$id;Harness::ok('shared group elects queue leader',$leader!==null);
$follower=$leader==='a'?'b':'a';$done=false;$ok=null;$brokers[$follower]->publishAsync(1,1,'','q','body',2,8,[['color','blue']],null,"\0\0",function($success)use(&$done,&$ok){$done=true;$ok=$success;});Harness::eq('publication waits for commit',false,$done);
for($i=0;$i<50&&!$done;$i++){pumpB($wire,$clusters,$brokers,10);usleep(10000);}
Harness::eq('committed publication confirmed',true,$ok);foreach($brokers as $id=>$b){Harness::eq("replica body stored once $id",1,count($b->msgs));Harness::eq("replica header preserved $id",[['color','blue']],current($b->msgs)['headers']);}
Harness::eq('follower cannot deliver quorum',null,$brokers[$follower]->getReady('q'));$msg=$brokers[$leader]->getReady('q');Harness::ok('leader can deliver quorum',$msg!==null);$brokers[$leader]->ack($msg);for($i=0;$i<30;$i++){pumpB($wire,$clusters,$brokers,3);usleep(10000);}foreach($brokers as $id=>$b)Harness::eq("committed ack removes replica $id",0,count($b->msgs));
$group=RaftNode::queueGroup('/','specific');
foreach($brokers as $id=>$b){$b->declareQueue('specific',['x-queue-type'=>'quorum']);$b->queues['specific']['raftGroup']=$group;$clusters[$id]->registerQueueGroup($group,$id==='b');$b->declareQueue('stream',['x-queue-type'=>'stream']);$b->queues['stream']['raftGroup']=RaftNode::queueGroup('/','stream');$clusters[$id]->registerQueueGroup($b->queues['stream']['raftGroup'],$id==='c');}
pumpB($wire,$clusters,$brokers,100);$specificLeader=null;foreach($clusters as $id=>$c)if($c->queueLeader('/','specific')===$id)$specificLeader=$id;
Harness::eq('queue-specific leader independent from shared quorum','b',$specificLeader);
$ok=null;$brokers['a']->publishAsync(1,1,'','specific','specific-body',2,0,[],null,null,function($v)use(&$ok){$ok=$v;});for($i=0;$i<40&&$ok===null;$i++){pumpB($wire,$clusters,$brokers,5);usleep(10000);}Harness::eq('publication forwarded to queue-specific leader',true,$ok);Harness::eq('shared-group leader cannot deliver other leader queue',null,$brokers['a']->getReady('specific'));Harness::ok('queue-specific leader delivers',$brokers['b']->getReady('specific')!==null);
$streamOk=null;$brokers['a']->streamAppendAsync('stream','stream-body',[],null,null,null,function($ok,$offset)use(&$streamOk){$streamOk=[$ok,$offset];});for($i=0;$i<40&&$streamOk===null;$i++){pumpB($wire,$clusters,$brokers,5);usleep(10000);}Harness::eq('stream confirms committed offset',[true,0],$streamOk);foreach($brokers as $id=>$b)Harness::eq("committed stream replicated $id",'stream-body',$b->streamRead('stream',0,1)[0]['body']);
Harness::done();
