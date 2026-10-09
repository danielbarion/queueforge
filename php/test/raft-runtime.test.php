<?php
require_once dirname(__DIR__) . '/src/RaftNode.php';
function check($ok,$why){if(!$ok)throw new Exception($why);echo "ok $why\n";}
function force($n){$r=new ReflectionProperty(RaftNode::class,'groups');$g=$r->getValue($n);foreach($g as $row)$row['core']->startTimer(0);}
$home='/tmp/qf-runtime-'.bin2hex(random_bytes(4));$nodes=[];$wire=[];$applied=[];
foreach(['a','b','c'] as $id){$nodes[$id]=new RaftNode($id,"$home/$id",['a','b','c'],function($to,$msg)use(&$wire,$id){$wire[]=[$id,$to,$msg];},function($g,$e)use(&$applied,$id){$applied[$id][$g][]=$e;},function($g,$s){},fn($g)=>[]);if($id==="a")force($nodes[$id]);}
function pump($nodes,&$wire,$count=100){for($i=0;$i<$count;$i++){foreach($nodes as $n)$n->tick();$batch=$wire;$wire=[];foreach($batch as [$from,$to,$m])$nodes[$to]->step($from,$m);}}
pump($nodes,$wire);$leader=null;foreach($nodes as $id=>$n)if($n->leader('quorum')===$id)$leader=$id;
check($leader!==null,'three-node election');$follower=$leader==='a'?'b':'a';$confirmed=false;
$nodes[$follower]->propose('quorum','enq',['message_id'=>'one','body_b64'=>'AAE='],function($ok)use(&$confirmed){$confirmed=$ok;});
pump($nodes,$wire); // Leader heartbeat can be due after commit; force timer via core tick future, using wall wait.
for($i=0;$i<30&&!$confirmed;$i++){usleep(10000);pump($nodes,$wire,3);}
check($confirmed,'forwarded confirmation');foreach($nodes as $id=>$n)check(count($applied[$id]['quorum']??[])===1,"locally applied $id");
$recovery=new RaftNode($leader,"$home/$leader",['a','b','c'],function(){},function(){},function(){},fn()=>[]);check(count($recovery->groupNames())===2,'durable recovery');
// The singleton cannot apply or confirm when its hard-state rename fails.
$dir="$home/fail";$sent=[];$didApply=false;$done=false;
$n=new RaftNode('solo',$dir,['solo'],function($to,$m)use(&$sent){$sent[]=$m;},function()use(&$didApply){$didApply=true;},function(){},fn()=>[]);
mkdir("$dir/quorum/state.json");force($n);$n->propose('quorum','enq',['message_id'=>'blocked'],function($ok)use(&$done){$done=$ok;});$n->tick();check(!$done&&!$didApply,'failed hard state suppresses application and confirmation');rmdir("$dir/quorum/state.json");usleep(110000);$n->tick();$n->tick();check($done&&$didApply,'ordered retry succeeds');
check(RaftNode::queueGroup('/a','b')!==RaftNode::queueGroup('/','a/b'),'queue group collision avoided');
// A leader's successful reply alone cannot confirm a follower's local publish.
$held=[];$wire=[];$confirmed=false;$record=count($applied[$follower]['quorum']??[]);
$nodes[$follower]->propose('quorum','enq',['message_id'=>'two','body_b64'=>'AgM='],function($ok)use(&$confirmed){$confirmed=$ok;});
for($i=0;$i<35;$i++){
 foreach($nodes as $n)$n->tick();$batch=$wire;$wire=[];
 foreach($batch as [$from,$to,$m]){
  if($to===$follower&&($m['g']??'')==='quorum'&&($m['t']??'')==='append'&&($m['commit']??0)>=3){$held[]=[$from,$to,$m];continue;}
  $nodes[$to]->step($from,$m);
 } usleep(10000);
}
check(!$confirmed&&count($held)>0,'leader reply waits for follower durable commit/application');
foreach($held as [$from,$to,$m])$nodes[$to]->step($from,$m);pump($nodes,$wire);check($confirmed&&count($applied[$follower]['quorum'])===$record+1,'confirmation completes after held commit');
// A recovered snapshot installation failure holds group traffic until retry.
$dir="$home/snapshot";RaftNode::atomic("$dir/meta",'state.json',['term'=>4,'vote'=>null]);RaftNode::atomic("$dir/meta",'snapshot.json',['index'=>7,'term'=>4,'voters'=>['solo'],'state'=>['saved'=>true]]);
$attempt=0;$installed=false;$snapNode=new RaftNode('solo',$dir,['solo'],function(){},function(){},function($g,$state)use(&$attempt,&$installed){if(++$attempt===1)throw new Exception('installation fault');$installed=$state===['saved'=>true];},fn()=>[]);
force($snapNode);$snapNode->tick();check(!$installed,'snapshot installation failure retained');usleep(110000);$snapNode->tick();check($installed,'snapshot installation retries');
