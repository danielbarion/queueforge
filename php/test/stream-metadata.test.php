<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
foreach(['Routing','Features','Policy','Codec','Auth','Store','Raft','RaftNode','Cluster','Broker','Streams','Protocols']as$f)require_once dirname(__DIR__)."/src/$f.php";
function smStr(string$s):string{return pack('n',strlen($s)).$s;}
function smStrings(array$a):string{$s=pack('N',count($a));foreach($a as$v)$s.=smStr($v);return$s;}
function smFrame(int$k,int$c,string$b=''):string{$s=pack('nnN',$k,1,$c).$b;return pack('N',strlen($s)).$s;}
function smExact($fp,int$n):string{$s='';while(strlen($s)<$n){$b=fread($fp,$n-strlen($s));if($b===false||$b==='')break;$s.=$b;}return$s;}
function smRead($fp):?array{$h=smExact($fp,4);if(strlen($h)!==4)return null;$s=smExact($fp,unpack('N',$h)[1]);if(strlen($s)<10)throw new RuntimeException('short stream response');return['key'=>unpack('n',$s)[1],'corr'=>unpack('N',substr($s,4,4))[1],'code'=>unpack('n',substr($s,8,2))[1],'body'=>$s];}
function smOpen(int$p){$fp=stream_socket_client("tcp://127.0.0.1:$p",$e,$m,3);if(!$fp)throw new RuntimeException($m);stream_set_timeout($fp,5);$plain="\0admin\0devpassword12";fwrite($fp,smFrame(19,1,smStr('PLAIN').pack('N',strlen($plain)).$plain));$reply=smRead($fp);if(($reply['code']??0)!==1)throw new RuntimeException('stream login failed');smRead($fp);fwrite($fp,smFrame(21,2,smStr('/')));if((smRead($fp)['code']??0)!==1)throw new RuntimeException('stream open failed');return$fp;}
function smCreate(string$n,array$parts,array$keys,array$args=[]):string{$m=pack('N',count($args));foreach($args as$k=>$v)$m.=smStr($k).smStr($v);return smStr($n).smStrings($parts).smStrings($keys).$m;}
function smTopology(array$n):array{$s=json_decode((string)@file_get_contents($n['dir'].'/data/topology.json'),true);return$s['states']['/']??[];}
function smAwait(callable$f,float$seconds=5):bool{$until=microtime(true)+$seconds;do{if($f())return true;usleep(20000);}while(microtime(true)<$until);return false;}
Harness::guard('native superstream topology is majority committed and replicated',static function():void{
 $members=[];$ports=[];$nodes=[];foreach(['a','b','c']as$id){$p=Harness::freePort();$members[]=['id'=>$id,'addr'=>"127.0.0.1:$p"];$ports[$id]=Harness::freePort();}
 try{
  foreach($members as$m)$nodes[$m['id']]=Harness::broker(['stream = "127.0.0.1:'.$ports[$m['id']].'"'],10,$m['id'],['listen'=>$m['addr'],'members'=>$members],['QUEUEFORGE_CORES'=>'1']);
  Harness::ok('all three peers enable durable Raft',smAwait(static function()use($nodes):bool{foreach($nodes as$n)if(!is_file($n['dir'].'/data/raft/enabled'))return false;return true;},10));
  $fp=smOpen($ports['a']);
  fwrite($fp,smFrame(29,10,smCreate('super',['p-0','p-1'],['west','east'])));
  $reply=smRead($fp);Harness::eq('committed create reply',['key'=>0x801d,'corr'=>10,'code'=>1],array_intersect_key($reply??[],array_flip(['key','corr','code'])));
  $durable=0;foreach($nodes as$n){foreach(file($n['dir'].'/data/raft/meta/log.jsonl',FILE_IGNORE_NEW_LINES)as$line){$e=json_decode($line,true);if(($e['kind']??'')==='binding'&&($e['data']['exchange']??'')==='super'&&($e['data']['queue']??'')==='p-1'){$durable++;break;}}}Harness::ok('final binding persisted on majority before reply',$durable>=2);
  Harness::ok('every durable replica has exchange queues bindings',smAwait(static function()use($nodes):bool{foreach($nodes as$n){$s=smTopology($n);if(($s['exchanges']['super']??null)!=='direct'||!isset($s['queues']['p-0'],$s['queues']['p-1']))return false;$bindings=array_values(array_filter($s['bindings']??[],static fn($r)=>$r['exchange']==='super'));if(count($bindings)!==2||array_column($bindings,'key')!==['west','east'])return false;}return true;}));
  foreach($nodes as$id=>$n){$s=smTopology($n);Harness::eq("$id partition order",[['x-stream-partition-order','1']],array_values(array_filter($s['bindings'],static fn($r)=>$r['queue']==='p-1'))[0]['args']);Harness::ok("$id stream group assigned",str_starts_with($s['queues']['p-0']['raftGroup']??'','q:v2:'));}
  fwrite($fp,smFrame(29,11,smCreate('invalid',['valid-part','amq.bad'],['a','b'])));Harness::eq('late invalid partition rejected',17,smRead($fp)['code']);Harness::ok('invalid request creates no exchange',!isset(smTopology($nodes['a'])['exchanges']['invalid']));
  fwrite($fp,smFrame(7,20,chr(3).smStr('p-0').pack('nn',1,0)));Harness::eq('partition subscriber installed',1,smRead($fp)['code']);
  $replica=smOpen($ports['b']);fwrite($replica,smFrame(7,21,chr(4).smStr('p-1').pack('nn',1,0)));Harness::eq('replica subscriber installed',1,smRead($replica)['code']);
  fwrite($fp,smFrame(30,12,smStr('super')));$notification=smRead($fp);Harness::eq('committed deletion notifies subscriber',16,$notification['key']);Harness::ok('notification identifies deleted partition',str_contains($notification['body'],'p-0'));Harness::eq('committed delete reply',1,smRead($fp)['code']);
  $notification=smRead($replica);Harness::eq('replica committed deletion notifies subscriber',16,$notification['key']);Harness::ok('replica notification identifies its partition',str_contains($notification['body'],'p-1'));fclose($replica);
  Harness::ok('deletion replicated without orphan bindings',smAwait(static function()use($nodes):bool{foreach($nodes as$n){$s=smTopology($n);if(isset($s['exchanges']['super'])||isset($s['queues']['p-0'])||isset($s['queues']['p-1']))return false;foreach($s['bindings']??[]as$r)if($r['exchange']==='super')return false;}return true;}));
  $gone=smOpen($ports['a']);fwrite($gone,smFrame(29,30,smCreate('disconnected',['disconnected-p'],['one'])));fclose($gone);
  Harness::ok('accepted topology completes after disconnect',smAwait(static function()use($nodes):bool{foreach($nodes as$n){$s=smTopology($n);if(!isset($s['exchanges']['disconnected'],$s['queues']['disconnected-p']))return false;foreach($s['bindings']??[]as$r)if($r['exchange']==='disconnected')continue 2;return false;}return true;}));
  Harness::stop($nodes['b']);Harness::stop($nodes['c']);unset($nodes['b'],$nodes['c']);
  stream_set_timeout($fp,1);fwrite($fp,smFrame(29,13,smCreate('isolated',['isolated-p'],['one'])));$reply=smRead($fp);Harness::ok('no majority never returns success',$reply===null||$reply['code']!==1);Harness::ok('no majority leaves local topology unchanged',!isset(smTopology($nodes['a'])['exchanges']['isolated']));fclose($fp);
 }finally{foreach($nodes as$n)Harness::stop($n);}
});
Harness::guard('superstream binding authorization validates whole request',static function():void{
 $dir=sys_get_temp_dir().'/qf-stream-auth-'.bin2hex(random_bytes(5));$b=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');$b->bootstrap('devpassword12');$b->users['limited']=Auth::hash('devpassword12');$b->tags['limited']=[];
 $b->permissions['limited']['/']=['configure'=>'.*','read'=>'.*','write'=>'^p-0$'];$p=new Protocols($b);$state=[];
 $plain="\0limited\0devpassword12";$buf=smFrame(19,1,smStr('PLAIN').pack('N',strlen($plain)).$plain).smFrame(21,2,smStr('/'));$p->stream($buf,$state);
 $buf=smFrame(29,3,smCreate('denied',['p-0','p-1'],['a','b']));$out=$p->stream($buf,$state);Harness::eq('binding write denied',16,unpack('n',substr($out,12,2))[1]);Harness::ok('denied binding leaves every resource absent',!isset($b->exchanges['denied'])&&!isset($b->queues['p-0']));
 $b->permissions['limited']['/']['write']='.*';$b->permissions['limited']['/']['read']='^other$';$buf=smFrame(29,4,smCreate('denied',['p-0'],['a']));$out=$p->stream($buf,$state);Harness::eq('binding exchange read denied',16,unpack('n',substr($out,12,2))[1]);
 $b->permissions['limited']['/']['read']='.*';$b->topicPermissions['limited']['/']['denied']=['read'=>'^allowed$','write'=>'.*'];$buf=smFrame(29,5,smCreate('denied',['p-0'],['forbidden']));$out=$p->stream($buf,$state);Harness::eq('binding routing read denied',16,unpack('n',substr($out,12,2))[1]);
 $b->topicPermissions['limited']['/']['denied']['read']='.*';$buf=smFrame(29,6,smCreate('denied',['p-0','p-1'],['a','b']));$out=$p->stream($buf,$state);Harness::eq('legacy superstream creation succeeds',1,unpack('n',substr($out,12,2))[1]);Harness::eq('legacy partition bindings retained',2,count($b->bindings));
 $b->permissions['limited']['/']['configure']='^(denied|p-0)$';$buf=smFrame(30,7,smStr('denied'));$out=$p->stream($buf,$state);Harness::eq('delete validates every partition permission',16,unpack('n',substr($out,12,2))[1]);Harness::ok('denied delete retains all partitions',isset($b->queues['p-0'],$b->queues['p-1'],$b->exchanges['denied']));
 $b->permissions['limited']['/']['configure']='.*';$buf=smFrame(30,8,smStr('denied'));$out=$p->stream($buf,$state);Harness::eq('legacy deletion succeeds',1,unpack('n',substr($out,12,2))[1]);Harness::eq('legacy deletion removes bindings',[],$b->bindings);$p->dropStream($state);
});
Harness::done();
