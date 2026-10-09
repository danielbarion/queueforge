<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
foreach(['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Amqp10','Integrations']as$class)require_once dirname(__DIR__).'/src/'.$class.'.php';
function igBroker():Broker{$dir=sys_get_temp_dir().'/qf-integrations-'.bin2hex(random_bytes(6));$b=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');$b->bootstrap('devpassword12');return$b;}
function igShovel(Broker $b,string $name,string $src,string $dst,string $srcUri='amqp://',string $destUri='amqp://'):void{$b->parameters['shovel']['/'][$name]=['src-uri'=>$srcUri,'dest-uri'=>$destUri,'src-queue'=>$src,'dest-queue'=>$dst];}
Harness::guard('local shovel durability binary properties and cancellation',static function():void{
 $b=igBroker();$b->declareQueue('src');$b->declareQueue('dst');$raw=Amqp10::writeProps(['contentType'=>'application/octet-stream','messageId'=>'id-1','deliveryMode'=>2],[['binary',"\0\xff"]]);
 $b->publish(0,0,0,'','src',"body\0\xff",2,4,[['binary',"\0\xff"]],null,$raw);$b->flushDurable();igShovel($b,'move','src','dst');$i=new Integrations($b);$i->tick();
 Harness::eq('source removed after durable acceptance',0,$b->readyCount('src'));Harness::eq('destination gets one body',1,$b->readyCount('dst'));$id=$b->getReady('dst');Harness::eq('binary body preserved',"body\0\xff",$b->msgs[$id]['body']);Harness::eq('raw properties preserved',$raw,$b->msgs[$id]['propRaw']);Harness::eq('bridge registered shared consumer',1,$b->consumerCount('src'));Harness::eq('runtime says running','running',$i->status()[0]['state']);
 unset($b->parameters['shovel']['/']['move']);$i->tick();Harness::eq('delete removes consumer',0,$b->consumerCount('src'));Harness::eq('delete removes runtime',[],$i->status());$i->close();
});
Harness::guard('missing destination cannot consume and reject cannot lose source',static function():void{
 $b=igBroker();$b->declareQueue('src');$b->publish(0,0,0,'','src','safe',2);$b->flushDurable();igShovel($b,'move','src','dst');$i=new Integrations($b);$i->tick();Harness::eq('missing destination keeps source',1,$b->readyCount('src'));
 $b->declareQueue('dst',['x-max-length'=>0,'x-overflow'=>'reject-publish']);$i->tick();Harness::eq('reject requeues source',1,$b->readyCount('src'));Harness::eq('reject writes nothing',0,$b->readyCount('dst'));Harness::eq('failure status truthful','retrying',$i->status()[0]['state']);$i->close();
});
Harness::guard('cross vhost isolation self-loop protocol validation',static function():void{
 $b=igBroker();$b->vhosts[]='tenant';$tenant=$b->forVhost('tenant');$b->declareQueue('src');$b->declareQueue('dst');$tenant->declareQueue('dst');$b->publish(0,0,0,'','src','tenant body',2);$b->flushDurable();igShovel($b,'cross','src','dst','amqp://','amqp:///tenant');$i=new Integrations($b);$i->tick();Harness::eq('tenant receives','tenant body',$tenant->pullBody('dst'));Harness::eq('root isolated',0,$b->readyCount('dst'));
 igShovel($b,'self','src','src');$i->tick();$rows=array_column($i->status(),null,'name');Harness::eq('self consuming loop rejected','error',$rows['self']['state']);
 $b->parameters['shovel']['/']['unsupported']=['src-protocol'=>'amqp10','src-queue'=>'src','dest-queue'=>'dst'];$i->tick();$rows=array_column($i->status(),null,'name');Harness::eq('unsupported is not claimed running','error',$rows['unsupported']['state']);$i->close();
});
Harness::guard('local federation actually republishes exchange routing and bounds cycles',static function():void{
 $b=igBroker();$b->vhosts[]='up';$up=$b->forVhost('up');$up->declareExchange('events','topic');$b->declareExchange('events','topic');$b->declareQueue('observer');$b->bind('observer','events','order.#');
 $b->parameters['federation-upstream']['/']['up']=['uri'=>'amqp:///up'];$b->policies['/']['fed']=['pattern'=>'^events$','apply-to'=>'exchanges','definition'=>['federation-upstream'=>'up']];$i=new Integrations($b);$i->tick();
 $up->publish(0,0,0,'events','order.new',"event\0",2,0,[['origin','source']]);$up->flushDurable();$i->tick();$id=$b->getReady('observer');Harness::ok('federation body exists',$id!==null);Harness::eq('routing key preserved','order.new',$b->msgs[$id]['key']);Harness::eq('federation body preserved',"event\0",$b->msgs[$id]['body']);Harness::eq('hop count attached',1,array_column($b->msgs[$id]['headers'],1,0)['x-qf-federation-hops']);
 $up->publish(0,0,0,'events','order.loop','loop',2,0,[['x-qf-federation-hops',1]]);$up->flushDurable();$i->tick();Harness::eq('max hops stops forwarding',0,$b->readyCount('observer'));unset($b->policies['/']['fed']);$i->tick();Harness::eq('removed policy stops link',[],$i->status());$i->close();
});
Harness::guard('native endpoint class models confirms without optimistic source ACK',static function():void{
 // Offline unit gate exercises real codec frames; socket integration below is opt-in.
 $peer=new IntegrationPeer('amqp://admin:password@localhost:5672/%2f','dst',true);$reflect=new ReflectionClass($peer);$phase=$reflect->getProperty('phase');$phase->setValue($peer,'ready');$frame=$reflect->getMethod('frame');$result=null;
 $peer->publish('','dst',['body'=>'payload','mode'=>2],[],null,static function(bool$ok)use(&$result){$result=$ok;});Harness::eq('before confirm result pending',null,$result);$frame->invoke($peer,1,pack('nn',60,80).Codec::u64(1)."\0");Harness::eq('ACK confirms',true,$result);
 $result=null;$peer->publish('','missing',['body'=>'bad'],[],null,static function(bool$ok)use(&$result){$result=$ok;});$frame->invoke($peer,1,pack('nn',60,50).pack('n',312).Codec::shortstr('NO_ROUTE')."\0\0");$frame->invoke($peer,1,pack('nn',60,80).Codec::u64(2)."\0");Harness::eq('return overrides publisher ACK',false,$result);
 $result=null;$peer->publish('','dst',['body'=>'pending'],[],null,static function(bool$ok)use(&$result){$result=$ok;});$peer->close();Harness::eq('disconnect fails pending destination',false,$result);
});
if(getenv('QF_INTEGRATIONS_NETWORK')==='1')Harness::guard('real remote AMQP source destination credentials and confirms',static function():void{
 $server=Harness::broker();try{
  $b=igBroker();$b->declareQueue('src');$b->declareQueue('back');$b->publish(0,0,0,'','src',"remote\0body",2);$b->flushDurable();$uri='amqp://admin:devpassword12@127.0.0.1:'.$server['port'].'/%2f';igShovel($b,'out','src','remote','amqp://',$uri);$i=new Integrations($b);
  $end=microtime(true)+8;do{$i->tick();usleep(1000);}while($b->readyCount('src')>0&&microtime(true)<$end);Harness::eq('remote confirm settles local source',0,$b->readyCount('src'));
  igShovel($b,'back','remote','back',$uri,'amqp://');$end=microtime(true)+8;do{$i->tick();usleep(1000);}while($b->readyCount('back')===0&&microtime(true)<$end);Harness::eq('remote source consumed with real login',"remote\0body",$b->pullBody('back'));
  $b->declareQueue('auth-src');$b->publish(0,0,0,'','auth-src','keep',2);igShovel($b,'bad-auth','auth-src','blocked','amqp://',str_replace('devpassword12','wrong',$uri));$end=microtime(true)+1;do{$i->tick();usleep(1000);$rows=array_column($i->status(),null,'name');}while(($rows['bad-auth']['error']??'')===''&&microtime(true)<$end);Harness::eq('failed remote login retains local source',1,$b->readyCount('auth-src'));Harness::ok('bad remote login visible',($rows['bad-auth']['error']??'')!=='');
  $b->declareExchange('remote-events','topic');$b->declareQueue('fed-observer');$b->bind('fed-observer','remote-events','key.#');$b->parameters['federation-upstream']['/']['remote']=['uri'=>$uri];$b->policies['/']['fed']=['pattern'=>'^remote-events$','apply-to'=>'exchanges','definition'=>['federation-upstream'=>'remote']];$end=microtime(true)+5;do{$i->tick();usleep(1000);$rows=array_column($i->status(),null,'name');}while(($rows['remote:remote-events']['state']??'')!=='running'&&microtime(true)<$end);
  Harness::eq('remote federation handshake running','running',$rows['remote:remote-events']['state']??'absent');$client=new Amqp('127.0.0.1',$server['port']);$client->channel();$client->confirmSelect();$client->publish('remote-events','key.one',"federated\0body",['deliveryMode'=>2,'headers'=>['origin'=>'remote'],'contentType'=>'application/octet-stream']);$client->expect(60,80);$end=microtime(true)+5;do{$i->tick();usleep(1000);}while($b->readyCount('fed-observer')===0&&microtime(true)<$end);$id=$b->getReady('fed-observer');Harness::eq('remote federation binary delivery',"federated\0body",$b->msgs[$id]['body']??null);Harness::eq('remote federation routing survives','key.one',$b->msgs[$id]['key']??null);Harness::eq('remote federation properties survive','application/octet-stream',Amqp10::readProps($b->msgs[$id]['propRaw']??null)['contentType']??null);$client->close();$i->close();
 }finally{Harness::stop($server);}
});
Harness::done();
