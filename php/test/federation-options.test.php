<?php
declare(strict_types=1);
require_once __DIR__.'/lib/Harness.php';
foreach(['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Amqp10','Integrations']as$class)require_once dirname(__DIR__).'/src/'.$class.'.php';

/** A broker with an 'up' vhost whose 'events' exchange federates into the default vhost's 'events'. */
function foBroker(array $upstream, array $queueArgs = []): array
{
    $dir=sys_get_temp_dir().'/qf-fedopts-'.bin2hex(random_bytes(6));
    $b=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');$b->bootstrap('devpassword12');
    $b->vhosts[]='up';$up=$b->forVhost('up');$up->declareExchange('events','topic');
    $b->declareExchange('events','topic');$b->declareQueue('observer',$queueArgs);$b->bind('observer','events','#');
    $b->parameters['federation-upstream']['/']['up']=['uri'=>'amqp:///up']+$upstream;
    $b->policies['/']['fed']=['pattern'=>'^events$','apply-to'=>'exchanges','definition'=>['federation-upstream'=>'up']];
    $i=new Integrations($b);$i->tick();
    return [$b,$up,$i];
}
function foLink(Integrations $i): IntegrationLink { $links=(new ReflectionProperty($i,'links'))->getValue($i); return array_values($links)[0]; }
function foLinkQueue(Integrations $i): string { return (string) foLink($i)->definition['src-queue']; }

Harness::guard('trust-user-id drops or keeps user-id on forwarded messages',static function():void{
    foreach([false,true] as $trust){
        [$b,$up,$i]=foBroker(['trust-user-id'=>$trust]);
        $raw=Amqp10::writeProps(['userId'=>'admin','deliveryMode'=>2],[]);
        $up->publish(0,0,0,'events','k','with-user',2,0,[],null,$raw);$up->flushDurable();$i->tick();
        $id=$b->getReady('observer');
        Harness::ok('message forwarded (trust='.var_export($trust,true).')',$id!==null);
        $props=Amqp10::readProps($b->msgs[$id]['propRaw']??null);
        Harness::eq('user-id '.($trust?'kept':'dropped'),$trust?'admin':null,$props['userId']??null);
        $i->close();
    }
});

Harness::guard('prefetch-count allows many deliveries in flight',static function():void{
    $definition=foLink(foBroker([])[2])->definition;
    Harness::eq('default prefetch-count',1000,$definition['prefetch-count']);
    Harness::eq('default ack-mode','on-confirm',$definition['ack-mode']);
    Harness::eq('default reconnect-delay',5.0,$definition['reconnect-delay']);
    [$b,$up,$i]=foBroker(['prefetch-count'=>5]);
    $link=foLink($i);$limit=(new ReflectionProperty($link,'limit'))->getValue($link);
    Harness::eq('prefetch-count is the in-flight limit',5,$limit);
    for($n=0;$n<20;$n++)$up->publish(0,0,0,'events','k',"m$n",2);$up->flushDurable();$i->tick();
    Harness::eq('a tick forwards every ready message',20,$b->readyCount('observer'));
    $i->close();
});

Harness::guard('ack-mode decides when the upstream delivery is settled',static function():void{
    // The downstream queue refuses everything, so the downstream publish fails.
    $full=['x-max-length'=>0,'x-overflow'=>'reject-publish'];
    foreach(['on-confirm'=>1,'on-publish'=>0,'no-ack'=>0] as $mode=>$left){
        [$b,$up,$i]=foBroker(['ack-mode'=>$mode,'reconnect-delay'=>3],$full);
        $queue=foLinkQueue($i);
        if($mode==='no-ack')Harness::eq('no-ack consumes upstream without acks',true,$up->queues[$queue]['consumers'][0]['noAck']??false);
        $up->publish(0,0,0,'events','k','refused',2);$up->flushDurable();$i->tick();
        Harness::eq("$mode: upstream messages left after a failed downstream publish",$left,$up->readyCount($queue));
        Harness::eq("$mode: nothing reached the full queue",0,$b->readyCount('observer'));
        $link=foLink($i);$retry=(new ReflectionProperty($link,'retry'))->getValue($link);
        Harness::ok("$mode: reconnect-delay spaces the retry",$retry-microtime(true)>2.5&&$retry-microtime(true)<=3.01,(string)($retry-microtime(true)));
        $i->close();
    }
});

Harness::guard('a remote upstream consumes with prefetch-count and no-ack',static function():void{
    $peer=new IntegrationPeer('amqp://guest:guest@127.0.0.1:5672/%2f','qf-fed-x',false,['src-exchange'=>'events','exchange-type'=>'topic'],null,7,true,2.0);
    $r=new ReflectionClass($peer);$r->getProperty('phase')->setValue($peer,'start');$frame=$r->getMethod('frame');$output=$r->getProperty('output');
    $frame->invoke($peer,1,pack('nn',50,21));
    $qos=$output->getValue($peer);
    Harness::eq('basic.qos carries prefetch-count',pack('nnNnC',60,10,0,7,0),substr($qos,7,11));
    $output->setValue($peer,'');$frame->invoke($peer,1,pack('nn',60,11));
    $consume=$output->getValue($peer);$o=7+4+2;Codec::readShortstr($consume,$o);Codec::readShortstr($consume,$o);
    Harness::eq('basic.consume sets no-ack',2,ord($consume[$o])&2);
    $frame->invoke($peer,1,pack('nn',60,21));
    Harness::ok('consuming peer is ready',$peer->ready()&&$peer->consuming());
    $output->setValue($peer,'');$peer->settle(1,true);
    Harness::eq('no-ack peer never acks','',$output->getValue($peer));
    $peer->close();
    Harness::ok('reconnect-delay spaces the reconnect',$r->getProperty('retry')->getValue($peer)-microtime(true)>1.5);
});
Harness::done();
