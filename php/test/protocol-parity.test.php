<?php
declare(strict_types=1);
require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Cluster','Broker','Amqp10','Protocols'] as $class) require_once dirname(__DIR__) . '/src/' . $class . '.php';

function ppBroker(?string $dir = null): Broker {
    $dir ??= sys_get_temp_dir() . '/qf-protocol-parity-' . bin2hex(random_bytes(6));
    $b = new Broker(new Store($dir . '/messages.log'), $dir . '/users.json');
    $b->bootstrap('devpassword12'); return $b;
}
function ppStr(string $s): string { return pack('n', strlen($s)) . $s; }
function ppMqtt(int $first, string $body): string {
    $n = strlen($body); $len = ''; do { $byte=$n%128; $n=intdiv($n,128);$len.=chr($n?$byte|128:$byte); } while($n);
    return chr($first).$len.$body;
}
function ppConnect(int $v=4,string $id='client',bool $clean=true,string $user='admin',string $pass='devpassword12',?array $will=null):string {
    $flags=0xc0|($clean?2:0)|($will?4|8:0);
    return ppMqtt(0x10, ppStr('MQTT').chr($v).chr($flags).pack('n',0).($v===5?"\0":'').ppStr($id).($will?($v===5?"\0":'').ppStr($will[0]).ppStr($will[1]):'').ppStr($user).ppStr($pass));
}
function ppFrame(int $key,string $body,int $v=1):string{$p=pack('nn',$key,$v).$body;return pack('N',strlen($p)).$p;}
function ppFrames(string $bytes):array{$out=[];while(strlen($bytes)>=4){$n=unpack('N',substr($bytes,0,4))[1];$out[]=['key'=>unpack('n',substr($bytes,4,2))[1],'body'=>substr($bytes,8,$n-4)];$bytes=substr($bytes,$n+4);}return$out;}
function ppStream(Protocols $p,array &$state,int $key,string $body,int $v=1):array{$buf=ppFrame($key,$body,$v);return ppFrames($p->stream($buf,$state));}
function ppStreamOpen(Protocols $p,array &$s):void {
    $auth="\0admin\0devpassword12"; $r=ppStream($p,$s,19,pack('N',1).ppStr('PLAIN').pack('N',strlen($auth)).$auth);
    Harness::eq('stream auth real credentials',1,unpack('n',substr($r[0]['body'],4,2))[1]);ppStream($p,$s,21,pack('N',2).ppStr('/'));
}
function ppStomp(Protocols $p,int $conn,string $frame,$fp):string{$buf=$frame;return$p->stomp($buf,$fp,$conn);}
function ppStompLogin(Protocols $p,int $conn,$fp):void {Harness::ok('STOMP authenticated',str_contains(ppStomp($p,$conn,"CONNECT\naccept-version:1.2\nlogin:admin\npasscode:devpassword12\nhost:/\n\n\0",$fp),'CONNECTED'));}

Harness::guard('MQTT authentication and MQTT5 binary QoS1 cross protocol',static function():void{
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect(5,'bad',true,'admin','wrong');
    Harness::eq('MQTT5 rejects bad login',"\x20\x03\0\x86\0",$p->mqtt($buf,$fp,1));$p->mqttClosing=false;
    $packet=ppConnect(5);$buf=substr($packet,0,7);Harness::eq('partial connect buffered','',$p->mqtt($buf,$fp,2));$buf.=substr($packet,7);Harness::eq('MQTT5 connack',"\x20\x03\0\0\0",$p->mqtt($buf,$fp,2));
    $b->declareQueue('amqp-observer');$b->bind('amqp-observer','amq.topic','a.b');
    $buf=ppMqtt(0x32,ppStr('a/b').pack('n',21)."\0"."binary\0body");Harness::eq('QoS1 publish ack',"\x40\x02\0\x15",$p->mqtt($buf,$fp,2));
    $id=$b->getReady('amqp-observer');Harness::eq('MQTT routes binary to AMQP','binary'."\0".'body',$b->msgs[$id]['body']);Harness::eq('MQTT QoS1 stored durable',2,$b->msgs[$id]['mode']);
    fclose($fp);
});
Harness::guard('MQTT session retained will and AMQP inbound',static function():void{
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect(4,'persistent',false);$p->mqtt($buf,$fp,1);
    $buf=ppMqtt(0x82,pack('n',1).ppStr('events/#').chr(1));Harness::eq('MQTT QoS1 granted',"\x90\x03\0\1\1",$p->mqtt($buf,$fp,1));
    $p->dropMqtt(1,false);$b->publish(0,0,0,'amq.topic','events.one','offline',2);$b->flushDurable();
    $buf=ppConnect(4,'persistent',false);$out=$p->mqtt($buf,$fp,2);Harness::ok('session present',str_starts_with($out,"\x20\x02\1\0"));Harness::ok('offline delivery',str_contains($out,'offline'));Harness::eq('QoS1 waiting for ack',1,count($b->msgs));
    $buf="\x40\x02\0\1";$p->mqtt($buf,$fp,2);Harness::eq('PUBACK settles',0,count($b->msgs));
    $buf=ppMqtt(0x33,ppStr('events/retained').pack('n',3).'remember');$p->mqtt($buf,$fp,2);
    $p2=new Protocols($b);$fp2=fopen('php://temp','w+');$buf=ppConnect(4,'retained-reader');$p2->mqtt($buf,$fp2,3);$buf=ppMqtt(0x82,pack('n',4).ppStr('events/#').chr(0));$out=$p2->mqtt($buf,$fp2,3);Harness::ok('retained survives adapter restart',str_contains($out,'remember'));
    $b->declareQueue('will-observer');$b->bind('will-observer','amq.topic','events.will');$buf=ppConnect(4,'will-client',true,'admin','devpassword12',['events/will','goodbye']);$p2->mqtt($buf,$fp2,4);$p2->dropMqtt(4);Harness::eq('abnormal close publishes will','goodbye',$b->pullBody('will-observer'));
    $buf=ppConnect(4,'graceful',true,'admin','devpassword12',['events/will','unexpected']);$p2->mqtt($buf,$fp2,5);$buf="\xe0\0";$p2->mqtt($buf,$fp2,5);Harness::eq('graceful suppresses will',null,$b->pullBody('will-observer'));
    fclose($fp);fclose($fp2);
});
Harness::guard('STOMP binary framing properties transactions ack nack',static function():void{
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');ppStompLogin($p,1,$fp);
    ppStomp($p,1,"SUBSCRIBE\nid:s\ndestination:/queue/work\nack:client-individual\nprefetch-count:1\n\n\0",$fp);
    ppStomp($p,1,"BEGIN\ntransaction:t\n\n\0",$fp);
    $body="a\0b\nc";$send="SEND\ndestination:/queue/work\ntransaction:t\ncontent-length:5\ncontent-type:application/octet-stream\ncustom:a\\cb\ncustom:ignored\n\n".$body."\0";
    $buf=substr($send,0,-2);Harness::eq('STOMP fragmented binary waits','',$p->stomp($buf,$fp,1));$buf.=substr($send,-2);$p->stomp($buf,$fp,1);Harness::eq('transaction holds send',0,$b->readyCount('work'));
    $out=ppStomp($p,1,"COMMIT\ntransaction:t\nreceipt:done\n\n\0",$fp);Harness::ok('commit sends binary MESSAGE',str_contains($out,$body));Harness::ok('first escaped header wins',str_contains($out,'custom:a\\cb'));Harness::ok('content type survives',str_contains($out,'content-type:application/octet-stream'));
    preg_match('/\nack:([^\n]+)/',$out,$m);$ack=$m[1];$out=ppStomp($p,1,"NACK\nid:$ack\nrequeue:true\n\n\0",$fp);Harness::ok('NACK redelivery',str_contains($out,'redelivered:true'));preg_match('/\nack:([^\n]+)/',$out,$m);
    ppStomp($p,1,"ACK\nid:{$m[1]}\n\n\0",$fp);Harness::eq('ACK removes broker body',0,count($b->msgs));
    ppStomp($p,1,"BEGIN\ntransaction:a\n\n\0",$fp);ppStomp($p,1,"SEND\ndestination:/queue/work\ntransaction:a\n\nnever\0",$fp);ppStomp($p,1,"ABORT\ntransaction:a\n\n\0",$fp);Harness::eq('ABORT discards body',0,count($b->msgs));fclose($fp);
});
Harness::guard('STOMP MQTT and AMQP share topic routing and enforce permissions',static function():void{
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');ppStompLogin($p,1,$fp);ppStomp($p,1,"SUBSCRIBE\nid:topic\ndestination:/topic/cross.*\n\n\0",$fp);
    $buf=ppConnect(4,'cross');$p->mqtt($buf,$fp,2);$buf=ppMqtt(0x30,ppStr('cross/one').'shared');$p->mqtt($buf,$fp,2);$p->tick();rewind($fp);Harness::ok('MQTT reaches STOMP queue',str_contains(stream_get_contents($fp),'shared'));
    $b->putUser('limited','password12',['management']);$b->setPermissions('limited','/','^none$','^none$','^none$');
    $out=ppStomp($p,3,"CONNECT\naccept-version:1.2\nlogin:limited\npasscode:password12\nhost:/\n\n\0",$fp);Harness::ok('limited authenticated',str_contains($out,'CONNECTED'));
    $out=ppStomp($p,3,"SEND\ndestination:/topic/cross.one\n\nblocked\0",$fp);Harness::ok('limited write refused',str_contains($out,'ERROR'));fclose($fp);
});
Harness::guard('Stream auth durable dedup offsets credit metadata codec',static function():void{
    $b=ppBroker();$p=new Protocols($b);$state=[];$bad=[];$auth="\0admin\0wrong";$r=ppStream($p,$bad,19,pack('N',1).ppStr('PLAIN').pack('N',strlen($auth)).$auth);Harness::eq('stream bad password',8,unpack('n',substr($r[0]['body'],4,2))[1]);$p->streamClosing=false;
    ppStreamOpen($p,$state);$r=ppStream($p,$state,13,pack('N',3).ppStr('log').pack('N',0));Harness::eq('create real stream queue',1,unpack('n',substr($r[0]['body'],4,2))[1]);
    ppStream($p,$state,1,pack('NC',4,1).ppStr('named').ppStr('log'));
    $entry="\0\x53\x75\xa0\x03a\0b";$publish=chr(1).pack('N',1).pack('J',4294967297).pack('N',strlen($entry)).$entry;$r=ppStream($p,$state,2,$publish);Harness::eq('publish confirms exact 64 bit id',pack('J',4294967297),substr($r[0]['body'],5,8));
    ppStream($p,$state,2,$publish);Harness::eq('named publisher deduplicates',1,$b->streamNext('log'));Harness::eq('AMQP1 data decoded','a'."\0".'b',$b->streamRead('log',0,1)[0]['body']);
    $b->streamAppend('log','amqp-inbound');$b->flushDurable();
    $r=ppStream($p,$state,7,pack('NC',5,1).ppStr('log').pack('nJ',4,0).pack('n',1).pack('N',0));Harness::eq('subscribe success',1,unpack('n',substr($r[0]['body'],4,2))[1]);Harness::eq('initial credit delivers one',2,count($r));Harness::eq('chunk first offset zero',0,unpack('J',substr($r[1]['body'],25,8))[1]);
    $r=ppStream($p,$state,9,pack('Cn',1,1));Harness::eq('credit resumes at offset one',1,count($r));Harness::ok('AMQP inbound encoded data',str_contains($r[0]['body'],'amqp-inbound'));
    ppStream($p,$state,10,ppStr('consumer').ppStr('log').pack('J',1));$r=ppStream($p,$state,11,pack('N',6).ppStr('consumer').ppStr('log'));Harness::eq('stored offset queried',1,unpack('J',substr($r[0]['body'],6,8))[1]);
    $restored=ppBroker($b->dataDir());Harness::eq('dedup survives durable replay',4294967297,$restored->streamPublisherSequence('log','named'));Harness::eq('offset survives durable replay',1,$restored->streamStoredOffset('log','consumer'));Harness::eq('stream records survive durable replay',2,$restored->streamNext('log'));
    $r=ppStream($p,$state,15,pack('N',7).pack('N',1).ppStr('log'));Harness::eq('metadata response wire key',0x800f,$r[0]['key']);
});
Harness::guard('Stream superstream routing and single active consumer handshake',static function():void{
    $b=ppBroker();$p=new Protocols($b);$s=[];ppStreamOpen($p,$s);
    $r=ppStream($p,$s,29,pack('N',3).ppStr('super').pack('N',2).ppStr('super-0').ppStr('super-1').pack('N',2).ppStr('0').ppStr('1').pack('N',0));Harness::eq('super stream created',1,unpack('n',substr($r[0]['body'],4,2))[1]);
    $r=ppStream($p,$s,24,pack('N',4).ppStr('1').ppStr('super'));Harness::ok('route finds partition',str_contains($r[0]['body'],'super-1'));
    $r=ppStream($p,$s,7,pack('NC',5,1).ppStr('super-0').pack('nn',1,1).pack('N',2).ppStr('single-active-consumer').ppStr('true').ppStr('name').ppStr('group'));
    Harness::eq('SAC emits consumer update',26,$r[1]['key']);$corr=unpack('N',substr($r[1]['body'],0,4))[1];$b->streamAppend('super-0','active');
    $r=ppStream($p,$s,0x801a,pack('Nnn',$corr,1,1));Harness::eq('SAC client response activates delivery',8,$r[0]['key']);
});
Harness::guard('Virtual host isolation across MQTT STOMP and stream', static function(): void {
    $b=ppBroker();$b->vhosts[]='isolated';$b->saveTopology();$child=$b->forVhost('isolated');
    $p=new Protocols($b);$fp=fopen('php://temp','w+');$child->declareQueue('observer');$child->bind('observer','amq.topic','isolated.topic');
    $b->declareQueue('observer');$b->bind('observer','amq.topic','isolated.topic');
    $buf=ppConnect(4,'tenant',true,'isolated:admin');$out=$p->mqtt($buf,$fp,1);Harness::eq('MQTT isolated login',"\x20\x02\0\0",$out);
    $buf=ppMqtt(0x30,ppStr('isolated/topic').'tenant');$p->mqtt($buf,$fp,1);
    Harness::eq('MQTT child vhost routing','tenant',$child->pullBody('observer'));Harness::eq('MQTT root vhost untouched',null,$b->pullBody('observer'));
    $out=ppStomp($p,2,"CONNECT\naccept-version:1.2\nlogin:admin\npasscode:devpassword12\nhost:isolated\n\n\0",$fp);Harness::ok('STOMP tenant login',str_contains($out,'CONNECTED'));
    ppStomp($p,2,"SEND\ndestination:/queue/work\n\ntenant-work\0",$fp);Harness::eq('STOMP child queue','tenant-work',$child->pullBody('work'));Harness::ok('STOMP root absent',!isset($b->queues['work']));
    $state=[];$auth="\0admin\0devpassword12";ppStream($p,$state,19,pack('N',1).ppStr('PLAIN').pack('N',strlen($auth)).$auth);ppStream($p,$state,21,pack('N',2).ppStr('isolated'));
    ppStream($p,$state,13,pack('N',3).ppStr('tenant-log').pack('N',0));Harness::ok('stream child queue',isset($child->queues['tenant-log']));Harness::ok('stream root absent',!isset($b->queues['tenant-log']));fclose($fp);
});
Harness::guard('MQTT persistent session restores across complete broker restart', static function(): void {
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect(4,'restart',false);$p->mqtt($buf,$fp,1);$buf=ppMqtt(0x82,pack('n',1).ppStr('restart/#').chr(1));$p->mqtt($buf,$fp,1);$p->dropMqtt(1,false);
    $b->publish(0,0,0,'amq.topic','restart.event','persisted',2);$b->flushDurable();$restored=ppBroker($b->dataDir());$p2=new Protocols($restored);$buf=ppConnect(4,'restart',false);$out=$p2->mqtt($buf,$fp,2);
    Harness::ok('restarted MQTT session present',str_starts_with($out,"\x20\x02\1\0"));Harness::ok('restarted MQTT offline payload',str_contains($out,'persisted'));fclose($fp);
});
Harness::guard('Stream offsets CRC unknown publishers and raw body sections', static function(): void {
    $b=ppBroker();$p=new Protocols($b);$s=[];ppStreamOpen($p,$s);ppStream($p,$s,13,pack('N',3).ppStr('wire').pack('N',0));$b->streamAppend('wire','first');$b->streamAppend('wire','last');
    $r=ppStream($p,$s,7,pack('NC',4,1).ppStr('wire').pack('nnN',2,1,0));Harness::ok('last offset chooses last body',str_contains($r[1]['body'],'last'));Harness::eq('last offset is one',1,unpack('J',substr($r[1]['body'],25,8))[1]);
    $wire=$r[1]['body'];$crc=unpack('N',substr($wire,33,4))[1];$length=unpack('N',substr($wire,37,4))[1];Harness::eq('delivery CRC covers entry data',$crc,crc32(substr($wire,49,$length)));Harness::eq('chunk data length matches frame',strlen($wire)-49,$length);
    $data="\0\x53\x75\xa0\x01x";$r=ppStream($p,$s,2,chr(99).pack('N',1).pack('J',7).pack('N',strlen($data)).$data);Harness::eq('undeclared publisher emits error',4,$r[0]['key']);Harness::eq('unknown publisher error code',18,unpack('n',substr($r[0]['body'],13,2))[1]);
    ppStream($p,$s,1,pack('NC',5,1).ppStr('').ppStr('wire'));$sections="\0\x53\x77\xa1\x05value";ppStream($p,$s,2,chr(1).pack('N',1).pack('J',8).pack('N',strlen($sections)).$sections);
    $record=$b->streamRead('wire',2,1)[0];Harness::eq('amqp-value stored verbatim',$sections,$record['body']);$r=ppStream($p,$s,7,pack('NC',6,2).ppStr('wire').pack('nJ',4,2).pack('nN',1,0));Harness::ok('amqp-value delivered verbatim',str_contains($r[1]['body'],$sections));
});
Harness::guard('MQTT5 binary properties survive broker restart', static function(): void {
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect(5,'binary-props',false);$p->mqtt($buf,$fp,1);
    $buf=ppMqtt(0x82,pack('n',1)."\0".ppStr('binary/#').chr(1));$p->mqtt($buf,$fp,1);
    $props="\x09\x00\x02\xff\x00";$buf=ppMqtt(0x32,ppStr('binary/event').pack('n',2).chr(strlen($props)).$props.'survives');$p->mqtt($buf,$fp,1);$p->dropMqtt(1,false);
    $restored=ppBroker($b->dataDir());$p2=new Protocols($restored);$buf=ppConnect(5,'binary-props',false);$out=$p2->mqtt($buf,$fp,2);Harness::ok('MQTT5 binary props restored exactly',str_contains($out,$props.'survives'));fclose($fp);
});
Harness::guard('Stream publishing IDs use unsigned 64 bit ordering', static function(): void {
    $b=ppBroker();$p=new Protocols($b);$s=[];ppStreamOpen($p,$s);ppStream($p,$s,13,pack('N',3).ppStr('unsigned').pack('N',0));ppStream($p,$s,1,pack('NC',4,1).ppStr('unsigned-reference').ppStr('unsigned'));
    foreach ([PHP_INT_MAX, PHP_INT_MIN, PHP_INT_MIN+1] as $sequence) {
        $data="\0\x53\x75\xa0\x01x";$r=ppStream($p,$s,2,chr(1).pack('N',1).pack('J',$sequence).pack('N',strlen($data)).$data);Harness::eq('full publishing ID echoed',pack('J',$sequence),substr($r[0]['body'],5,8));
    }
    Harness::eq('unsigned increasing IDs each append',3,$b->streamNext('unsigned'));
    $data="\0\x53\x75\xa0\x01x";ppStream($p,$s,2,chr(1).pack('N',1).pack('J',PHP_INT_MAX).pack('N',strlen($data)).$data);Harness::eq('older unsigned ID deduplicates',3,$b->streamNext('unsigned'));
});
Harness::guard('Unroutable nonmandatory MQTT and STOMP publish still accepted', static function(): void {
    $b=ppBroker();$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect();$p->mqtt($buf,$fp,1);$buf=ppMqtt(0x32,ppStr('nobody/listening').pack('n',7).'unrouted');Harness::eq('MQTT no subscriber QoS1 ACK',"\x40\x02\0\7",$p->mqtt($buf,$fp,1));
    ppStompLogin($p,2,$fp);$out=ppStomp($p,2,"SEND\ndestination:/topic/nobody.listening\npersistent:true\nreceipt:accepted\n\nunrouted\0",$fp);Harness::ok('STOMP unrouted SEND receipt',str_contains($out,'receipt-id:accepted'));Harness::ok('STOMP unrouted SEND no error',!str_contains($out,'ERROR'));fclose($fp);
});
Harness::guard('Shared broker consumer counts SAC takeover and cumulative ACK', static function(): void {
    $b=ppBroker();$b->declareQueue('sac',['x-single-active-consumer'=>'true']);$p=new Protocols($b);$a=fopen('php://temp','w+');$z=fopen('php://temp','w+');ppStompLogin($p,1,$a);ppStompLogin($p,2,$z);
    ppStomp($p,1,"SUBSCRIBE\nid:a\ndestination:/amq/queue/sac\n\n\0",$a);ppStomp($p,2,"SUBSCRIBE\nid:z\ndestination:/amq/queue/sac\n\n\0",$z);Harness::eq('STOMP consumers registered centrally',2,$b->consumerCount('sac'));
    $b->publish(0,0,0,'','sac','only-active',1);$b->flushDurable();$p->tick();rewind($a);rewind($z);Harness::ok('SAC first consumer delivers',str_contains(stream_get_contents($a),'only-active'));Harness::eq('SAC standby silent','',stream_get_contents($z));
    ppStomp($p,1,"UNSUBSCRIBE\nid:a\n\n\0",$a);Harness::eq('unsubscribe updates broker count',1,$b->consumerCount('sac'));$b->publish(0,0,0,'','sac','takeover',1);$b->flushDurable();$p->tick();rewind($z);Harness::ok('SAC standby takes over',str_contains(stream_get_contents($z),'takeover'));
    ppStomp($p,2,"SUBSCRIBE\nid:c\ndestination:/queue/cumulative\nack:client\n\n\0",$z);ppStomp($p,2,"SEND\ndestination:/queue/cumulative\n\none\0",$z);$out=ppStomp($p,2,"SEND\ndestination:/queue/cumulative\n\ntwo\0",$z);preg_match('/\nack:([^\n]+)/',$out,$match);Harness::eq('two unacked bodies remain',2,count($b->msgs));ppStomp($p,2,"ACK\nid:{$match[1]}\n\n\0",$z);Harness::eq('client ACK settles cumulative bodies',0,count($b->msgs));
    $p->dropStomp(2);Harness::eq('disconnect removes broker consumers',0,$b->consumerCount('sac'));fclose($a);fclose($z);
});
Harness::guard('OAuth credential identities and empty-login protocol authentication', static function(): void {
    $b=ppBroker();$key=openssl_pkey_new(['private_key_bits'=>2048,'private_key_type'=>OPENSSL_KEYTYPE_RSA]);if($key===false)throw new RuntimeException('RSA key generation failed');$details=openssl_pkey_get_details($key);$public=openssl_pkey_get_public($details['key']);
    // Supply the JWKS cache fixture; signature verification and claims/scopes remain real.
    $url='http://jwks.fixture.invalid/'.bin2hex(random_bytes(6));$cache=new ReflectionProperty(Security::class,'jwks');$cache->setValue(null,[$url."\0"=>['at'=>microtime(true),'keys'=>['protocol-key'=>$public]]]);$b->authConfig=['oauth'=>['jwksUrl'=>$url,'resourceServerId'=>'rabbitmq']];
    $b64=static fn(string $s):string=>rtrim(strtr(base64_encode($s),'+/','-_'),'=');$token=static function(string $scope)use($key,$b64):string{$h=$b64(json_encode(['alg'=>'RS256','kid'=>'protocol-key']));$c=$b64(json_encode(['sub'=>'oauth-user','exp'=>time()+3600,'aud'=>'rabbitmq','scope'=>$scope]));openssl_sign($h.'.'.$c,$signature,$key,OPENSSL_ALGO_SHA256);return$h.'.'.$c.'.'.$b64($signature);};
    $wide=$token('rabbitmq.configure:%2F/* rabbitmq.write:%2F/* rabbitmq.read:%2F/*');$read=$token('rabbitmq.read:%2F/*');$p=new Protocols($b);$fp=fopen('php://temp','w+');$buf=ppConnect(5,'oauth-wide',true,'',$wide);Harness::eq('MQTT empty username OAuth login',"\x20\x03\0\0\0",$p->mqtt($buf,$fp,1));
    $buf=ppConnect(5,'oauth-read',true,'',$read);Harness::eq('second OAuth credential login',"\x20\x03\0\0\0",$p->mqtt($buf,$fp,2));$b->declareQueue('oauth-observer');$b->bind('oauth-observer','amq.topic','oauth.one');
    $buf=ppMqtt(0x32,ppStr('oauth/one').pack('n',1)."\0".'allowed');Harness::eq('first OAuth credential retains write',"\x40\x02\0\1",$p->mqtt($buf,$fp,1));Harness::eq('OAuth publication routes','allowed',$b->pullBody('oauth-observer'));
    $buf=ppMqtt(0x32,ppStr('oauth/one').pack('n',2)."\0".'blocked');Harness::eq('second credential retains restricted scope',"\x40\x04\0\2\x87\0",$p->mqtt($buf,$fp,2));Harness::eq('denied OAuth publish absent',null,$b->pullBody('oauth-observer'));
    $buf=ppConnect(5,'impersonation',true,'admin',$wide);Harness::eq('OAuth cannot impersonate internal admin',"\x20\x03\0\x86\0",$p->mqtt($buf,$fp,3));$p->mqttClosing=false;
    $out=ppStomp($p,4,"CONNECT\naccept-version:1.2\nlogin:\npasscode:$wide\nhost:/\n\n\0",$fp);Harness::ok('STOMP empty login OAuth connected',str_contains($out,'CONNECTED'));
    $state=[];$auth="\0\0".$wide;$r=ppStream($p,$state,19,pack('N',1).ppStr('PLAIN').pack('N',strlen($auth)).$auth);Harness::eq('stream empty login OAuth authenticated',1,unpack('n',substr($r[0]['body'],4,2))[1]);$r=ppStream($p,$state,21,pack('N',2).ppStr('/'));Harness::eq('OAuth stream vhost authorized',1,unpack('n',substr($r[0]['body'],4,2))[1]);fclose($fp);
});
Harness::done();
