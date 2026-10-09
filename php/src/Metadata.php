<?php
declare(strict_types=1);

/** Parse and validate metadata writes without changing live topology before commit. */
final class Metadata
{
    /** @return ?array{commands:list<array{kind:string,data:array}>,response:string,name?:string} */
    public static function amqp(Broker $broker, int $conn, int $channel, int $class, int $method, string $payload, string $user): ?array
    {
        if (!in_array([$class,$method], [[50,10],[50,20],[50,40],[50,50],[40,10],[40,20],[40,30],[40,40]], true)) return null;
        if (strlen($payload)<6 || unpack('nclass/nmethod',substr($payload,0,4))!==['class'=>$class,'method'=>$method]) self::fail(501,'malformed metadata method');
        $at=6; $name=self::short($payload,$at); $vhost=$broker->vhost;
        if ($class===50 && $method===10) {
            $bits=self::bits($payload,$at); if($bits&1)return null;
            $args=self::table($payload,$at); self::end($payload,$at);
            $durable=($bits&2)!==0;$exclusive=($bits&4)!==0;$autoDelete=($bits&8)!==0;$nowait=($bits&16)!==0;
            if($exclusive)return null;
            $generated=$name==='';if($generated)$name='amq.gen-'.bin2hex(random_bytes(8));
            self::allowed($broker,$user,'configure',$name);self::locked($broker,$conn,$name);
            $q=$broker->queues[$name]??null;self::queueArgs($args);
            if($q!==null){
                foreach(['durable'=>$durable,'exclusive'=>$exclusive,'autoDelete'=>$autoDelete] as $field=>$value)if(($q[$field]??($field==='durable'))!==$value)self::fail(406,"inequivalent arg $field for queue '$name'");
                self::sameArgs($q['declaredArgs']??[],$args,'queue');
                // An equivalent redeclare is a read; preserve its existing home and group.
                return ['commands'=>[], 'response'=>$nowait?'':Codec::queueDeclareOk($channel,$name,count($q['ready']??[]),count($q['consumers']??[])), 'name'=>$name];
            }
            if(!$generated&&str_starts_with($name,'amq.'))self::fail(403,"queue name '$name' is reserved");
            if(!$broker->queueAllowed($vhost))self::fail(403,'queue limit');
            $type=(string)($args['x-queue-type']??'classic');
            if(in_array($type,['quorum','stream'],true)&&!$durable)self::fail(406,'quorum and stream queues must be durable');
            if(!$durable&&!$broker->transientNonexcl)self::fail(541,'transient non-exclusive queues are disabled');
            $group=in_array($type,['quorum','stream'],true)?$broker->cluster?->queueGroup($vhost,$name):null;
            $home=$type==='classic'?(count($broker->members)<2?$broker->nodeId:Features::home($broker->members,$vhost,$name)):$broker->nodeId;
            $data=['vhost'=>$vhost,'name'=>$name,'durable'=>$durable,'exclusive'=>false,'autoDelete'=>$autoDelete,'auto_delete'=>$autoDelete,'args'=>$args,'home'=>$home,'raftGroup'=>$group];
            if($group!==null)$data['raftLeader']=self::leader($broker,$args['x-queue-leader-locator']??null);
            return self::staged('queue',$data,$nowait?'':Codec::queueDeclareOk($channel,$name,0,0),$name);
        }
        if ($class===40 && $method===10) {
            $kind=self::short($payload,$at);$bits=self::bits($payload,$at);if($bits&1)return null;
            $args=self::table($payload,$at);self::end($payload,$at);self::allowed($broker,$user,'configure',$name);
            if($name==='')return ['commands'=>[], 'response'=>($bits&16)?'':Codec::method($channel,40,11)];
            if(str_starts_with($name,'amq.'))self::fail(403,"exchange name '$name' is reserved");
            if(!in_array($kind,['direct','fanout','topic','headers','x-delayed-message','x-consistent-hash','x-local-random'],true))self::fail(503,"unknown exchange type '$kind'");
            $durable=($bits&2)!==0;$autoDelete=($bits&4)!==0;$internal=($bits&8)!==0;
            if(isset($args['alternate-exchange'])&&!is_string($args['alternate-exchange']))self::fail(406,'alternate-exchange must be a string');
            if($kind==='x-delayed-message'&&!in_array($args['x-delayed-type']??null,['direct','fanout','topic','headers'],true))self::fail(406,'x-delayed-type is required');
            if(isset($broker->exchanges[$name])){
                $row=$broker->exchangeRows[$name]??[];
                foreach(['type'=>$kind,'durable'=>$durable,'autoDelete'=>$autoDelete,'internal'=>$internal,'alternate'=>$args['alternate-exchange']??null] as $field=>$value){$existing=$field==='type'?$broker->exchanges[$name]:($row[$field]??($field==='durable'?true:($field==='alternate'?null:false)));if($existing!==$value)self::fail(406,"inequivalent arg $field for exchange '$name'");}
                self::sameArgs($row['arguments']??[],$args,'exchange');
                return ['commands'=>[], 'response'=>($bits&16)?'':Codec::method($channel,40,11)];
            }
            $data=['vhost'=>$vhost,'name'=>$name,'type'=>$kind,'kind'=>$kind,'durable'=>$durable,'autoDelete'=>$autoDelete,'auto_delete'=>$autoDelete,'internal'=>$internal,'alternate'=>$args['alternate-exchange']??null,'arguments'=>$args,'delayedType'=>$args['x-delayed-type']??null];
            return self::staged('exchange',$data,($bits&16)?'':Codec::method($channel,40,11));
        }
        if (($class===50&&in_array($method,[20,50],true))||($class===40&&in_array($method,[30,40],true))) {
            $source=self::short($payload,$at);$key=self::short($payload,$at);$unbind=($class===50&&$method===50)||($class===40&&$method===40);
            $nowait=$class===50&&$method===50?false:(self::bits($payload,$at)&1)!==0;
            $table=self::table($payload,$at);self::end($payload,$at);$args=[];foreach($table as $k=>$v)$args[]=[(string)$k,$v];
            self::allowed($broker,$user,'write',$name);self::allowed($broker,$user,'read',$source);
            if($source==='')self::fail(403,'cannot bind the default exchange');
            if(!isset($broker->exchanges[$source]))self::fail(404,"no exchange '$source'");
            if(($broker->exchanges[$source]??null)==='topic'&&!$broker->topicReadAllowed($user,$vhost,$source,$key))self::fail(403,'topic read permission denied');
            if($class===50){self::locked($broker,$conn,$name);if(!isset($broker->queues[$name]))self::fail(404,"no queue '$name'");if($broker->queues[$name]['exclusive']??false)return null;
                $data=['vhost'=>$vhost,'exchange'=>$source,'queue'=>$name,'routingKey'=>$key,'routing_key'=>$key,'args'=>$args];$response=Codec::method($channel,50,$unbind?51:21);
            }else{if($name===''||!isset($broker->exchanges[$name]))self::fail(404,"no exchange '$name'");$data=['vhost'=>$vhost,'source'=>$source,'destination'=>$name,'destinationType'=>'exchange','routingKey'=>$key,'routing_key'=>$key,'args'=>$args];$response=Codec::method($channel,40,$unbind?51:31);}
            return self::staged($unbind?'unbind':'binding',$data,$nowait?'':$response);
        }
        $bits=self::bits($payload,$at);self::end($payload,$at);self::allowed($broker,$user,'configure',$name);
        if($class===50){
            self::locked($broker,$conn,$name);$q=$broker->queues[$name]??null;if($q['exclusive']??false)return null;
            if(($bits&1)&&($q['consumers']??[])!==[])self::fail(406,"queue '$name' is in use");
            if(($bits&2)&&($q['ready']??[])!==[])self::fail(406,"queue '$name' is not empty");
            return self::staged('delete_queue',['vhost'=>$vhost,'name'=>$name,'queue'=>$name],($bits&4)?'':Codec::queueDeleteOk($channel,count($q['ready']??[])));
        }
        if($name===''||str_starts_with($name,'amq.'))self::fail(403,'cannot delete a reserved exchange');
        if(!isset($broker->exchanges[$name]))return ['commands'=>[], 'response'=>($bits&2)?'':Codec::method($channel,40,21)];
        if($bits&1){foreach($broker->bindings as $b)if($b['exchange']===$name)self::fail(406,"exchange '$name' is in use");foreach($broker->e2e as $b)if($b['source']===$name)self::fail(406,"exchange '$name' is in use");}
        return self::staged('delete_exchange',['vhost'=>$vhost,'name'=>$name],($bits&2)?'':Codec::method($channel,40,21));
    }
    private static function staged(string $kind,array $data,string $response,?string $name=null): array
    {
        $out=['commands'=>[['kind'=>$kind,'data'=>$data]],'response'=>$response];if($name!==null)$out['name']=$name;return $out;
    }
    private static function allowed(Broker $b,string $user,string $operation,string $name): void
    {
        if(!$b->resourceAllowed($user,$b->vhost,$operation,$name))self::fail(403,"$operation access to '$name' denied");
    }
    private static function locked(Broker $b,int $conn,string $name): void
    {
        $owner=$b->queues[$name]['owner']??null;if($owner!==null&&$owner!==$conn)self::fail(405,"queue '$name' is locked");
    }
    private static function queueArgs(array $args): void
    {
        if(array_key_exists('x-queue-type',$args)&&(!is_string($args['x-queue-type'])||!in_array($args['x-queue-type'],['classic','quorum','stream'],true)))self::fail(406,'invalid x-queue-type');
        if(!Features::knownQueueType((string)($args['x-queue-type']??'')))self::fail(406,'unsupported x-queue-type');
        foreach(['x-message-ttl','x-expires','x-max-length','x-max-length-bytes','x-max-priority','x-delivery-limit','x-max-age'] as $key)if(array_key_exists($key,$args)){
            if($key==='x-max-age'){if(!is_string($args[$key])||preg_match('/^[1-9][0-9]*[smhDMY]$/D',$args[$key])!==1)self::fail(406,'invalid x-max-age');}
            elseif(!is_int($args[$key])||$args[$key]<0||($key==='x-expires'&&$args[$key]===0)||($key==='x-max-priority'&&($args[$key]<1||$args[$key]>255)))self::fail(406,"invalid $key");
        }
        foreach(['x-dead-letter-exchange','x-dead-letter-routing-key'] as $key)if(array_key_exists($key,$args)&&!is_string($args[$key]))self::fail(406,"invalid $key");
        if(array_key_exists('x-single-active-consumer',$args)&&!is_bool($args['x-single-active-consumer']))self::fail(406,'invalid x-single-active-consumer');
        foreach(['x-overflow'=>['drop-head','reject-publish','reject-publish-dlx'],'x-queue-leader-locator'=>['client-local','balanced'],'x-dead-letter-strategy'=>['at-most-once','at-least-once']] as $key=>$values)if(array_key_exists($key,$args)&&!in_array($args[$key],$values,true))self::fail(406,"invalid $key");
    }
    private static function sameArgs(array $old,array $new,string $resource): void
    {
        foreach(array_unique([...array_keys($old),...array_keys($new)]) as $key){if($key==='x-queue-type'){$a=$old[$key]??'classic';$b=$new[$key]??'classic';}else{$a=$old[$key]??null;$b=$new[$key]??null;}if($a!==$b)self::fail(406,"inequivalent arg '$key' for $resource");}
    }
    private static function leader(Broker $b,mixed $locator): string
    {
        if($locator!=='balanced'||$b->cluster?->raft===null)return $b->nodeId;
        $counts=[];foreach($b->members as $m)$counts[$m['id']]=0;
        foreach($b->cluster->raft->groupNames() as $g){if(!str_starts_with($g,'q:'))continue;$id=$b->cluster->raft->leader($g);if(isset($counts[$id]))$counts[$id]++;}
        ksort($counts,SORT_STRING);asort($counts,SORT_NUMERIC);return (string)(array_key_first($counts)??$b->nodeId);
    }
    private static function short(string $payload,int &$at): string
    {
        $len=self::bits($payload,$at);if($at+$len>strlen($payload))self::fail(501,'truncated short string');$v=substr($payload,$at,$len);$at+=$len;return $v;
    }
    private static function bits(string $payload,int &$at): int
    {
        if(!isset($payload[$at]))self::fail(501,'truncated metadata method');return ord($payload[$at++]);
    }
    private static function table(string $payload,int &$at): array
    {
        if($at+4>strlen($payload))self::fail(501,'truncated argument table');$size=unpack('N',substr($payload,$at,4))[1];if($at+4+$size>strlen($payload))self::fail(501,'truncated argument table');
        return self::collection($payload,$at,false,0);
    }
    /** Strict field parsing keeps malformed nested tables out of the committed log. */
    private static function collection(string $raw,int &$at,bool $array,int $depth): array
    {
        if($depth>32)self::fail(501,'argument table nesting limit');
        $size=unpack('N',self::bytes($raw,$at,4))[1];$end=$at+$size;if($end>strlen($raw))self::fail(501,'truncated field collection');$values=[];
        while($at<$end){$key=$array?count($values):self::short($raw,$at);$type=chr(self::bits($raw,$at));$value=self::field($raw,$at,$type,$depth+1);if($at>$end)self::fail(501,'field exceeds argument table');$values[$key]=$value;}
        return $values;
    }
    private static function field(string $raw,int &$at,string $type,int $depth): mixed
    {
        if($type==='V')return null;
        if($type==='t')return self::bits($raw,$at)!==0;
        if($type==='S'||$type==='x'){$n=unpack('N',self::bytes($raw,$at,4))[1];return self::bytes($raw,$at,$n);}
        if($type==='F'||$type==='A')return self::collection($raw,$at,$type==='A',$depth);
        if(in_array($type,['l','L','T'],true))return Codec::readU64(self::bytes($raw,$at,8),0);
        if($type==='f')return unpack('G',self::bytes($raw,$at,4))[1];
        if($type==='d')return unpack('E',self::bytes($raw,$at,8))[1];
        if($type==='D'){$scale=self::bits($raw,$at);return unpack('N',self::bytes($raw,$at,4))[1]/(10**$scale);}
        $width=match($type){'b','B'=>1,'s','U','u'=>2,'I','i'=>4,default=>0};if($width===0)self::fail(501,'unsupported field type');
        $bytes=self::bytes($raw,$at,$width);$n=$width===1?ord($bytes):unpack($width===2?'n':'N',$bytes)[1];
        if(in_array($type,['b','s','U','I'],true)&&$n>=(1<<($width*8-1)))$n-=1<<($width*8);return $n;
    }
    private static function bytes(string $raw,int &$at,int $size): string
    {
        if($size<0||$size>strlen($raw)-$at)self::fail(501,'truncated argument field');$bytes=substr($raw,$at,$size);$at+=$size;return $bytes;
    }
    private static function end(string $payload,int $at): void {if($at!==strlen($payload))self::fail(501,'trailing metadata bytes');}
    private static function fail(int $code,string $text): never {throw new RuntimeException(($code===403?'ACCESS_REFUSED':($code===404?'NOT_FOUND':($code===405?'RESOURCE_LOCKED':($code===406?'PRECONDITION_FAILED':'FRAME_ERROR')))).' - '.$text,$code);}
}
