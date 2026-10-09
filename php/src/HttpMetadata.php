<?php
declare(strict_types=1);

/** Management metadata is validated against an isolated topology before any Raft proposal. */
final class HttpMetadata
{
    public static function stage(Broker $root,string $method,string $path,array $json):?array
    {
        $url=parse_url($path);$path=$url['path']??$path;$query=[];parse_str($url['query']??'',$query);
        if(!in_array($method,['PUT','POST','DELETE'],true))return null;
        if(!preg_match('~^/api/(users|vhosts|permissions|topic-permissions|policies|operator-policies|user-limits|vhost-limits|parameters|global-parameters|queues|exchanges|bindings|definitions)(/|$)~',$path))return null;
        if(preg_match('~/(publish|get|purge|contents)$~',$path)||array_key_exists('tracing',$json))return null;
        if(preg_match('~^/api/queues/([^/]+)/([^/]+)$~',$path,$m)&&$method==='DELETE'){
            $scope=$root->forVhost(rawurldecode($m[1]));$name=rawurldecode($m[2]);
            if(($query['if-empty']??'false')==='true'&&$scope->readyCount($name)>0)self::fail('queue is not empty',406);
            if(($query['if-unused']??'false')==='true'&&$scope->consumerCount($name)>0)self::fail('queue is in use',406);
        }
        $dir=sys_get_temp_dir().'/qf-http-stage-'.bin2hex(random_bytes(12));$scratch=null;
        try{
            $scratch=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');
            $scratch->installRaftState('meta',$root->raftState('meta'));$scratch->nodeId=$root->nodeId;$scratch->members=$root->members;$scratch->transientNonexcl=$root->transientNonexcl;
            // Snapshots intentionally omit node-local shovels; preserve their validation context.
            $scratch->parameters=$root->parameters;$scratch->policies=$root->policies;$scratch->operatorPolicies=$root->operatorPolicies;
            foreach($root->allBrokers()as$host=>$scope)if(isset($scope->exchanges['']))$scratch->forVhost($host)->exchanges['']=$scope->exchanges[''];
            foreach($root->allBrokers()as$host=>$scope)foreach($scope->queues as$name=>$queue)if(isset($scratch->forVhost($host)->queues[$name]))$scratch->forVhost($host)->queues[$name]['declaredArgs']=$queue['declaredArgs']??[];
            $admin='qf-stage-'.bin2hex(random_bytes(8));$scratch->users[$admin]=Auth::hash('stage-password');$scratch->tags[$admin]=['administrator'];
            $commands=[];$code=$method==='DELETE'?204:201;$body='';
            if($method==='POST'&&preg_match('~^/api/definitions(?:/([^/]+))?$~',$path,$m)){
                self::definitions($scratch,$root,$json,isset($m[1])?rawurldecode($m[1]):null,$admin,$commands);$code=204;
            }else{
                $handled=self::request($scratch,$root,$method,$path,$json,$admin,$commands,$code,$body);
                if(!$handled)return null;
            }
            return ['commands'=>$commands,'response'=>self::response($code,$body)];
        }finally{
            unset($scratch);gc_collect_cycles();self::remove($dir);
        }
    }
    private static function emit(Broker $scratch,string $kind,array $data,array &$commands):void
    {
        $scratch->applyRaft('meta',$kind,$data,0);$commands[]=['kind'=>$kind,'data'=>$data];
    }
    private static function request(Broker $b,Broker $live,string $method,string $path,array $j,string $admin,array &$commands,int &$code,string &$body):bool
    {
        if($method==='POST'&&preg_match('~^/api/bindings/([^/]+)$~',$path,$m)){
            $type=$j['destination_type']??'queue';if(!in_array($type,['queue','exchange'],true))self::fail('invalid binding destination type');
            $path='/api/bindings/'.$m[1].'/e/'.rawurlencode(self::string($j['source']??'')).'/'.($type==='queue'?'q':'e').'/'.rawurlencode(self::string($j['destination']??''));
        }elseif($method==='DELETE'&&preg_match('~^/api/bindings/([^/]+)/([^/]+)/([^/]+)/([^/]+)$~',$path,$m))$path='/api/bindings/'.$m[1].'/e/'.$m[2].'/q/'.$m[3].'/'.$m[4];
        if(preg_match('~^/api/users/([^/]+)$~',$path,$m)){
            $name=rawurldecode($m[1]);self::name($name);
            if($method==='PUT'){
                $tags=$j['tags']??'';if(!is_string($tags)&&!is_array($tags))self::fail('tags must be a string or array');$tags=is_array($tags)?$tags:explode(',',$tags);foreach($tags as$tag)if(!is_string($tag))self::fail('tags must contain strings');$tags=array_values(array_filter(array_map('trim',$tags),static fn($v)=>$v!==''));
                $hash=$j['password_hash']??'';if(!is_string($hash))self::fail('password_hash must be a string');
                if($hash!==''){$raw=base64_decode($hash,true);if($raw===false||!in_array(strlen($raw),[36,68],true))self::fail('invalid RabbitMQ password hash');$algorithm=$j['hashing_algorithm']??null;if($algorithm!==null&&$algorithm!== (strlen($raw)===36?'rabbit_password_hashing_sha256':'rabbit_password_hashing_sha512'))self::fail('password hash algorithm mismatch');}
                else {if(isset($j['password'])&&!is_string($j['password']))self::fail('password must be a string');Auth::check($j['password']??'');$hash=Auth::hash($j['password']);}
                self::emit($b,'user',['name'=>$name,'hash'=>$hash,'tags'=>$tags===[]?['management']:$tags],$commands);return true;
            }
            if($method==='DELETE'){self::exists(isset($b->users[$name]),'user');self::emit($b,'delete_user',['name'=>$name],$commands);return true;}return false;
        }
        if(preg_match('~^/api/vhosts/([^/]+)$~',$path,$m)){
            $name=rawurldecode($m[1]);self::name($name);
            if($method==='PUT'){self::emit($b,'vhost',['name'=>$name],$commands);return true;}
            if($method==='DELETE'){if($name==='/')self::fail('the default vhost cannot be deleted');self::exists(in_array($name,$b->vhosts,true),'vhost');self::emit($b,'delete_vhost',['name'=>$name],$commands);return true;}return false;
        }
        if(preg_match('~^/api/(permissions|topic-permissions)/([^/]+)/([^/]+)(?:/([^/]+))?$~',$path,$m)){
            $vhost=rawurldecode($m[2]);$user=rawurldecode($m[3]);$b->forVhost($vhost);self::exists(isset($b->users[$user]),'user');$topic=$m[1]==='topic-permissions';$data=['vhost'=>$vhost,'user'=>$user];
            if($method==='PUT'){
                foreach($topic?['write','read']:['configure','write','read']as$key){$data[$key]=self::regex($j[$key]??'');}
                if($topic)$data['exchange']=self::string($j['exchange']??'');self::emit($b,$topic?'topic_permission':'permission',$data,$commands);return true;
            }
            if($method==='DELETE'){
                if($topic){if(isset($m[4])){$data['exchange']=rawurldecode($m[4]);self::exists(isset($b->topicPermissions[$user][$vhost][$data['exchange']]),'topic permission');self::emit($b,'delete_topic_permission',$data,$commands);}else{self::exists(!empty($b->topicPermissions[$user][$vhost]),'topic permissions');foreach(array_keys($b->topicPermissions[$user][$vhost])as$exchange)self::emit($b,'delete_topic_permission',$data+['exchange'=>$exchange],$commands);}}
                else{self::exists(isset($b->permissions[$user][$vhost]),'permission');self::emit($b,'delete_permission',$data,$commands);}return true;
            }return false;
        }
        if(preg_match('~^/api/(policies|operator-policies)/([^/]+)/([^/]+)$~',$path,$m)){
            $vhost=rawurldecode($m[2]);$name=rawurldecode($m[3]);self::name($name);$b->forVhost($vhost);$operator=$m[1]==='operator-policies';$data=['vhost'=>$vhost,'name'=>$name,'operator'=>$operator];
            if($method==='PUT'){
                $error=Policy::validate($j);if($error!==null)self::fail($error);self::regex($j['pattern']);$priority=$j['priority']??0;if(!is_int($priority))self::fail('policy priority must be an integer');
                foreach($j['definition']as$key=>$value){if(in_array($key,['message-ttl','expires','max-length','max-length-bytes','max-priority','delivery-limit'],true)&&(!is_int($value)||$value<0))self::fail('invalid policy '.$key);if(in_array($key,['dead-letter-exchange','dead-letter-routing-key','alternate-exchange','federation-upstream','federation-upstream-set'],true)&&!is_string($value))self::fail('invalid policy '.$key);if($key==='overflow'&&!in_array($value,['drop-head','reject-publish','reject-publish-dlx'],true))self::fail('invalid policy overflow');}
                foreach($j['definition']as$key=>$value){if($key==='expires'&&$value===0)self::fail('expires must be positive');if($key==='max-priority'&&($value<1||$value>255))self::fail('invalid policy max-priority');if($key==='dead-letter-strategy'&&!in_array($value,['at-most-once','at-least-once'],true))self::fail('invalid dead-letter strategy');if($key==='queue-mode'&&!in_array($value,['default','lazy'],true))self::fail('invalid queue mode');}
                self::emit($b,'policy',$data+['pattern'=>$j['pattern'],'apply_to'=>$j['apply-to']??'all','priority'=>$priority,'definition'=>$j['definition']],$commands);return true;
            }
            if($method==='DELETE'){$field=$operator?'operatorPolicies':'policies';self::exists(isset($b->{$field}[$vhost][$name]),'policy');self::emit($b,'delete_policy',$data,$commands);return true;}return false;
        }
        if(preg_match('~^/api/(user|vhost)-limits/([^/]+)/([^/]+)$~',$path,$m)){
            $name=rawurldecode($m[2]);$limit=rawurldecode($m[3]);$allowed=$m[1]==='user'?['max-connections','max-channels']:['max-connections','max-queues'];if(!in_array($limit,$allowed,true))self::fail('unknown limit');
            $field=$m[1]==='user'?'userLimits':'vhostLimits';if($m[1]==='user')self::exists(isset($b->users[$name]),'user');else$b->forVhost($name);$value=$b->{$field}[$name]??[];
            if($method==='PUT'){if(!isset($j['value'])||!is_int($j['value'])||$j['value']< -1)self::fail('limit must be an integer >= -1');$value[$limit]=$j['value'];}
            elseif($method==='DELETE'){unset($value[$limit]);}else return false;
            self::emit($b,$m[1].'_limits',[$m[1]=>$name,'value'=>$value],$commands);$code=204;return true;
        }
        if(preg_match('~^/api/parameters/([^/]+)/([^/]+)/([^/]+)$~',$path,$m)){
            $component=rawurldecode($m[1]);$vhost=rawurldecode($m[2]);$name=rawurldecode($m[3]);$b->forVhost($vhost);self::name($component);self::name($name);$data=compact('component','vhost','name');
            // Shovels belong to the declaring node and must bypass replicated metadata.
            if($component==='shovel')return false;
            if($method==='PUT'){if(!array_key_exists('value',$j))self::fail('parameter needs a value');self::emit($b,'parameter',$data+['value'=>$j['value']],$commands);return true;}
            if($method==='DELETE'){self::exists(isset($b->parameters[$component][$vhost][$name]),'parameter');self::emit($b,'delete_parameter',$data,$commands);return true;}return false;
        }
        if(preg_match('~^/api/global-parameters/([^/]+)$~',$path,$m)){
            $name=rawurldecode($m[1]);self::name($name);
            if($method==='PUT'){if(!array_key_exists('value',$j))self::fail('parameter needs a value');self::emit($b,'global_parameter',['name'=>$name,'value'=>$j['value']],$commands);return true;}
            if($method==='DELETE'){self::exists(array_key_exists($name,$b->parameters['global']['/']??[]),'parameter');self::emit($b,'delete_global_parameter',['name'=>$name],$commands);return true;}return false;
        }
        if(preg_match('~^/api/(queues|exchanges)/([^/]+)/([^/]+)$~',$path,$m)&&in_array($method,['PUT','DELETE'],true)){
            $scope=$b->forVhost(rawurldecode($m[2]));$name=rawurldecode($m[3]);self::name($name);$queue=$m[1]==='queues';if($queue&&in_array($scope->vhost,$live->vhosts,true)&&($live->forVhost($scope->vhost)->queues[$name]['exclusive']??false))self::fail('queue is exclusive',405);$class=$queue?50:40;$command=$method==='PUT'?10:40;if(!$queue&&$method==='DELETE')$command=20;
            if($method==='PUT'){
                $args=self::map($j['arguments']??[]);$bits=0;foreach(['durable'=>2,'auto_delete'=>$queue?8:4,'internal'=>8]as$key=>$bit){if($queue&&$key==='internal')continue;$value=$j[$key]??($key==='durable');if(!is_bool($value))self::fail($key.' must be boolean');if($value)$bits|=$bit;}
                if(($j['exclusive']??false)!==false)self::fail('management cannot declare an exclusive queue');
                $payload=pack('nnn',$class,$command,0).Codec::shortstr($name).($queue?'':Codec::shortstr(self::string($j['type']??'direct'))).chr($bits).self::table($args);
            }else{self::exists($queue?isset($scope->queues[$name]):isset($scope->exchanges[$name]),$queue?'queue':'exchange');$payload=pack('nnn',$class,$command,0).Codec::shortstr($name)."\0";}
            self::protocol($scope,$live,$class,$command,$payload,$admin,$commands);if($method==='PUT')$body=json_encode(['name'=>$name],JSON_THROW_ON_ERROR);return true;
        }
        if(preg_match('~^/api/bindings/([^/]+)/e/([^/]+)/(q|e)/([^/]+)(?:/([^/]+))?$~',$path,$m)){
            if(!in_array($method,['POST','DELETE'],true))return false;$scope=$b->forVhost(rawurldecode($m[1]));$source=rawurldecode($m[2]);$dest=rawurldecode($m[4]);$queue=$m[3]==='q';if($queue&&in_array($scope->vhost,$live->vhosts,true)&&($live->forVhost($scope->vhost)->queues[$dest]['exclusive']??false))self::fail('queue is exclusive',405);$class=$queue?50:40;$command=$method==='DELETE'?($queue?50:40):($queue?20:30);$key=self::string($j['routing_key']??(isset($m[5])?rawurldecode($m[5]):''));$args=self::map($j['arguments']??[]);self::name($source);self::name($dest);if(strlen($key)>255||str_contains($key,"\0"))self::fail('invalid routing key');
            if($method==='DELETE'&&isset($m[5])&&!array_key_exists('routing_key',$j)){
                $found=null;foreach($queue?$scope->bindings:$scope->e2e as$row){$rowSource=$row['exchange']??$row['source'];$rowDest=$row['queue']??$row['destination'];$rowArgs=$row['args']??[];$properties=$row['key']=== ''&&$rowArgs===[]?'~':$row['key'];if($rowSource===$source&&$rowDest===$dest&&($properties===$key||$row['key']===$key)){$found=$row;break;}}
                self::exists($found!==null,'binding');$key=$found['key'];$args=[];foreach($found['args']??[]as[$k,$value])$args[$k]=$value;
            }
            $payload=pack('nnn',$class,$command,0).Codec::shortstr($dest).Codec::shortstr($source).Codec::shortstr($key).($queue&&$command===50?'':"\0").self::table($args);
            self::protocol($scope,$live,$class,$command,$payload,$admin,$commands);if($method==='POST'){$code=201;$body='';}return true;
        }
        return false;
    }
    private static function protocol(Broker $scope,Broker $live,int $class,int $method,string $payload,string $admin,array &$commands):void
    {
        $scope->transientNonexcl=$live->transientNonexcl;
        $staged=Metadata::amqp($scope,0,1,$class,$method,$payload,$admin);if($staged===null)self::fail('unsupported metadata operation');
        foreach($staged['commands']as$command){if($command['kind']==='queue'&&in_array($command['data']['args']['x-queue-type']??'classic',['quorum','stream'],true)){$group=$live->cluster?->queueGroup($scope->vhost,$command['data']['name']);if($group!==null){$command['data']['raftGroup']=$group;$command['data']['raftLeader']=$live->nodeId;}}
            self::emit($scope->root(),$command['kind'],$command['data'],$commands);}
    }
    private static function definitions(Broker $b,Broker $live,array $j,?string $vhost,string $admin,array &$commands):void
    {
        $allowed=['users','vhosts','permissions','topic_permissions','queues','exchanges','bindings','policies','operator_policies','user_limits','vhost_limits','parameters','global_parameters','rabbit_version','rabbitmq_version','product_name','product_version'];foreach(array_keys($j)as$key)if(!in_array($key,$allowed,true))self::fail('unsupported definitions field '.$key);
        foreach(['vhosts','users','exchanges','queues','bindings','permissions','topic_permissions','policies','operator_policies','user_limits','vhost_limits','parameters','global_parameters']as$field){
            if(!isset($j[$field]))continue;if(!is_array($j[$field])||!array_is_list($j[$field]))self::fail('definitions '.$field.' must be an array');
            foreach($j[$field]as$row){if(!is_array($row))self::fail('invalid definitions row');$host=$vhost??self::string($row['vhost']??'/');$enc=rawurlencode($host);$name=rawurlencode(self::string($row['name']??''));$method='PUT';
                $path=match($field){'vhosts'=>'/api/vhosts/'.$name,'users'=>'/api/users/'.$name,'queues'=>'/api/queues/'.$enc.'/'.$name,'exchanges'=>'/api/exchanges/'.$enc.'/'.$name,'permissions'=>'/api/permissions/'.$enc.'/'.rawurlencode(self::string($row['user']??'')),'topic_permissions'=>'/api/topic-permissions/'.$enc.'/'.rawurlencode(self::string($row['user']??'')),'policies'=>'/api/policies/'.$enc.'/'.$name,'operator_policies'=>'/api/operator-policies/'.$enc.'/'.$name,'parameters'=>'/api/parameters/'.rawurlencode(self::string($row['component']??'')).'/'.$enc.'/'.$name,'global_parameters'=>'/api/global-parameters/'.$name,default=>''};
                if($field==='exchanges'&&(self::string($row['name']??'')===''||str_starts_with($row['name'],'amq.'))){
                    $scope=$b->forVhost($host);$builtin=$row['name'];self::exists(isset($scope->exchanges[$builtin]),'built-in exchange');$properties=$scope->exchangeRows[$builtin]??[];
                    foreach(['type'=>$scope->exchanges[$builtin],'durable'=>$properties['durable']??true,'auto_delete'=>$properties['autoDelete']??false,'internal'=>$properties['internal']??false]as$key=>$expected)if(($row[$key]??$expected)!==$expected)self::fail('inequivalent built-in exchange');
                    if(($row['arguments']??[])!==($properties['arguments']??[]))self::fail('inequivalent built-in exchange arguments');continue;
                }
                if($field==='bindings'){$type=$row['destination_type']??'queue';if(!in_array($type,['queue','exchange'],true))self::fail('invalid binding destination type');$path='/api/bindings/'.$enc.'/e/'.rawurlencode(self::string($row['source']??'')).'/'.($type==='queue'?'q':'e').'/'.rawurlencode(self::string($row['destination']??''));$method='POST';}
                if($vhost!==null&&in_array($field,['users','vhosts','user_limits','global_parameters'],true))self::fail('global definitions fields are invalid in a vhost import');
                if(in_array($field,['user_limits','vhost_limits'],true)){$kind=$field==='user_limits'?'user':'vhost';$id=self::string($row[$kind]??$row['name']??'');$limits=$row['value']??array_diff_key($row,[$kind=>true,'name'=>true]);foreach($limits as$key=>$value){$code=201;$body='';if(!self::request($b,$live,'PUT','/api/'.$kind.'-limits/'.rawurlencode($id).'/'.rawurlencode((string)$key),['value'=>$value],$admin,$commands,$code,$body))self::fail('unsupported limit');}continue;}
                $code=201;$body='';if(!self::request($b,$live,$method,$path,$row,$admin,$commands,$code,$body))self::fail('unsupported definitions operation');
            }
        }
    }
    private static function map(mixed $value):array {if(!is_array($value)||($value!==[]&&array_is_list($value)))self::fail('arguments must be an object');return$value;}
    private static function table(array $args):string {self::tableValues($args);$pairs=[];foreach($args as$key=>$value){self::name((string)$key);$pairs[]=[$key,$value];}return Codec::writeTable($pairs);}
    private static function tableValues(array $args,int $depth=0):void {if($depth>32)self::fail('arguments nesting limit');foreach($args as$key=>$value){if(is_string($key))self::name($key);if(is_array($value))self::tableValues($value,$depth+1);elseif(!is_string($value)&&!is_int($value)&&!is_bool($value))self::fail('unsupported argument value');}}
    private static function regex(mixed $value):string {$value=self::string($value);if(@preg_match('~'.str_replace('~','\\~',$value).'~','')===false)self::fail('invalid regular expression');return$value;}
    private static function string(mixed $value):string {if(!is_string($value))self::fail('expected string');return$value;}
    private static function name(string $name):void {if($name===''||strlen($name)>255||str_contains($name,"\0"))self::fail('invalid resource name');}
    private static function exists(bool $exists,string $resource):void {if(!$exists)self::fail($resource.' not found',404);}
    private static function fail(string $message,int $code=400):never {throw new RuntimeException($message,$code);}
    private static function response(int $code,string $body):string {$text=[201=>'Created',204=>'No Content'][$code]??'OK';return"HTTP/1.1 $code $text\r\nContent-Type: application/json\r\nContent-Length: ".strlen($body)."\r\nConnection: close\r\n\r\n".$body;}
    private static function remove(string $path):void {if(is_file($path)||is_link($path)){@unlink($path);return;}if(!is_dir($path))return;foreach(new DirectoryIterator($path)as$entry)if(!$entry->isDot())self::remove($entry->getPathname());@rmdir($path);}
}
