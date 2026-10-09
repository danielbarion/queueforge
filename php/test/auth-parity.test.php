<?php
declare(strict_types=1);

require_once __DIR__ . '/lib/Harness.php';
foreach (['Routing','Features','Policy','Codec','Auth','Store','Broker','Security'] as $file) require_once dirname(__DIR__) . '/src/' . $file . '.php';

function b64url(string $raw): string { return rtrim(strtr(base64_encode($raw), '+/', '-_'), '='); }
function jwt($key, array $claims, array $header = []): string
{
    $h = b64url(json_encode($header + ['alg' => 'RS256', 'kid' => 'fixture'], JSON_THROW_ON_ERROR));
    $c = b64url(json_encode($claims, JSON_THROW_ON_ERROR));
    openssl_sign($h . '.' . $c, $sig, $key, OPENSSL_ALGO_SHA256);
    return $h . '.' . $c . '.' . b64url($sig);
}

/** A real localhost HTTP/HTTPS or BER LDAP peer; no LDAP extension/daemon dependency. */
function authFixture(string $dir, string $mode, array $options = []): array
{
    $port = Harness::freePort();
    $script = <<<'MOCK'
<?php
[$script,$mode,$port,$dir,$json] = $argv; $options=json_decode($json,true);
$context=stream_context_create(['ssl'=>['local_cert'=>$options['cert']??null,'local_pk'=>$options['key']??null,'verify_peer'=>false]]);
$listener=stream_socket_server('tcp://127.0.0.1:'.$port,$errno,$error,STREAM_SERVER_BIND|STREAM_SERVER_LISTEN,$context);
if(!$listener){fwrite(STDERR,$error);exit(1);} echo "ready\n";fflush(STDOUT);
function readExact($c,$n){$s='';while(strlen($s)<$n){$b=fread($c,$n-strlen($s));if($b===false||$b==='')throw new Exception('closed');$s.=$b;}return $s;}
function frame($c){$h=readExact($c,2);$n=ord($h[1]);$more='';if($n&128){$k=$n&127;$more=readExact($c,$k);$n=0;for($i=0;$i<$k;$i++)$n=($n<<8)|ord($more[$i]);}return $h.$more.readExact($c,$n);}
function response($id,$tag,$body){return "\x30".chr(3+2+strlen($body))."\x02\x01".chr($id).chr($tag).chr(strlen($body)).$body;}
while($c=@stream_socket_accept($listener,10)){
 stream_set_timeout($c,2);
 try{
 if($mode==='https'&&!stream_socket_enable_crypto($c,true,STREAM_CRYPTO_METHOD_TLS_SERVER)){fclose($c);continue;}
 if($mode==='http'||$mode==='https'){
  $request='';while(!str_contains($request,"\r\n\r\n")){$b=fread($c,4096);if(!$b)break;$request.=$b;}
  $body=file_get_contents($dir.'/jwks.json');fwrite($c,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ".strlen($body)."\r\nConnection: close\r\n\r\n".$body);
 }else{
  for($step=0;$step<4;$step++){
   $wire=frame($c);file_put_contents($dir.'/ldap.log',base64_encode($wire)."\n",FILE_APPEND);
   $at=(ord($wire[1])&128)?2+(ord($wire[1])&127):2;$id=ord($wire[$at+2]);$op=ord($wire[$at+3]);
   if($op===0x42)break;
   if($op===0x60){$ok=str_contains($wire,'correct')&&!str_contains($wire,'incorrect');if($step>0)$ok=str_contains($wire,'service-secret');$code=$ok?0:49;fwrite($c,response(($options['wrongId']??false)?$id+1:$id,0x61,"\x0a\x01".chr($code)."\x04\x00\x04\x00"));if(!$ok)break;}
   elseif($op===0x63){if($options['member']??false)fwrite($c,response($id,0x64,"\x04\x01x\x30\x00"));fwrite($c,response($id,0x65,"\x0a\x01".chr(($options['searchError']??false)?32:0)."\x04\x00\x04\x00"));}
   else break;
  }
 }
 }catch(Throwable $e){} fclose($c);
}
MOCK;
    $path = $dir . '/mock-' . bin2hex(random_bytes(3)) . '.php'; file_put_contents($path, $script);
    $proc = proc_open([PHP_BINARY, $path, $mode, (string) $port, $dir, json_encode($options, JSON_THROW_ON_ERROR)], [1 => ['pipe','w'], 2 => ['file', $dir . '/fixture.err','a']], $pipes);
    if (!is_resource($proc) || fgets($pipes[1]) !== "ready\n") throw new RuntimeException('auth fixture did not start');
    return ['proc' => $proc, 'pipes' => $pipes, 'port' => $port];
}
function stopAuthFixture(array $fixture): void { proc_terminate($fixture['proc']); foreach ($fixture['pipes'] as $pipe) fclose($pipe); proc_close($fixture['proc']); }

Harness::guard('RS256 claims, JWKS and scoped principals', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-auth-' . bin2hex(random_bytes(5)); mkdir($dir);
    $broker = new Broker(new Store($dir . '/messages.log'), $dir . '/users.json'); $broker->vhosts = ['/','prod','dev'];
    $key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
    $detail = openssl_pkey_get_details($key);
    $jwk = ['kty'=>'RSA','kid'=>'fixture','alg'=>'RS256','use'=>'sig','n'=>b64url($detail['rsa']['n']),'e'=>b64url($detail['rsa']['e'])];
    file_put_contents($dir . '/jwks.json', json_encode(['keys'=>[$jwk]]));
    $fixture = authFixture($dir, 'http');
    try {
        $cfg = ['oauth'=>['resourceServerId'=>'rabbitmq','jwksUrl'=>'http://127.0.0.1:' . $fixture['port'] . '/keys','jwksCaPath'=>null]];
        $claims = ['sub'=>'external','exp'=>time()+600,'aud'=>['other','rabbitmq'],'scope'=>'rabbitmq.read:%2F/orders-* rabbitmq.write:prod/ex-*/sales.* rabbitmq.configure:prod/app-* rabbitmq.tag:management other.tag:administrator'];
        $token = jwt($key, $claims); $p = Security::verify($broker, $cfg, '', $token);
        Harness::eq('subject is the external login', 'external', $p['name'] ?? '');
        Harness::eq('only this server grants tags', ['management'], $p['tags'] ?? []);
        Harness::ok('decoded vhost and resource wildcard', Security::allows($p, '/', 'read', 'orders-1'));
        Harness::ok('regex metacharacters stay literal', !Security::allows($p, '/', 'read', 'ordersX1'));
        Harness::ok('scopes do not grant another operation', !Security::allows($p, '/', 'configure', 'orders-1'));
        Harness::ok('routing key and resource match together', Security::allows($p, 'prod', 'write', 'ex-a', 'sales.eu'));
        Harness::ok('routing key is restricted', !Security::allows($p, 'prod', 'write', 'ex-a', 'private.eu'));
        Harness::ok('unscoped vhost denied', !Security::hasVhost($p, 'dev'));
        $pattern = $p['permissions']['/']['read'] ?? '(?!)';
        Harness::ok('permission map matches authorized resources only', preg_match('~'.$pattern.'~D','orders-1')===1 && preg_match('~'.$pattern.'~D','elsewhere')===0);
        Harness::eq('provided name is the login', 'login-name', Security::verify($broker, $cfg, 'login-name', $token)['name'] ?? '');
        foreach (['expired'=>['exp'=>time()-1], 'missing exp'=>['exp'=>null], 'string exp'=>['exp'=>'9999999999'], 'wrong audience'=>['aud'=>'elsewhere'], 'future nbf'=>['nbf'=>time()+120], 'malformed nbf'=>['nbf'=>'later']] as $name=>$patch) {
            Harness::eq($name . ' rejected', null, Security::verify($broker, $cfg, 'external', jwt($key, array_replace($claims,$patch))));
        }
        Harness::eq('unknown signing key rejected', null, Security::verify($broker,$cfg,'external',jwt($key,$claims,['kid'=>'unknown'])));
        Harness::eq('algorithm confusion rejected', null, Security::verify($broker,$cfg,'external',jwt($key,$claims,['alg'=>'HS256'])));
        $parts = explode('.', $token); $parts[1] = b64url(json_encode($claims + ['extra'=>'tampered']));
        Harness::eq('tampered token rejected', null, Security::verify($broker,$cfg,'external',implode('.',$parts)));
        Harness::eq('broken token rejected', null, Security::verify($broker,$cfg,'external','not.a.jwt'));
        $broker->users['internal'] = Auth::hash('local-password');
        Harness::eq('supplied internal name cannot be shadowed', null, Security::verify($broker,$cfg,'internal',$token));
        Harness::eq('token subject cannot shadow internal name', null, Security::verify($broker,$cfg,'',jwt($key,array_replace($claims,['sub'=>'internal']))));
        $expired = $p; $expired['expiresAt'] = (microtime(true)-1)*1000;
        Harness::ok('existing principal expires', !Security::allows($expired,'/','read','orders-1') && !Security::hasVhost($expired,'/'));
        file_put_contents($dir.'/jwks.json','{"keys":"bad"}'); $cfg['oauth']['jwksUrl'] .= '?invalid';
        Harness::eq('malformed JWKS denied',null,Security::verify($broker,$cfg,'external',$token));
        file_put_contents($dir.'/jwks.json',json_encode(['keys'=>[$jwk+['key_ops'=>['sign']]]])); $cfg['oauth']['jwksUrl'] .= '2';
        Harness::eq('nonverification key denied',null,Security::verify($broker,$cfg,'external',$token));
    } finally { stopAuthFixture($fixture); }
});

Harness::guard('JWKS TLS validates configured CA', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-auth-tls-' . bin2hex(random_bytes(5)); mkdir($dir);
    $broker = new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');
    $key=openssl_pkey_new(['private_key_bits'=>2048,'private_key_type'=>OPENSSL_KEYTYPE_RSA]);$rsa=openssl_pkey_get_details($key)['rsa'];
    file_put_contents($dir.'/jwks.json',json_encode(['keys'=>[['kid'=>'fixture','kty'=>'RSA','n'=>b64url($rsa['n']),'e'=>b64url($rsa['e'])]]]));
    file_put_contents($dir.'/openssl.cnf',"[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,digitalSignature,keyCertSign\n");
    $tlsKey=openssl_pkey_new(['private_key_bits'=>2048]);$options=['config'=>$dir.'/openssl.cnf','digest_alg'=>'sha256','x509_extensions'=>'ext'];
    $csr=openssl_csr_new(['commonName'=>'localhost'],$tlsKey,$options);$cert=openssl_csr_sign($csr,null,$tlsKey,1,$options);openssl_x509_export($cert,$pem);openssl_pkey_export($tlsKey,$private);
    file_put_contents($dir.'/ca.pem',$pem);file_put_contents($dir.'/key.pem',$private);
    $fixture=authFixture($dir,'https',['cert'=>$dir.'/ca.pem','key'=>$dir.'/key.pem']);
    try {
        $cfg=['oauth2'=>['resource_server_id'=>'rabbitmq','jwks_url'=>'https://localhost:'.$fixture['port'].'/keys','jwks_ca_path'=>$dir.'/ca.pem']];
        $token=jwt($key,['sub'=>'tls-user','exp'=>time()+600,'aud'=>'rabbitmq','scope'=>'rabbitmq.read:*/*']);
        Harness::eq('configured CA authenticates HTTPS JWKS','tls-user',Security::verify($broker,$cfg,'',$token)['name']??'');
        $cfg['oauth2']['jwks_url'].='?untrusted';$cfg['oauth2']['jwks_ca_path']=null;
        Harness::eq('untrusted JWKS certificate denied',null,Security::verify($broker,$cfg,'',$token));
        $cfg['oauth2']['jwks_ca_path']=$dir.'/ca.pem';$cfg['oauth2']['jwks_url']='https://127.0.0.1:'.$fixture['port'].'/keys';
        Harness::eq('CA does not bypass hostname verification',null,Security::verify($broker,$cfg,'',$token));
    }finally{stopAuthFixture($fixture);}
});

Harness::guard('LDAP bind, escaped DN and group authorization over BER', static function (): void {
    $dir = sys_get_temp_dir() . '/qf-auth-ldap-' . bin2hex(random_bytes(5)); mkdir($dir);
    $broker=new Broker(new Store($dir.'/messages.log'),$dir.'/users.json');
    foreach (['member'=>['member'=>true], 'nonmember'=>[], 'searchfailure'=>['member'=>true,'searchError'=>true], 'wrongid'=>['wrongId'=>true]] as $name=>$options) {
        $fixture=authFixture($dir,'ldap',$options);
        try {
            $cfg=['ldap'=>['server'=>'127.0.0.1','port'=>$fixture['port'],'userDnPattern'=>'uid=${username},dc=example','adminGroup'=>'cn=admins,dc=example','bindDn'=>'cn=service,dc=example','bindPassword'=>'service-secret']];
            $p=Security::verify($broker,$cfg,'alice,admin','correct');
            if($name==='wrongid'){Harness::eq('mismatched LDAP response id denied',null,$p);continue;}
            Harness::eq($name.' tags',$name==='member'?['administrator','management']:['management'],$p['tags']??[]);
            Harness::ok($name.' default LDAP access',Security::allows($p,'any-vhost','configure','anything'));
            Harness::eq($name.' bad password rejected',null,Security::verify($broker,$cfg,'alice','incorrect'));
            Harness::eq($name.' anonymous bind rejected',null,Security::verify($broker,$cfg,'alice',''));
            Harness::eq($name.' empty user rejected',null,Security::verify($broker,$cfg,'','correct'));
        }finally{stopAuthFixture($fixture);}
    }
    $log=file_get_contents($dir.'/ldap.log');$requests=array_map('base64_decode',array_filter(explode("\n",$log)));
    Harness::ok('DN value escaped instead of injecting attribute',array_any($requests,static fn($wire)=>str_contains($wire,'uid=alice\\,admin,dc=example')));
    Harness::ok('group query uses service credentials',array_any($requests,static fn($wire)=>str_contains($wire,'cn=service,dc=example')&&str_contains($wire,'service-secret')));
    Harness::ok('membership filter carries exact user DN',array_any($requests,static fn($wire)=>str_contains($wire,'member')&&str_contains($wire,'uid=alice\\,admin,dc=example')));
});

Harness::done();
