<?php
$root=dirname(__DIR__,2);require "$root/php/src/Auth.php";require "$root/php/src/Codec.php";
$dir='/tmp/qf-external-pki-'.bin2hex(random_bytes(4));mkdir($dir);
register_shutdown_function(static function() use($dir): void {
    $files=new RecursiveIteratorIterator(new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS), RecursiveIteratorIterator::CHILD_FIRST);
    foreach($files as $file) $file->isDir() ? rmdir($file->getPathname()) : unlink($file->getPathname());
    rmdir($dir);
});
function runCmd(array $cmd){$p=proc_open($cmd,[1=>['pipe','w'],2=>['pipe','w']],$pipes);$out=stream_get_contents($pipes[1]);$err=stream_get_contents($pipes[2]);foreach($pipes as $fp)fclose($fp);if(proc_close($p))throw new Exception($err);}
runCmd(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',"$dir/ca.key",'-out',"$dir/ca.pem",'-subj','/CN=QF Test CA','-days','1']);
foreach(['server'=>'localhost','client'=>'cert-user','nobody'=>'missing-user'] as $name=>$cn){
 runCmd(['openssl','req','-newkey','rsa:2048','-nodes','-keyout',"$dir/$name.key",'-out',"$dir/$name.csr",'-subj',"/CN=$cn"]);
 file_put_contents("$dir/$name.ext",$name==='server'?"subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n":"extendedKeyUsage=clientAuth\n");
 runCmd(['openssl','x509','-req','-in',"$dir/$name.csr",'-CA',"$dir/ca.pem",'-CAkey',"$dir/ca.key",'-CAcreateserial','-out',"$dir/$name.pem",'-days','1','-extfile',"$dir/$name.ext"]);
}
runCmd(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',"$dir/stranger.key",'-out',"$dir/stranger.pem",'-subj','/CN=cert-user','-days','1']);
function port(){ $s=stream_socket_server('tcp://127.0.0.1:0');$n=stream_socket_get_name($s,false);fclose($s);return (int)substr(strrchr($n,':'),1); }
function bytes($fp,$n){$b='';while(strlen($b)<$n){$c=@fread($fp,$n-strlen($b));if(!$c)throw new Exception('closed');$b.=$c;}return $b;}
function frame($fp){$h=bytes($fp,7);$n=unpack('N',substr($h,3))[1];return bytes($fp,$n+1);}
function check($ok,$why){if(!$ok)throw new Exception($why);echo "ok $why\n";}
function login($port,$cert,$mechanism,$secure=true){global $dir;
 $ssl=['cafile'=>"$dir/ca.pem",'verify_peer'=>true,'verify_peer_name'=>true,'peer_name'=>'localhost'];if($cert){$ssl['local_cert']="$dir/$cert.pem";$ssl['local_pk']="$dir/$cert.key";}
 $fp=@stream_socket_client(($secure?'tls':'tcp')."://127.0.0.1:$port",$errno,$error,2,STREAM_CLIENT_CONNECT,stream_context_create(['ssl'=>$ssl]));if(!$fp)return ['open'=>false,'mechanisms'=>''];stream_set_timeout($fp,2);
 try{
 fwrite($fp,"AMQP\0\0\x09\x01");$start=frame($fp);$o=6;$len=unpack('N',substr($start,$o,4))[1];$o+=4+$len;$mechanisms=Codec::readLongstr($start,$o);
 fwrite($fp,Codec::method(0,10,11,pack('N',0).Codec::shortstr($mechanism).Codec::longstr($mechanism==='PLAIN'?"\0admin\0devpassword12":'ignored-authzid').Codec::shortstr('en_US')));
 $tune=frame($fp);if(substr($tune,0,4)!==pack('nn',10,30))return ['open'=>false,'mechanisms'=>$mechanisms];
 fwrite($fp,Codec::method(0,10,31,pack('nNn',2047,131072,0)));fwrite($fp,Codec::method(0,10,40,Codec::shortstr('/').Codec::shortstr('')."\0"));$open=frame($fp);
 return ['open'=>substr($open,0,4)===pack('nn',10,41),'mechanisms'=>$mechanisms];
 }catch(Throwable $e){return ['open'=>false,'mechanisms'=>$mechanisms??''];}finally{fclose($fp);}
}
foreach(['primary','secondary'] as $mode){
 $plain=port();$tls=$mode==='primary'?$plain:port();$data="$dir/$mode";mkdir($data);
 $perms=['/'=>['configure'=>'.*','write'=>'.*','read'=>'.*']];file_put_contents("$data/users.json",json_encode(['admin'=>['hash'=>Auth::hash('devpassword12'),'tags'=>['administrator'],'permissions'=>$perms],'cert-user'=>['hash'=>Auth::hash('unused-password'),'tags'=>[],'permissions'=>$perms]]));
 $config="[listeners]\namqp=\"127.0.0.1:$plain\"\n".($mode==='secondary'?"amqps=\"127.0.0.1:$tls\"\n":'')."[data]\ndir=\"$data\"\n[tls]\nenabled=".($mode==='primary'?'true':'false')."\ncert_path=\"$dir/server.pem\"\nkey_path=\"$dir/server.key\"\nca_path=\"$dir/ca.pem\"\n";file_put_contents("$dir/$mode.toml",$config);
 $proc=proc_open([PHP_BINARY,"$root/php/bin/queueforge",'--config',"$dir/$mode.toml"],[1=>['file',"$dir/$mode.out",'w'],2=>['file',"$dir/$mode.err",'w']],$pipes,$root);
 try{
 $until=microtime(true)+5;$ready=false;while(microtime(true)<$until){$ready=@stream_socket_client("tcp://127.0.0.1:$plain",$e,$s,.1);if($ready){fclose($ready);break;}usleep(10000);}check((bool)$ready,"$mode listener starts");
 $good=login($tls,'client','EXTERNAL');check($good['open']&&$good['mechanisms']==='PLAIN EXTERNAL',"$mode trusted CN authenticates EXTERNAL");
 $no=login($tls,null,'EXTERNAL');check(!$no['open'],"$mode absent certificate denied and unadvertised");
 check(!login($tls,'stranger','EXTERNAL')['open'],"$mode untrusted certificate denied");
 check(!login($tls,'nobody','EXTERNAL')['open'],"$mode trusted unknown CN denied");
 check(login($tls,'client','PLAIN')['open'],"$mode TLS PLAIN with trusted client certificate supported");
 if($mode==='secondary')check(!login($plain,null,'EXTERNAL',false)['open'],"plain socket EXTERNAL denied");
 }finally{proc_terminate($proc);proc_close($proc);}
}
echo "13 strict TLS checks passed\n";
