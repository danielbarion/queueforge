<?php
declare(strict_types=1);

/** Bounded RFC6455 transport. Protocol authentication occurs inside MQTT/STOMP. */
final class WebSocket
{
    public static function upgrade(string $head): ?array
    {
        $lines=explode("\r\n",$head);if(!preg_match('#^GET /ws(?:\?[^ ]*)? HTTP/1\.1$#',array_shift($lines)))return null;
        $headers=[];foreach($lines as $line){$parts=explode(':',$line,2);if(count($parts)===2)$headers[strtolower(trim($parts[0]))]=trim($parts[1]);}
        $key=$headers['sec-websocket-key']??'';$decoded=base64_decode($key,true);
        if(strtolower($headers['upgrade']??'')!=='websocket'||!in_array('upgrade',array_map('trim',explode(',',strtolower($headers['connection']??''))),true)||($headers['sec-websocket-version']??'')!=='13'||$decoded===false||strlen($decoded)!==16)return null;
        $protocol=null;foreach(array_map('trim',explode(',',$headers['sec-websocket-protocol']??'')) as $candidate)if(in_array($candidate,['mqtt','stomp','v12.stomp','v11.stomp','v10.stomp'],true)){$protocol=$candidate;break;}
        if($protocol===null)return null;
        $accept=base64_encode(sha1($key.'258EAFA5-E914-47DA-95CA-C5AB0DC85B11',true));
        return ['kind'=>$protocol==='mqtt'?'mqtt':'stomp','response'=>"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: $accept\r\nSec-WebSocket-Protocol: $protocol\r\n\r\n"];
    }
    public static function frame(string $body,int $opcode=2):string
    {
        $n=strlen($body);return chr(0x80|$opcode).($n<126?chr($n):($n<=65535?"\x7e".pack('n',$n):"\x7f".pack('J',$n))).$body;
    }
    public static function decode(string &$buffer,array &$state):array
    {
        $messages=[];$reply='';$closed=false;
        while(strlen($buffer)>=2){
            $a=ord($buffer[0]);$b=ord($buffer[1]);$fin=(bool)($a&0x80);$opcode=$a&15;$length=$b&127;$at=2;
            if(($a&0x70)!==0||!($b&0x80))throw new RuntimeException('Invalid WebSocket frame');
            if($length===126){if(strlen($buffer)<4)break;$length=unpack('n',substr($buffer,2,2))[1];$at=4;if($length<126)throw new RuntimeException('Invalid WebSocket length');}
            elseif($length===127){if(strlen($buffer)<10)break;$length=unpack('J',substr($buffer,2,8))[1];$at=10;if($length<65536)throw new RuntimeException('Invalid WebSocket length');}
            if($length<0||$length>16*1024*1024||($opcode>=8&&(!$fin||$length>125)))throw new RuntimeException('WebSocket frame too large');
            if(strlen($buffer)<$at+4+$length)break;$mask=substr($buffer,$at,4);$at+=4;$payload=substr($buffer,$at,$length);$buffer=substr($buffer,$at+$length);
            for($i=0;$i<$length;$i++)$payload[$i]=$payload[$i]^$mask[$i%4];
            if($opcode===8){if($length===1)throw new RuntimeException('Invalid WebSocket close');$reply.=self::frame($payload,8);$closed=true;break;}
            if($opcode===9){$reply.=self::frame($payload,10);continue;}if($opcode===10)continue;
            if($opcode===0){if(!isset($state['fragmentOpcode']))throw new RuntimeException('Unexpected continuation');$state['fragment'].=$payload;}
            elseif($opcode===1||$opcode===2){if(isset($state['fragmentOpcode']))throw new RuntimeException('Interleaved fragmented messages');$state['fragmentOpcode']=$opcode;$state['fragment']=$payload;}
            else throw new RuntimeException('Unknown WebSocket opcode');
            if(strlen($state['fragment'])>16*1024*1024)throw new RuntimeException('WebSocket message too large');
            if($fin){if($state['fragmentOpcode']===1&&!preg_match('//u',$state['fragment']))throw new RuntimeException('Invalid WebSocket UTF-8');$messages[]=$state['fragment'];unset($state['fragmentOpcode'],$state['fragment']);}
        }
        return compact('messages','reply','closed');
    }
}
