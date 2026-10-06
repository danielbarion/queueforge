<?php
declare(strict_types=1);

/**
 * Append-only log. A durable confirm is released only after fsync covers its record.
 */
final class Store
{
    /** @var resource */
    public $fp;
    public int $end = 0;
    public int $synced = 0;

    public function __construct(public string $path)
    {
        $dir = dirname($path);
        if (!is_dir($dir) && !mkdir($dir, 0777, true) && !is_dir($dir)) {
            throw new RuntimeException("cannot create $dir");
        }
        $fp = fopen($path, 'c+b');
        if ($fp === false) {
            throw new RuntimeException("cannot open $path");
        }
        $this->fp = $fp;
        $this->end = (int) filesize($path);
        $this->synced = $this->end;
    }

    /** @return list<array{id:int,queue:string,body:string,mode:int}> */
    public function replay(): array
    {
        rewind($this->fp);
        $live = [];
        $pos = 0;
        $size = $this->end;
        while ($pos + 5 <= $size) {
            $head = fread($this->fp, 5);
            if ($head === false || strlen($head) < 5) {
                break;
            }
            $type = ord($head[0]);
            $len = unpack('N', substr($head, 1, 4))[1];
            $payload = $len > 0 ? fread($this->fp, $len) : '';
            if ($payload === false || strlen($payload) < $len) {
                break;
            }
            $pos += 5 + $len;
            if ($type === 1 && strlen($payload) >= 8) {
                $id = Codec::readU64($payload, 0);
                $o = 8;
                $queue = Codec::readShortstr($payload, $o);
                if ($o + 4 > strlen($payload)) {
                    continue;
                }
                $bodyLen = unpack('N', substr($payload, $o, 4))[1];
                $o += 4;
                $body = substr($payload, $o, $bodyLen);
                $mode = $o + $bodyLen < strlen($payload) ? ord($payload[$o + $bodyLen]) : 2;
                $live[$id] = ['id' => $id, 'queue' => $queue, 'body' => $body, 'mode' => $mode];
            } elseif ($type === 2 && strlen($payload) >= 8) {
                $id = Codec::readU64($payload, 0);
                unset($live[$id]);
            }
        }
        fseek($this->fp, $this->end);
        return array_values($live);
    }

    public function appendPublish(int $id, string $queue, string $body, int $mode): int
    {
        $payload = Codec::u64($id) . Codec::shortstr($queue) . pack('N', strlen($body)) . $body . chr($mode);
        return $this->append(1, $payload);
    }

    public function appendAck(int $id): int
    {
        return $this->append(2, Codec::u64($id));
    }

    public function sync(): void
    {
        if ($this->synced === $this->end) {
            return;
        }
        fflush($this->fp);
        if (!fsync($this->fp)) {
            throw new RuntimeException('fsync failed');
        }
        $this->synced = $this->end;
    }

    private function append(int $type, string $payload): int
    {
        $rec = chr($type) . pack('N', strlen($payload)) . $payload;
        $n = fwrite($this->fp, $rec);
        if ($n !== strlen($rec)) {
            throw new RuntimeException('short write');
        }
        $this->end += $n;
        return $this->end;
    }
}
