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

    /** @return list<array{id:int,queue:string,body:string,mode:int,propRaw:?string}> */
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
                $o += $bodyLen;
                $mode = $o < strlen($payload) ? ord($payload[$o]) : 2;
                $o += 1;
                // Records written before properties were persisted stop here.
                $propRaw = null;
                if ($o + 4 <= strlen($payload)) {
                    $propLen = unpack('N', substr($payload, $o, 4))[1];
                    $o += 4;
                    if ($propLen > 0) {
                        $propRaw = substr($payload, $o, $propLen);
                    }
                }
                $live[$id] = ['id' => $id, 'queue' => $queue, 'body' => $body, 'mode' => $mode, 'propRaw' => $propRaw];
            } elseif ($type === 2 && strlen($payload) >= 8) {
                $id = Codec::readU64($payload, 0);
                unset($live[$id]);
            }
        }
        fseek($this->fp, $this->end);
        return array_values($live);
    }

    /**
     * Publish record: u64 id, shortstr queue, u32+body, mode byte, then
     * u32+propRaw. The trailing property field was added later, so replay
     * treats its absence as "no properties" and older logs still load.
     */
    public function appendPublish(int $id, string $queue, string $body, int $mode, ?string $propRaw = null): int
    {
        $props = $propRaw ?? '';
        $payload = Codec::u64($id)
            . Codec::shortstr($queue)
            . pack('N', strlen($body)) . $body
            . chr($mode)
            . pack('N', strlen($props)) . $props;
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
