/**
 * Record crcs of the overwrite log, apart from `fast-log.ts` so the sync
 * worker loads only this.
 */

/** IEEE CRC-32, the same polynomial the table version used. Native, so replay and the group commit do not loop per byte in JS. */
export function crc32(buf: Uint8Array): number {
  return Bun.hash.crc32(buf) >>> 0;
}

/** Magic, type, id, the three length fields, body length, and crc. */
export const RECORD_FIXED = 4 + 1 + 8 + 2 + 2 + 4 + 4 + 4;

/**
 * Fill in the salted crc of every record in `buf`, which holds whole records
 * from offset 0 as `writeRows` in `fast-log.ts` wrote them with `fill` false. Runs on
 * the sync worker.
 */
export function fillCrcs(buf: Buffer, salt: number) {
  let o = 0;
  while (o + RECORD_FIXED <= buf.length) {
    const start = o;
    const type = buf[o + 4];
    o += 13;
    o += 2 + buf.readUInt16LE(o);
    if (type === 3) {
      o += 4 + buf.readUInt32LE(o);
      o += 4 + buf.readUInt32LE(o);
      const count = buf.readUInt32LE(o);
      o += 4;
      for (let k = 0; k < count; k++) o += 2 + buf.readUInt16LE(o);
    } else {
      o += 2 + buf.readUInt16LE(o);
      o += 4 + buf.readUInt32LE(o);
      o += 4 + buf.readUInt32LE(o);
    }
    buf.writeUInt32LE((crc32(buf.subarray(start, o)) ^ salt) >>> 0, o);
    o += 4;
  }
}
