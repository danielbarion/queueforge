import { statfsSync } from "node:fs";

/**
 * Available bytes on the filesystem that holds `path`.
 *
 * This is the same sample the disk alarm uses: unprivileged free blocks times
 * the block size. A failed probe is 0, which is a real reading, not a placeholder.
 */
export function availableBytes(path: string): number {
  try {
    const stat = statfsSync(path || ".");
    const blocks = Number(stat.bavail);
    const size = Number(stat.bsize);
    if (!Number.isFinite(blocks) || !Number.isFinite(size) || blocks < 0 || size <= 0) return 0;
    return blocks * size;
  } catch {
    return 0;
  }
}
