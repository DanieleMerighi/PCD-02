import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { getFSReport } from '../lib/FSStatLib.js';
import { Histogram } from '../lib/Histogram.js';
import { AsyncSemaphore } from '../lib/AsyncSemaphore.js';

async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), 'fsstat-'));
  // Layout:
  //   root/a.bin           size 100   -> band 0  [0,   1000)
  //   root/b.bin           size 1500  -> band 1  [1000,2000)
  //   root/sub/c.bin       size 2500  -> band 2  [2000,3000)  (last in-range)
  //   root/sub/d.bin       size 3000  -> band 2  (boundary: == MaxFS)
  //   root/sub/big.bin     size 5000  -> band 3  (oversize)
  //   root/sub/deep/e.bin  size 500   -> band 0
  //   root/sub/loop        symlink to root (must NOT be followed)
  // Expected: totalFiles = 6, bands = [2, 1, 2, 1]
  await mkdir(join(root, 'sub', 'deep'), { recursive: true });
  await writeFile(join(root, 'a.bin'), Buffer.alloc(100));
  await writeFile(join(root, 'b.bin'), Buffer.alloc(1500));
  await writeFile(join(root, 'sub', 'c.bin'), Buffer.alloc(2500));
  await writeFile(join(root, 'sub', 'd.bin'), Buffer.alloc(3000));
  await writeFile(join(root, 'sub', 'big.bin'), Buffer.alloc(5000));
  await writeFile(join(root, 'sub', 'deep', 'e.bin'), Buffer.alloc(500));
  try {
    await symlink(root, join(root, 'sub', 'loop'));
  } catch {
    // symlink may fail on some filesystems; the rest of the test stays valid.
  }
  return root;
}

test('getFSReport counts files and bins by size', async () => {
  const root = await buildFixture();
  try {
    const report = await getFSReport(root, 3000, 3);
    assert.equal(report.totalFiles, 6);
    assert.deepEqual(report.bands, [2, 1, 2, 1]);
    assert.equal(report.numBands, 3);
    assert.equal(report.bands.length, 4);
    assert.equal(report.skippedEntries.length, 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('getFSReport handles empty directory', async () => {
  const root = await mkdtemp(join(tmpdir(), 'fsstat-empty-'));
  try {
    const report = await getFSReport(root, 1000, 4);
    assert.equal(report.totalFiles, 0);
    assert.deepEqual(report.bands, [0, 0, 0, 0, 0]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('getFSReport rejects on non-directory root', async () => {
  await assert.rejects(() => getFSReport('/this/path/does/not/exist/xyz', 1000, 4));
});

test('Histogram boundary: size === MaxFS goes to last in-range band', () => {
  const h = new Histogram(1000, 4);
  h.add(0);     // band 0
  h.add(250);   // band 1
  h.add(999);   // band 3
  h.add(1000);  // band 3 (boundary: clipped, NOT oversize)
  h.add(1001);  // band 4 (oversize)
  assert.deepEqual(h.bands, [1, 1, 0, 2, 1]);
});

test('AsyncSemaphore caps concurrent tasks', async () => {
  const sem = new AsyncSemaphore(2);
  let active = 0;
  let peak = 0;
  const tasks = Array.from({ length: 10 }, () =>
    sem.run(async () => {
      active++;
      peak = Math.max(peak, active);
      await new Promise((r) => setTimeout(r, 5));
      active--;
    }),
  );
  await Promise.all(tasks);
  assert.equal(peak, 2);
});
