import { resolve } from 'node:path';
import { AsyncSemaphore } from './AsyncSemaphore.js';
import { Histogram } from './Histogram.js';
import { scan, assertReadableDir } from './Scanner.js';

const DEFAULT_CONCURRENCY = 64;

// Public facade. Returns the report shape:
//   { root, totalFiles, maxFS, numBands,
//     bands: number[NB+1], bandRanges: [{from,to}],
//     skippedEntries: [{path, error}] }

export async function getFSReport(D, MaxFS, NB, opts = {}) {
  const root = resolve(D);
  await assertReadableDir(root);

  const concurrency = opts.concurrency ?? DEFAULT_CONCURRENCY;
  const semaphore = new AsyncSemaphore(concurrency);
  const histogram = new Histogram(MaxFS, NB);
  const skippedEntries = [];
  let totalFiles = 0;

  await scan({
    root,
    semaphore,
    skippedEntries,
    onFile: (_path, size) => {
      totalFiles++;
      histogram.add(size);
    },
  });

  return {
    root,
    totalFiles,
    maxFS: MaxFS,
    numBands: NB,
    bands: histogram.bands,
    bandRanges: histogram.bandRanges,
    skippedEntries,
  };
}
