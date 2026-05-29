#!/usr/bin/env node
import { getFSReport } from '../lib/FSStatLib.js';

function usage() {
  console.error('Usage: fsstat <directory> <MaxFS-bytes> <NB> [--concurrency N]');
  console.error('Example: fsstat ./src 1048576 8     # 1 MiB cap, 8 bands');
  process.exit(2);
}

function parseArgs(argv) {
  const positional = [];
  const opts = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--concurrency') {
      opts.concurrency = Number(argv[++i]);
    } else if (a === '-h' || a === '--help') {
      usage();
    } else {
      positional.push(a);
    }
  }
  if (positional.length !== 3) usage();
  const [dir, maxFSRaw, nbRaw] = positional;
  const maxFS = Number(maxFSRaw);
  const nb = Number(nbRaw);
  if (!Number.isFinite(maxFS) || maxFS <= 0) usage();
  if (!Number.isInteger(nb) || nb < 1) usage();
  return { dir, maxFS, nb, opts };
}

function humanBytes(n) {
  if (!Number.isFinite(n)) return '∞';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 100 || i === 0 ? 0 : 1)} ${units[i]}`;
}

function printReport(report) {
  console.log(`Root:        ${report.root}`);
  console.log(`Total files: ${report.totalFiles}`);
  console.log(`MaxFS:       ${humanBytes(report.maxFS)}`);
  console.log(`Bands:       ${report.numBands} + 1 oversize`);
  console.log('');
  console.log('Distribution:');
  const maxCount = Math.max(1, ...report.bands);
  const barWidth = 30;
  for (let i = 0; i < report.bands.length; i++) {
    const { from, to } = report.bandRanges[i];
    const count = report.bands[i];
    const label =
      to === Infinity
        ? `> ${humanBytes(from).padStart(8)}             `
        : `${humanBytes(from).padStart(8)} – ${humanBytes(to).padStart(8)}`;
    const bar = '█'.repeat(Math.round((count / maxCount) * barWidth));
    console.log(`  ${label}  ${String(count).padStart(7)}  ${bar}`);
  }
  if (report.skippedEntries.length > 0) {
    console.log('');
    console.log(`Skipped (${report.skippedEntries.length}):`);
    for (const s of report.skippedEntries.slice(0, 10)) {
      console.log(`  - ${s.path}  [${s.error}]`);
    }
    if (report.skippedEntries.length > 10) {
      console.log(`  ... ${report.skippedEntries.length - 10} more`);
    }
  }
}

const { dir, maxFS, nb, opts } = parseArgs(process.argv.slice(2));
const t0 = performance.now();
const report = await getFSReport(dir, maxFS, nb, opts);
const elapsedMs = performance.now() - t0;
printReport(report);
console.log('');
console.log(`Elapsed: ${elapsedMs.toFixed(1)} ms`);
