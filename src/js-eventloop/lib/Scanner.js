import { readdir, lstat, stat } from 'node:fs/promises';
import { join } from 'node:path';

// Recursive directory scan on the event loop.

export async function scan({ root, onFile, semaphore, skippedEntries }) {
  await walk(root, { onFile, semaphore, skippedEntries });
}

async function walk(dir, ctx) {
  let entries;
  try {
    entries = await ctx.semaphore.run(() => readdir(dir, { withFileTypes: true }));
  } catch (err) {
    ctx.skippedEntries.push({ path: dir, error: err.code ?? err.message });
    return;
  }

  await Promise.all(entries.map((entry) => handleEntry(dir, entry, ctx)));
}

async function handleEntry(dir, entry, ctx) {
  const path = join(dir, entry.name);

  if (entry.isSymbolicLink()) return;

  if (entry.isDirectory()) {
    await walk(path, ctx);
    return;
  }

  if (entry.isFile()) {
    try {
      const st = await ctx.semaphore.run(() => stat(path));
      ctx.onFile(path, st.size);
    } catch (err) {
      ctx.skippedEntries.push({ path, error: err.code ?? err.message });
    }
    return;
  }

  // not regular files, ignore silently.
}

// Used when the root itself might be a symlink or unreadable: surfaces a
// meaningful error to the caller rather than silently producing an empty
// report.
export async function assertReadableDir(root) {
  const st = await lstat(root);
  if (!st.isDirectory()) {
    throw new Error(`FSStatLib: ${root} is not a directory`);
  }
}
