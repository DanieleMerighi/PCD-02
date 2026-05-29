// File-size distribution over NB equal-width bands on [0, MaxFS],
// plus one extra "oversize" band for sizes strictly greater than MaxFS.
//
// Band index rule (size >= 0):
//   - if size > MaxFS         -> bands[NB]            (oversize)
//   - else                    -> floor(size / width)  clipped to NB-1
//     so size === MaxFS lands in the last in-range band, not in oversize.

export class Histogram {
  #maxFS;
  #numBands;
  #width;
  #counts;

  constructor(maxFS, numBands) {
    if (!Number.isFinite(maxFS) || maxFS <= 0) {
      throw new RangeError(`Histogram: MaxFS must be > 0, got ${maxFS}`);
    }
    if (!Number.isInteger(numBands) || numBands < 1) {
      throw new RangeError(`Histogram: NB must be a positive integer, got ${numBands}`);
    }
    this.#maxFS = maxFS;
    this.#numBands = numBands;
    this.#width = maxFS / numBands;
    this.#counts = new Array(numBands + 1).fill(0);
  }

  add(size) {
    if (size > this.#maxFS) {
      this.#counts[this.#numBands]++;
      return;
    }
    const idx = Math.min(Math.floor(size / this.#width), this.#numBands - 1);
    this.#counts[idx]++;
  }

  get bands() {
    return [...this.#counts];
  }

  get bandRanges() {
    const ranges = [];
    for (let i = 0; i < this.#numBands; i++) {
      ranges.push({ from: i * this.#width, to: (i + 1) * this.#width });
    }
    ranges.push({ from: this.#maxFS, to: Infinity });
    return ranges;
  }
}
