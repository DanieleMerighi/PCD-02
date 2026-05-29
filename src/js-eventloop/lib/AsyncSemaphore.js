// Counting semaphore for cooperative tasks on the event loop.

export class AsyncSemaphore {
  #permits;
  #waiters = [];

  constructor(permits) {
    if (!Number.isInteger(permits) || permits < 1) {
      throw new RangeError(`AsyncSemaphore: permits must be a positive integer, got ${permits}`);
    }
    this.#permits = permits;
  }

  acquire() {
    if (this.#permits > 0) {
      this.#permits--;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  release() {
    const next = this.#waiters.shift();
    if (next) next();
    else this.#permits++;
  }

  async run(task) {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }
}
