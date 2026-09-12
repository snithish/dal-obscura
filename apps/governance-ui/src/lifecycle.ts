/** Small, dependency-free guards shared by asynchronous UI operations. */

export function isCurrentEpoch(expected: number, current: number): boolean {
  return expected === current;
}

export function nextEpoch(current: number): number {
  return current + 1;
}
