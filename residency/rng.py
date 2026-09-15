"""Deterministic pseudo-random numbers.

A 32-bit linear congruential generator (Numerical Recipes constants), the
same stream the erasure harness and its companion paper use, so a seed means
the same draws everywhere:

    s_{n+1} = (1664525 * s_n + 1013904223) mod 2**32,   u_n = s_{n+1} / 2**32

lhbench used ``random.Random`` (Mersenne Twister).  Its seeds therefore do
not reproduce its exact draws here; ``reconcile.py`` compares the committed
lhbench results as data rather than re-deriving them.
"""
import math

MOD = 2 ** 32


class LCG:
    def __init__(self, seed: int):
        self.s = seed % MOD

    def u(self) -> float:
        self.s = (1664525 * self.s + 1013904223) % MOD
        return self.s / MOD

    def below(self, p: float) -> bool:
        return self.u() < p

    def below_int(self, n: int) -> int:
        """Uniform integer in [0, n).  n must be positive."""
        if n <= 0:
            raise ValueError("below_int needs n > 0")
        return int(self.u() * n)

    def weighted_index(self, cumulative) -> int:
        """Index drawn proportionally to weights, given their running sums."""
        total = cumulative[-1]
        r = self.u() * total
        lo, hi = 0, len(cumulative) - 1
        while lo < hi:                       # first index with cumulative > r
            mid = (lo + hi) // 2
            if cumulative[mid] > r:
                hi = mid
            else:
                lo = mid + 1
        return lo

    def poisson(self, lam: float) -> int:
        """Knuth's algorithm (lhbench used the same one)."""
        if lam <= 0:
            return 0
        limit = math.exp(-lam)
        k, p = 0, 1.0
        while True:
            p *= self.u()
            if p <= limit:
                return k
            k += 1
            if k > 10_000:
                return k

    def sample(self, population: list, k: int) -> list:
        """k distinct elements, order of draw, without replacement."""
        pool = list(population)
        out = []
        for _ in range(min(k, len(pool))):
            out.append(pool.pop(self.below_int(len(pool))))
        return out


def power_law_weights(n: int, alpha: float) -> list[float]:
    """Zipf-like: subject i gets weight 1/(i+1)^alpha (lhbench)."""
    if n <= 0:
        raise ValueError("n must be positive")
    if alpha < 0:
        raise ValueError("alpha must be non-negative")
    return [1.0 / ((i + 1) ** alpha) for i in range(n)]


def cumulative(weights) -> list[float]:
    out, acc = [], 0.0
    for w in weights:
        acc += w
        out.append(acc)
    return out
