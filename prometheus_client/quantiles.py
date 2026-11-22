"""Streaming quantile estimation over a sliding time window.

This module implements a CKMS-style targeted quantile estimator (as
described in "Effective Computation of Biased Quantiles over Data
Streams" by Cormode, Korn, Muthukrishnan and Srivastava). The estimator
keeps only a small, compressed list of samples, so memory stays bounded
no matter how many observations are recorded.

Observations are bucketed by time so that quantiles are computed over a
sliding window. The compressed state of every bucket can be checkpointed
to disk and restored after a process restart, so quantiles resume from
where they left off instead of being recomputed from scratch. Raw
observations are never persisted.
"""

import atexit
import json
import math
import os
from threading import Lock
import time
from typing import Dict, List, Optional, Tuple
import weakref

# Number of observations buffered before they are merged into the
# compressed sample list. Matches the Go client.
_BUFFER_CAP = 500

# A compressed sample: (value, g, delta). g is the jump in rank over the
# previous sample, delta is the maximum error of the rank.
_Sample = Tuple[float, float, float]


class _Stream:
    """A CKMS stream computing targeted quantiles with bounded error."""

    __slots__ = ('_targets', '_buffer', '_samples', '_n')

    def __init__(self, targets: Dict[float, float]):
        self._targets = dict(targets)
        self._buffer: List[float] = []
        self._samples: List[_Sample] = []
        self._n = 0

    def insert(self, value: float) -> None:
        self._buffer.append(value)
        if len(self._buffer) >= _BUFFER_CAP:
            self.flush()

    def _invariant(self, rank: float) -> float:
        minimum = math.inf
        for quantile, error in self._targets.items():
            if quantile * self._n <= rank:
                allowed = (2.0 * error * rank) / quantile
            else:
                allowed = (2.0 * error * (self._n - rank)) / (1.0 - quantile)
            minimum = min(minimum, allowed)
        return minimum

    def flush(self) -> None:
        if not self._buffer:
            return
        self._buffer.sort()
        merged: List[_Sample] = []
        old = self._samples
        i = 0
        rank = 0.0
        for value in self._buffer:
            while i < len(old) and old[i][0] <= value:
                rank += old[i][1]
                merged.append(old[i])
                i += 1
            if i < len(old):
                delta = max(0.0, math.floor(self._invariant(rank)) - 1.0)
            else:
                delta = 0.0
            merged.append((value, 1.0, delta))
            self._n += 1
            rank += 1
        merged.extend(old[i:])
        self._buffer = []
        self._samples = merged
        self._compress()

    def _compress(self) -> None:
        samples = self._samples
        if len(samples) < 2:
            return
        kept: List[_Sample] = []
        x_value, x_g, x_delta = samples[-1]
        rank = self._n - 1 - x_g
        for i in range(len(samples) - 2, -1, -1):
            c_value, c_g, _ = samples[i]
            if c_g + x_g + x_delta <= self._invariant(rank):
                x_g += c_g
            else:
                kept.append((x_value, x_g, x_delta))
                x_value, x_g, x_delta = samples[i]
            rank -= c_g
        kept.append((x_value, x_g, x_delta))
        kept.reverse()
        self._samples = kept

    def query(self, quantile: float) -> float:
        self.flush()
        samples = self._samples
        if not samples:
            return 0.0
        target = math.ceil(quantile * self._n)
        target += math.ceil(self._invariant(target) / 2.0)
        rank = 0.0
        previous = samples[0]
        for candidate in samples[1:]:
            rank += previous[1]
            if rank + candidate[1] + candidate[2] > target:
                return previous[0]
            previous = candidate
        return previous[0]

    def to_dict(self) -> dict:
        self.flush()
        return {'n': self._n, 'samples': self._samples}

    @classmethod
    def from_dict(cls, targets: Dict[float, float], data: dict) -> '_Stream':
        stream = cls(targets)
        stream._n = data['n']
        stream._samples = [tuple(sample) for sample in data['samples']]
        return stream


class Estimator:
    """Estimates targeted quantiles over a sliding time window.

    Observations go into time buckets, each holding one compressed
    stream. Buckets older than ``max_age`` are dropped, so the window
    slides without storing raw observations. If ``checkpoint_path`` is
    given, the compressed state is written there on every bucket
    rotation and on ``flush()``, and is loaded back on construction so
    that a restarted process resumes the previous window.
    """

    def __init__(self,
                 objectives: Dict[float, float],
                 max_age: float = 600.0,
                 age_buckets: int = 5,
                 checkpoint_path: Optional[str] = None,
                 ):
        if not objectives:
            raise ValueError('objectives must not be empty')
        for quantile, error in objectives.items():
            if quantile <= 0.0 or quantile >= 1.0:
                raise ValueError('objective quantiles must be in (0, 1): %s' % quantile)
            if error <= 0.0 or error >= 1.0:
                raise ValueError('objective errors must be in (0, 1): %s' % error)
        if max_age <= 0:
            raise ValueError('max_age must be positive')
        if age_buckets < 1:
            raise ValueError('age_buckets must be at least 1')
        self._objectives = {float(q): float(e) for q, e in objectives.items()}
        self._max_age = float(max_age)
        self._age_buckets = int(age_buckets)
        self._bucket_duration = self._max_age / self._age_buckets
        self._buckets: List[Tuple[float, _Stream]] = []
        self._checkpoint_path = checkpoint_path
        self._lock = Lock()
        if checkpoint_path:
            self._load_checkpoint()
            _checkpointed_estimators.add(self)

    def observe(self, value: float, now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        with self._lock:
            self._rotate(now)
            self._buckets[-1][1].insert(value)

    def query(self, quantile: float, now: Optional[float] = None) -> float:
        now = time.time() if now is None else now
        with self._lock:
            self._rotate(now)
            merged = _Stream(self._objectives)
            for _, stream in self._buckets:
                stream.flush()
                merged._samples.extend(stream._samples)
                merged._n += stream._n
            merged._samples.sort(key=lambda sample: sample[0])
            return merged.query(quantile)

    def _rotate(self, now: float) -> None:
        rotated = False
        if not self._buckets:
            self._buckets.append((now, _Stream(self._objectives)))
        while now - self._buckets[-1][0] >= self._bucket_duration:
            self._buckets[-1][1].flush()
            start = self._buckets[-1][0] + self._bucket_duration
            if start + self._max_age < now:
                # The process was idle longer than the window; do not
                # create buckets that are already fully expired.
                start = now
            self._buckets.append((start, _Stream(self._objectives)))
            rotated = True
        cutoff = now - self._max_age
        live = [bucket for bucket in self._buckets if bucket[0] + self._bucket_duration > cutoff]
        if len(live) != len(self._buckets):
            self._buckets = live
            rotated = True
        if rotated:
            self._save_checkpoint()

    def flush(self) -> None:
        """Persist the compressed state to the checkpoint, if configured."""
        with self._lock:
            for _, stream in self._buckets:
                stream.flush()
            self._save_checkpoint()

    def _save_checkpoint(self) -> None:
        if not self._checkpoint_path:
            return
        data = {
            'objectives': {repr(q): e for q, e in self._objectives.items()},
            'max_age': self._max_age,
            'age_buckets': self._age_buckets,
            'buckets': [
                {'start': start, 'stream': stream.to_dict()}
                for start, stream in self._buckets
            ],
        }
        tmp_path = self._checkpoint_path + '.tmp'
        with open(tmp_path, 'w') as f:
            json.dump(data, f)
        os.replace(tmp_path, self._checkpoint_path)

    def _load_checkpoint(self) -> None:
        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        try:
            objectives = {float(q): float(e) for q, e in data['objectives'].items()}
            if (objectives != self._objectives
                    or float(data['max_age']) != self._max_age
                    or int(data['age_buckets']) != self._age_buckets):
                # Configuration changed; the old state is not comparable.
                return
            now = time.time()
            cutoff = now - self._max_age
            for bucket in data['buckets']:
                start = float(bucket['start'])
                if start + self._bucket_duration <= cutoff:
                    continue
                stream = _Stream.from_dict(self._objectives, bucket['stream'])
                self._buckets.append((start, stream))
        except (KeyError, TypeError, ValueError):
            self._buckets = []


_checkpointed_estimators: weakref.WeakSet = weakref.WeakSet()


def _flush_all_checkpoints() -> None:
    for estimator in list(_checkpointed_estimators):
        try:
            estimator.flush()
        except OSError:
            pass


atexit.register(_flush_all_checkpoints)
