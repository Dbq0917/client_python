import json
import os
import random
import tempfile
import unittest

from prometheus_client.quantiles import Estimator


class TestEstimator(unittest.TestCase):
    def test_query_empty(self):
        estimator = Estimator({0.5: 0.05})
        self.assertEqual(0.0, estimator.query(0.5))

    def test_quantiles_within_error_bounds(self):
        objectives = {0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
        estimator = Estimator(objectives)
        for i in range(1, 10001):
            estimator.observe(float(i))
        self.assertAlmostEqual(5000, estimator.query(0.5), delta=2 * 0.05 * 10000)
        self.assertAlmostEqual(9000, estimator.query(0.9), delta=2 * 0.01 * 10000)
        self.assertAlmostEqual(9900, estimator.query(0.99), delta=2 * 0.001 * 10000)

    def test_quantiles_with_unordered_observations(self):
        objectives = {0.5: 0.05, 0.9: 0.01}
        estimator = Estimator(objectives)
        values = list(range(1, 5001))
        random.Random(42).shuffle(values)
        for value in values:
            estimator.observe(float(value))
        self.assertAlmostEqual(2500, estimator.query(0.5), delta=2 * 0.05 * 5000)
        self.assertAlmostEqual(4500, estimator.query(0.9), delta=2 * 0.01 * 5000)

    def test_window_expires_old_observations(self):
        estimator = Estimator({0.5: 0.05}, max_age=10.0, age_buckets=5)
        for _ in range(20):
            estimator.observe(1.0, now=0.0)
        for _ in range(5):
            estimator.observe(100.0, now=5.0)
        self.assertEqual(1.0, estimator.query(0.5, now=5.0))
        # At t=12 the bucket holding the t=0 observations has expired.
        self.assertEqual(100.0, estimator.query(0.5, now=12.0))
        # At t=16 everything has expired.
        self.assertEqual(0.0, estimator.query(0.5, now=16.0))

    def test_invalid_objectives(self):
        self.assertRaises(ValueError, Estimator, {})
        self.assertRaises(ValueError, Estimator, {0.0: 0.05})
        self.assertRaises(ValueError, Estimator, {1.0: 0.05})
        self.assertRaises(ValueError, Estimator, {0.5: 0.0})
        self.assertRaises(ValueError, Estimator, {0.5: 1.0})
        self.assertRaises(ValueError, Estimator, {0.5: 0.05}, max_age=0)
        self.assertRaises(ValueError, Estimator, {0.5: 0.05}, age_buckets=0)


class TestEstimatorCheckpoint(unittest.TestCase):
    def test_restart_resumes_quantiles(self):
        objectives = {0.5: 0.05, 0.9: 0.01}
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            path = os.path.join(checkpoint_dir, 's.ckpt.json')
            estimator = Estimator(objectives, checkpoint_path=path)
            for i in range(1, 1001):
                estimator.observe(float(i))
            estimator.flush()

            restarted = Estimator(objectives, checkpoint_path=path)
            self.assertAlmostEqual(500, restarted.query(0.5), delta=2 * 0.05 * 1000)
            self.assertAlmostEqual(900, restarted.query(0.9), delta=2 * 0.01 * 1000)

            # The restored estimator keeps accumulating observations.
            for i in range(1001, 2001):
                restarted.observe(float(i))
            self.assertAlmostEqual(1000, restarted.query(0.5), delta=2 * 0.05 * 2000)

    def test_checkpoint_does_not_store_raw_observations(self):
        objectives = {0.5: 0.05, 0.9: 0.01, 0.99: 0.001}
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            path = os.path.join(checkpoint_dir, 's.ckpt.json')
            estimator = Estimator(objectives, checkpoint_path=path)
            rng = random.Random(42)
            for _ in range(100000):
                estimator.observe(rng.random())
            estimator.flush()

            with open(path) as f:
                data = json.load(f)
            stored = sum(len(bucket['stream']['samples']) for bucket in data['buckets'])
            self.assertLess(stored, 1000)
            self.assertLess(os.path.getsize(path), 100 * 1024)

    def test_checkpoint_ignored_when_config_changes(self):
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            path = os.path.join(checkpoint_dir, 's.ckpt.json')
            estimator = Estimator({0.5: 0.05}, checkpoint_path=path)
            for i in range(1, 1001):
                estimator.observe(float(i))
            estimator.flush()

            restarted = Estimator({0.5: 0.01}, checkpoint_path=path)
            self.assertEqual(0.0, restarted.query(0.5))

    def test_corrupt_checkpoint_ignored(self):
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            path = os.path.join(checkpoint_dir, 's.ckpt.json')
            with open(path, 'w') as f:
                f.write('not json')
            estimator = Estimator({0.5: 0.05}, checkpoint_path=path)
            estimator.observe(1.0)
            self.assertEqual(1.0, estimator.query(0.5))

    def test_expired_checkpoint_buckets_dropped(self):
        with tempfile.TemporaryDirectory() as checkpoint_dir:
            path = os.path.join(checkpoint_dir, 's.ckpt.json')
            estimator = Estimator({0.5: 0.05}, max_age=10.0, age_buckets=5, checkpoint_path=path)
            estimator.observe(1.0, now=0.0)
            estimator.flush()

            # Restarting long after max_age must not resurrect stale data.
            restarted = Estimator({0.5: 0.05}, max_age=10.0, age_buckets=5, checkpoint_path=path)
            self.assertEqual(0.0, restarted.query(0.5))


if __name__ == '__main__':
    unittest.main()
