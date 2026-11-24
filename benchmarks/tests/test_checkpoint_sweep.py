import json
import unittest

from benchmarks.checkpoint_sweep import checkpoint_work


def completion(second, number, size, duration, job="abc"):
    return "jobmanager-1 | " + json.dumps({
        "timestamp": f"1970-01-01T00:00:{second:02d}.000Z",
        "logger": "org.apache.flink.runtime.checkpoint.CheckpointCoordinator",
        "message": (f"Completed checkpoint {number} for job {job} "
                    f"({size} bytes, checkpointDuration={duration} ms, finalizationTime=1 ms)."),
    })


class CheckpointWorkTests(unittest.TestCase):
    def test_excludes_warmup_and_drain_and_deduplicates_checkpoint_ids_per_job(self):
        first = completion(10, 1, 250, 25)
        log = "\n".join((completion(9, 0, 500, 100), first, first,
                         completion(15, 1, 350, 35, job="def"), completion(20, 2, 1000, 500)))
        self.assertEqual(checkpoint_work(log, 10, 20), {
            "completed": 2, "reported_bytes": 600, "reported_bytes_per_second": 60,
            "median_duration_ms": 30,
        })

    def test_missing_checkpoints_are_not_reported_as_zero_cost(self):
        with self.assertRaisesRegex(ValueError, "no checkpoint completions"):
            checkpoint_work(completion(9, 1, 200, 20), 10, 20)
