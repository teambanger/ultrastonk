import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("verify", Path(__file__).parents[1] / "verify.py")
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class VerifySyncTest(unittest.TestCase):
    def test_run_identifier_validation(self):
        self.assertEqual(verify.run_identifier("evidence01"), "Vevidence01")
        for invalid in ("", "../bad", "a" * 11):
            with self.assertRaises(ValueError):
                verify.run_identifier(invalid)

    def test_gate_requires_file_and_times_out(self):
        with self.assertRaises(TimeoutError):
            verify.wait_for_file("/nonexistent/verifier-ready", 0)
        verify.wait_for_file(__file__, 0)


if __name__ == "__main__":
    unittest.main()
