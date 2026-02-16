import unittest

from tools.quality_check import build_steps


class QualityCheckTests(unittest.TestCase):
    def test_build_steps_default(self) -> None:
        steps = build_steps(include_lint=False, include_env_check=False)
        self.assertEqual([step.name for step in steps], ["compile", "tests"])

    def test_build_steps_with_optional_checks(self) -> None:
        steps = build_steps(include_lint=True, include_env_check=True)
        self.assertEqual([step.name for step in steps], ["compile", "tests", "ruff", "pip-check"])
        self.assertTrue(steps[2].optional)
        self.assertEqual(steps[2].required_bin, "ruff")


if __name__ == "__main__":
    unittest.main()
