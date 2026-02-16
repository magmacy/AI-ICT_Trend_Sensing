from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CheckStep:
    name: str
    command: list[str]
    optional: bool = False
    required_bin: str | None = None


def build_steps(include_lint: bool, include_env_check: bool) -> list[CheckStep]:
    steps = [
        CheckStep(
            name="compile",
            command=[sys.executable, "-m", "compileall", "-q", "."],
        ),
        CheckStep(
            name="tests",
            command=[sys.executable, "-m", "pytest", "-q"],
        ),
    ]

    if include_lint:
        steps.append(
            CheckStep(
                name="ruff",
                command=["ruff", "check", "."],
                optional=True,
                required_bin="ruff",
            )
        )

    if include_env_check:
        steps.append(
            CheckStep(
                name="pip-check",
                command=[sys.executable, "-m", "pip", "check"],
            )
        )

    return steps


def run_step(step: CheckStep, cwd: Path) -> bool:
    if step.required_bin and shutil.which(step.required_bin) is None:
        if step.optional:
            print(f"[skip] {step.name}: missing `{step.required_bin}`")
            return True
        print(f"[fail] {step.name}: missing `{step.required_bin}`")
        return False

    started = time.perf_counter()
    print(f"[run] {step.name}: {' '.join(step.command)}")
    proc = subprocess.run(step.command, cwd=str(cwd))
    elapsed = time.perf_counter() - started

    if proc.returncode != 0:
        print(f"[fail] {step.name} ({elapsed:.2f}s)")
        return False

    print(f"[ok] {step.name} ({elapsed:.2f}s)")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run project quality checks.")
    parser.add_argument("--lint", action="store_true", help="Run lint check (ruff) when installed.")
    parser.add_argument("--env-check", action="store_true", help="Run dependency consistency check (pip check).")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    steps = build_steps(include_lint=args.lint, include_env_check=args.env_check)

    started = time.perf_counter()
    for step in steps:
        if not run_step(step, cwd=repo_root):
            return 1

    elapsed = time.perf_counter() - started
    print(f"[done] all checks passed ({elapsed:.2f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
