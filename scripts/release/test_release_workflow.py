#!/usr/bin/env python3
"""Contract tests for the GitHub Actions workflows that ship the release.

Two properties break silently and are only observable when a workflow actually
runs: an action reference that is not pinned to a commit SHA, and a ``gh``
invocation inside a job that has no checkout for ``gh`` to resolve the
repository from. The assertions are text-based on purpose — the helper suites
are standard-library only, so no YAML parser is available.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
WORKFLOW_DIR = ROOT / ".github" / "workflows"
USES = re.compile(r"^\s*(?:-\s+)?uses:\s*(\S+)")
SHA_PINNED = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
# Steps are the only 6-space list items that carry a `run:` block; splitting on
# them is enough to attribute a command to the step whose env must declare the
# repository.
STEP_BOUNDARY = re.compile(r"(?m)^      - ")
GH_COMMAND = re.compile(r"(?m)^\s*gh\s+[a-z]")


def workflow_files() -> list[Path]:
    return sorted(WORKFLOW_DIR.glob("*.yml"))


class WorkflowContractTests(unittest.TestCase):
    def test_workflow_directory_is_not_empty(self) -> None:
        # Guards the two tests below against silently passing over zero files.
        self.assertTrue(workflow_files(), f"no workflow files under {WORKFLOW_DIR}")

    def test_every_action_reference_is_pinned_to_a_commit_sha(self) -> None:
        unpinned: list[str] = []
        for path in workflow_files():
            for number, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            ):
                match = USES.match(line)
                if not match:
                    continue
                reference = match.group(1)
                # A local reusable workflow is versioned by this repository's
                # own commit; only third-party actions need a SHA.
                if reference.startswith("./"):
                    continue
                if not SHA_PINNED.match(reference):
                    unpinned.append(f"{path.name}:{number}: {reference}")
        self.assertEqual(unpinned, [], f"unpinned action references: {unpinned}")

    def job(self, text: str, name: str) -> str:
        match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [a-z_-]+:|\Z)", text)
        self.assertIsNotNone(match, f"missing job {name}")
        return match.group(1)

    def test_release_quality_and_build_share_resolved_source_commit(self) -> None:
        text = (WORKFLOW_DIR / "release.yml").read_text(encoding="utf-8")
        quality = self.job(text, "quality")
        self.assertIn("needs: prepare", quality)
        self.assertIn("source_commit: ${{ needs.prepare.outputs.source_commit }}", quality)
        for name in ("build", "assemble"):
            with self.subTest(job=name):
                job = self.job(text, name)
                self.assertIn("ref: ${{ needs.prepare.outputs.source_commit }}", job)
                self.assertNotIn("ref: ${{ needs.prepare.outputs.tag }}", job)

    def test_reusable_ci_checks_the_requested_immutable_source(self) -> None:
        text = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
        self.assertIn("      source_commit:", text)
        source = self.job(text, "source")
        self.assertIn("inputs.source_commit || github.sha", source)
        self.assertIn("^[0-9a-f]{40}$", source)
        for name in ("test", "installer", "deny", "msrv"):
            with self.subTest(job=name):
                job = self.job(text, name)
                self.assertIn("needs: source", job)
                self.assertIn("ref: ${{ needs.source.outputs.commit }}", job)
        msrv = self.job(text, "msrv")
        self.assertIn('toolchain: "1.90.0"', msrv)
        self.assertIn("cargo +1.90.0 check --workspace --all-targets --all-features --locked", msrv)

    def test_gh_steps_declare_the_repository_they_act_on(self) -> None:
        """`gh` resolves its repository from a git remote, `GH_REPO`, or
        `--repo`; it never falls back to `GITHUB_REPOSITORY`."""
        release = WORKFLOW_DIR / "release.yml"
        text = release.read_text(encoding="utf-8")
        offenders: list[str] = []
        gh_steps = 0
        for block in STEP_BOUNDARY.split(text):
            if not GH_COMMAND.search(block):
                continue
            gh_steps += 1
            if "GH_REPO:" not in block and "--repo" not in block:
                offenders.append(block.splitlines()[0].strip())
        self.assertGreater(gh_steps, 0, "no gh step found in release.yml")
        self.assertEqual(
            offenders, [], f"gh steps without a repository: {offenders}"
        )


if __name__ == "__main__":
    unittest.main()
