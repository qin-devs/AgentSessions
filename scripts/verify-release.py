#!/usr/bin/env python3
"""agent-session-grep release verification script (#10).

Runs a bounded synthetic end-to-end release check against an already-built
binary: binary/version → sync → lexical search → get → context → resume
metadata → resume dry-run → deterministic handoff evidence → bigram-hash
embedding index plus semantic/hybrid effective modes → hook default-off →
provider capability matrix. The local loopback serve surface is intentionally
out of scope; it has a dedicated smoke harness (the five-entry-point
consistency harness exercises it for real).

Note on "semantic": this check isolates the local model cache and exercises
``bigram-hash-v1``, a fuzzy
lexical bigram hash — explicitly NOT a semantic model. The check verifies the
semantic/hybrid effective modes are reachable and correctly labelled, not that
the project performs semantic retrieval.

Usage:
    python scripts/verify-release.py --asg ./target/debug/agent-session-grep
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def _force_utf8_output() -> None:
    """Keep the verdict printable on any host code page.

    ``sys.stdout`` uses the platform encoding with ``errors="strict"``, and a
    captured CI step's stdout is a pipe rather than a console, so on a Windows
    runner it is the ANSI code page (cp1252). Printing the ✓/✗ status marks
    there raises UnicodeEncodeError and the release gate dies with a traceback
    instead of a verdict. Force UTF-8 (CI logs are UTF-8) and degrade an
    unencodable character rather than aborting the run. ``sys.stderr`` already
    defaults to ``backslashreplace``; it is included so both streams agree.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")


_force_utf8_output()


class VerificationError(Exception):
    pass


def parse_first_json_line(output: str) -> dict[str, Any]:
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("{"):
            return json.loads(stripped)
    raise VerificationError("expected a JSON frame on stdout")


def run_asg_raw(
    asg_bin: str,
    data_root: str,
    args: list[str],
    *,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run against synthetic state, preserving raw stdout for hook checks."""
    env = os.environ.copy()
    env["ASG_DATA_ROOT"] = data_root
    # --db isolates the catalog, not platform config/model discovery. Keep
    # installed user models out of this deterministic bigram-hash smoke.
    for key in (
        "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
        "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
    ):
        env[key] = str(Path(data_root) / key.lower())
    db = str(Path(data_root) / "asg.db")
    result = subprocess.run(
        [asg_bin, "--db", db, "--output", "json"] + args,
        input=stdin,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    if result.returncode != 0:
        raise VerificationError(
            f"command {' '.join(args)} exited {result.returncode}: "
            f"{result.stderr.strip()}"
        )
    return result


def run_asg(
    asg_bin: str,
    data_root: str,
    args: list[str],
    *,
    stdin: str | None = None,
) -> dict[str, Any]:
    """Run an enveloped command and return the first parsed JSON frame."""
    result = run_asg_raw(asg_bin, data_root, args, stdin=stdin)
    return parse_first_json_line(result.stdout)


def step(name: str, ok: bool, detail: str = "") -> bool:
    status = "✓" if ok else "✗"
    print(f"  {status} {name}: {detail}" if detail else f"  {status} {name}")
    return ok


def verify_build(asg_bin: str) -> bool:
    if not Path(asg_bin).is_file():
        return step("binary exists", False, f"{asg_bin} not found")
    frame = run_asg(asg_bin, tempfile.mkdtemp(), ["--version"])
    version = frame.get("data", {}).get("version")
    return step("binary/version", isinstance(version, str) and version != "", str(version))


def verify_sync(asg_bin: str, data_root: str) -> bool:
    fixture = (
        Path(__file__).parent
        / "evidence"
        / "fixtures"
        / "gate"
        / "claude"
        / "session-alpha.jsonl"
    )
    if not fixture.is_file():
        return step("sync", False, f"fixture not found: {fixture}")
    frame = run_asg(asg_bin, data_root, ["sync", str(fixture)])
    data = frame["data"]
    ok = data.get("committed", 0) > 0 and data.get("skipped", 1) == 0
    return step(
        "sync",
        ok,
        f"committed={data.get('committed')} skipped={data.get('skipped')}",
    )


def verify_search(asg_bin: str, data_root: str) -> bool:
    frame = run_asg(asg_bin, data_root, ["search", "retry", "--max-items", "5"])
    hits = frame["data"].get("hits", [])
    hit_ids = [hit.get("id") for hit in hits if isinstance(hit.get("id"), str)]
    ok = bool(hit_ids) and frame.get("retrieval_mode") == "lexical"
    return step("lexical search", ok, f"hits={len(hit_ids)} mode={frame.get('retrieval_mode')}")


def verify_get(asg_bin: str, data_root: str) -> bool:
    frame = run_asg(asg_bin, data_root, ["search", "retry", "--max-items", "1"])
    hits = frame["data"].get("hits", [])
    if not hits:
        return step("get", False, "no hit id from search")
    payload = run_asg(asg_bin, data_root, ["get", hits[0]["id"]])["data"].get("payload")
    ok = isinstance(payload, str) and "retry" in payload
    return step("get", ok, f"hit_id={hits[0]['id']}")


def verify_context(asg_bin: str, data_root: str) -> bool:
    frame = run_asg(asg_bin, data_root, ["search", "retry", "--max-items", "1"])
    hits = frame["data"].get("hits", [])
    if not hits:
        return step("context", False, "no session id from search")
    session_id = hits[0].get("session_id")
    if not isinstance(session_id, str):
        return step("context", False, "hit has no session_id")
    context = run_asg(asg_bin, data_root, ["context", session_id])["data"]
    messages = context.get("messages", [])
    evidence = context.get("evidence", [])
    ok = bool(messages) and len(evidence) >= 1
    return step("context", ok, f"messages={len(messages)} evidence={len(evidence)}")


def verify_resume(asg_bin: str, data_root: str) -> bool:
    frame = run_asg(asg_bin, data_root, ["search", "retry", "--max-items", "1"])
    session_id = frame["data"].get("hits", [{}])[0].get("session_id")
    if not isinstance(session_id, str):
        return step("resume dry-run", False, "no session id from search")
    metadata = run_asg(asg_bin, data_root, ["get-session-resume", session_id])["data"]
    resume = run_asg(asg_bin, data_root, ["resume", session_id])["data"]
    metadata_ok = {
        "session_id",
        "provider_id",
        "resume_available",
        "provider_session_id",
        "original_working_directory",
        "unavailable_reason",
    }.issubset(metadata)
    dry_run_ok = resume.get("executed") is False
    return step(
        "resume metadata + dry-run",
        metadata_ok and dry_run_ok,
        f"executed={resume.get('executed')}",
    )


def verify_handoff(asg_bin: str, data_root: str) -> bool:
    first = run_asg(asg_bin, data_root, ["handoff", "retry"])["data"]
    second = run_asg(asg_bin, data_root, ["handoff", "retry"])["data"]
    ok = (
        first.get("schema_version") == "1.0"
        and isinstance(first.get("pack_id"), str)
        and first.get("pack_id") == second.get("pack_id")
        and bool(first.get("evidence"))
        and not first.get("inference")
    )
    return step(
        "handoff evidence",
        ok,
        f"schema={first.get('schema_version')} pack_id={first.get('pack_id')} "
        f"evidence={len(first.get('evidence', []))} deterministic={first.get('pack_id') == second.get('pack_id')}",
    )


def verify_semantic(asg_bin: str, data_root: str) -> bool:
    embeddings = run_asg(asg_bin, data_root, ["index", "embeddings"])
    data = embeddings["data"]
    semantic = run_asg(
        asg_bin, data_root, ["search", "retry", "--mode", "semantic", "--max-items", "5"]
    )
    hybrid = run_asg(
        asg_bin, data_root, ["search", "retry", "--mode", "hybrid", "--max-items", "5"]
    )
    embedding_ok = (
        data.get("model_id") == "bigram-hash-v1"
        and data.get("dimension") == 384
        and data.get("indexed", 0) > 0
    )
    semantic_ok = semantic.get("retrieval_mode") == "semantic" and bool(
        semantic["data"].get("hits")
    )
    hybrid_ok = hybrid.get("retrieval_mode") == "hybrid" and bool(
        hybrid["data"].get("hits")
    )
    return step(
        "semantic/hybrid effective modes",
        embedding_ok and semantic_ok and hybrid_ok,
        "model=bigram-hash-v1 fuzzy lexical vector (not a semantic model), "
        f"modes={semantic.get('retrieval_mode')}/{hybrid.get('retrieval_mode')}",
    )


def verify_hook(asg_bin: str, data_root: str) -> bool:
    result = run_asg_raw(
        asg_bin,
        data_root,
        ["hook", "user-prompt-submit"],
        stdin='{"prompt":"retry"}',
    )
    # Hooks use bare hook protocol, not a CLI envelope. Disabled means no
    # stdout at all; even whitespace would be injected into the host context.
    return step("hook off by default", result.stdout == "", f"stdout_empty={result.stdout == ''}")


def verify_providers(asg_bin: str, data_root: str) -> bool:
    providers = run_asg(asg_bin, data_root, ["providers"])["data"].get("providers", [])
    maturities = {provider.get("maturity") for provider in providers}
    ids = {provider.get("provider_id") for provider in providers}
    ok = len(providers) == 16 and maturities == {"experimental", "unsupported"} and {
        "claude-code",
        "codex",
        "deepseek-harness",
        "zcode",
    }.issubset(ids)
    return step("providers matrix", ok, f"providers={len(providers)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="agent-session-grep release verification")
    parser.add_argument("--asg", required=True, help="Path to the built binary")
    args = parser.parse_args()

    # Resolve relative paths up front: subprocess later runs with cwd unchanged
    # only for the first check (verify_build uses its own tempdir), so pin the
    # absolute binary path before any check can chdir or rely on cwd.
    binary = str(Path(args.asg).expanduser().resolve())
    print("agent-session-grep release verification (#10)")
    print("=" * 50)
    print("Serve/Web is intentionally excluded: use the dedicated serve smoke.")

    data_root = tempfile.mkdtemp(prefix="asg-verify-")
    checks = [
        ("binary/version", verify_build, (binary,)),
        ("sync", verify_sync, (binary, data_root)),
        ("lexical search", verify_search, (binary, data_root)),
        ("get", verify_get, (binary, data_root)),
        ("context", verify_context, (binary, data_root)),
        ("resume metadata + dry-run", verify_resume, (binary, data_root)),
        ("handoff evidence", verify_handoff, (binary, data_root)),
        ("semantic/hybrid effective modes", verify_semantic, (binary, data_root)),
        ("hook off by default", verify_hook, (binary, data_root)),
        ("providers matrix", verify_providers, (binary, data_root)),
    ]
    results: list[bool] = []
    for index, (name, check, arguments) in enumerate(checks, 1):
        print(f"\n{index}. {name}:")
        try:
            results.append(check(*arguments))
        except (VerificationError, KeyError, TypeError, json.JSONDecodeError) as exc:
            results.append(step(name, False, str(exc)))

    shutil.rmtree(data_root, ignore_errors=True)
    passed = sum(results)
    total = len(results)
    print("\n" + "=" * 50)
    print(f"Result: {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
