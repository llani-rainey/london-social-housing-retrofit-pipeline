"""
pipeline.py — Run the full pipeline end to end

Usage:
    python pipeline.py

Stages:
    1. ingest_epc   — raw EPC CSV → bronze → silver
    2. ingest_core  — raw CORE CSV → bronze → silver
    3. build_gold   — silver EPC + CORE → gold analysis tables
"""

import subprocess
import sys
import time

STAGES = [
    ("EPC Ingest",  "src/ingest_epc.py"),
    ("CORE Ingest", "src/ingest_core.py"),
    ("Build Gold",  "src/build_gold.py"),
]

def run_stage(name, script):
    print(f"\n{'='*60}")
    print(f"  STAGE: {name}")
    print(f"{'='*60}")
    start = time.time()
    result = subprocess.run([sys.executable, script], capture_output=False)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"\n✗ {name} FAILED after {elapsed:.1f}s")
        sys.exit(1)
    print(f"\n✓ {name} completed in {elapsed:.1f}s")

if __name__ == "__main__":
    total_start = time.time()
    for name, script in STAGES:
        run_stage(name, script)
    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  Pipeline complete in {total:.1f}s")
    print(f"{'='*60}")
