# scripts/scrape_all.py
"""
Run all vendor scrapers sequentially as separate processes, then resolve into canonical tables.
Usage:
  py scripts\scrape_all.py           # scrape + resolve
  py scripts\scrape_all.py --no-resolve  # scrape only
"""
from __future__ import annotations

import sys
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

def run(cmd: list[str]) -> None:
    print(f"\n=== Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def main() -> None:
    # Ensure schema exists
    run([sys.executable, str(HERE / "migrate.py")])

    runners = [
        "scrape_toolstation.py",
        "scrape_screwfix.py",
        "scrape_toolstop.py",
        "scrape_dandm.py",
        "scrape_ukplanettools.py",
    ]
    for script in runners:
        run([sys.executable, str(HERE / script)])

    if "--no-resolve" not in sys.argv:
        run([sys.executable, str(HERE / "resolver.py")])

    print("\nAll done.")

if __name__ == "__main__":
    main()
