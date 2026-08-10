"""Pytest configuration: puts every service's own directory (plus the
shared common lib) on sys.path so tests can import service modules exactly
the way each service's own main.py does at runtime (e.g. `from validation
import ValidationEngine`), without needing package installs.

Note: no test imports any service's `config.py` or `main.py` directly
(those need live Kafka/Postgres env vars) — only the pure-logic modules
(validation, feed_health, features/statistical/isolation_forest_detector,
injector/generator, stats). That keeps the several identically-named
`config.py`/`main.py` files across services from colliding in
sys.modules even though all their directories are on sys.path at once.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

for rel in [
    "services/common",
    "services/ingestion",
    "services/anomaly_detection",
    "services/monitoring",
    "simulator",
    "api",
]:
    path = str(ROOT / rel)
    if path not in sys.path:
        sys.path.insert(0, path)
