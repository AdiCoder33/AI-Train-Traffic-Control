from __future__ import annotations

from pathlib import Path
import json


def main(scope: str, date: str) -> None:
    base = Path("artifacts") / scope / date
    traj = base / "rl_trajectories.jsonl"
    # Append a dummy trajectory line for demonstration
    sample = {"state": "...", "action": "...", "outcome": 0.0}
    with traj.open("a", encoding="utf-8") as f:
        f.write(json.dumps(sample) + "\n")


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1], sys.argv[2])

