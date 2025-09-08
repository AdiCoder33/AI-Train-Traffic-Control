"""Optional small MILP/CP-SAT local solver (stub).

Attempts to import PuLP or OR-Tools and solve a tiny local problem for a
single headway/block conflict (two trains near a block), deciding whether
to hold the follower or the leader (overtake), subject to headway.

If no solver is available, the module exposes SOLVER_AVAILABLE=False and
solve_local() returns None.
"""

from __future__ import annotations

from typing import Dict, Optional

SOLVER_AVAILABLE = False

try:
    import pulp  # type: ignore

    SOLVER_AVAILABLE = True
except Exception:
    try:
        from ortools.linear_solver import pywraplp  # type: ignore

        SOLVER_AVAILABLE = True
    except Exception:
        SOLVER_AVAILABLE = False


def solve_local(
    *,
    headway_min: float,
    follower_hold_min: float,
    leader_hold_min: float,
    follower_priority: int = 0,
    leader_priority: int = 0,
    time_limit_sec: int = 2,
) -> Optional[Dict[str, str]]:
    """Return a small decision: {'action': 'HOLD_FOLLOWER'|'HOLD_LEADER'}.

    This is a placeholder that prefers holding the lower-priority train
    if a solver is unavailable.
    """
    if not SOLVER_AVAILABLE:
        # Simple heuristic decision
        if follower_priority >= leader_priority:
            return {"action": "HOLD_LEADER"}
        return {"action": "HOLD_FOLLOWER"}

    # A minimal MILP could be set up here; to keep the footprint light and
    # avoid dependency issues, we currently return a priority-based choice.
    if follower_priority >= leader_priority:
        return {"action": "HOLD_LEADER"}
    return {"action": "HOLD_FOLLOWER"}

