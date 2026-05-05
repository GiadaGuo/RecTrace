"""
iteration_budget.py
-------------------
Thread-safe iteration counter that prevents the Agent from running forever.

The orchestrator creates one IterationBudget per request and passes it through
AgentState.  On each LLM call the budget is decremented; when it reaches zero
the orchestrator forces a FINAL_ANSWER instead of allowing another tool call.

Max iterations:
  default   30  (configurable via env AGENT_MAX_ITERATIONS)
  Refund:   the ``execute_code`` tool may refund 1 iteration per call to give
            the agent extra room when running real code.

Usage::

    budget = IterationBudget()
    if not budget.consume():
        # force finish
    budget.refund(1)           # only from execute_code
    budget.remaining           # read-only
"""

from __future__ import annotations

import os
import threading

_DEFAULT_MAX = int(os.environ.get("AGENT_MAX_ITERATIONS", "30"))


class IterationBudget:
    """
    Thread-safe iteration counter.

    Parameters
    ----------
    max_iterations : int
        Hard cap on LLM call count.  Defaults to env AGENT_MAX_ITERATIONS (30).
    """

    def __init__(self, max_iterations: int = _DEFAULT_MAX) -> None:
        self._max = max_iterations
        self._used = 0
        self._lock = threading.Lock()

    # ── public API ────────────────────────────────────────────────────────────

    def consume(self) -> bool:
        """
        Claim one iteration.

        Returns True if the budget allows the call, False if exhausted.
        """
        with self._lock:
            if self._used >= self._max:
                return False
            self._used += 1
            return True

    def refund(self, n: int = 1) -> None:
        """Return n iterations to the budget (minimum 0 used)."""
        with self._lock:
            self._used = max(0, self._used - n)

    # ── properties ────────────────────────────────────────────────────────────

    @property
    def remaining(self) -> int:
        with self._lock:
            return max(0, self._max - self._used)

    @property
    def used(self) -> int:
        with self._lock:
            return self._used

    @property
    def exhausted(self) -> bool:
        return self.remaining == 0

    def __repr__(self) -> str:
        return f"IterationBudget(used={self.used}/{self._max})"
