#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
Worker Library
"""

from typing import List

class Worker:
  def __init__(self, address: str, n_cores: int):
    self._address: str = address
    self._n_cores: int = n_cores
    self._n_active: int = 0
    self._pending: List[str] = []

  def address(self) -> str:
    return self._address

  def usage(self) -> float:
    return (self._n_active + len(self._pending)) / self._n_cores

  def push(self, task: str) -> None:
    self._pending.append(task)

  def heartbeat(self, n_active: int) -> List[str]:
    n_send: int = min(self._n_cores - n_active, len(self._pending))
    tasks: List[str] = self._pending[:n_send]
    self._n_active = n_active + n_send
    self._pending = self._pending[n_send:]
    return tasks
