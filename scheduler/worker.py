#!/usr/bin/env python3

"""
Worker Management Library for SPIT-Browser Scheduler
"""

from typing import Any, Dict, List

class Task(dict):
  def __init__(self, job_id: int, task_id: int, worker_id: int, program: str):
    self['job_id'] = job_id  # int
    self['task_id'] = task_id  # int
    self['worker_id'] = worker_id  # int
    self['program'] = program  # str
    self['sinks'] = []  # List[TaskReference]


class TaskReference(dict):
  def __init__(self, task: Task, workers: List['Worker']):
    self['job_id'] = task['job_id']  # int
    self['task_id'] = task['task_id']  # int
    self['worker_id'] = task['worker_id']  # int
    self['worker_address'] = workers[task['worker_id']]['address']  # str


class Job(dict):
  def __init__(self, job_id: int, tasks: List[Task]):
    self['job_id'] = job_id  # int
    self['tasks'] = tasks  # List[Task]


class Worker(dict):
  def __init__(self, worker_id: int, n_cores: int, address: str):
    self['worker_id'] = worker_id  # int
    self['n_cores'] = n_cores  # int
    self['address'] = address  # int
    self['active_tasks'] = []  # List[Union[TaskReference, Task]]
    self['pending_tasks'] = []  # List[Task]

  def availability(self) -> int:
    return self['n_cores'] - len(self['active_tasks']) - len(self['pending_tasks'])

  def heartbeat(self, active_tasks: List[TaskReference]) -> List[Task]:
    n_send: int = self['n_cores'] - len(active_tasks)
    self['active_tasks'] = active_tasks + self['pending_tasks'][:n_send]
    self['pending_tasks'] = self['pending_tasks'][n_send:]
    return self['active_tasks'][:n_send]
