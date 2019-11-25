#!/usr/bin/env python3

"""
Task Management Library for SPIT-Browser Scheduler
"""

from threading import Lock
from typing import Any, Dict, List

class Task(dict):
  """
  Representation of a Task.

  Keys:
    job_id (int): Job to which this task belongs.
    task_id (int): Task's ID within the job.
    worker_id (int): Worker to which this task is assigned.
    client_address (str): Address of requesting client.
    program (str): JavaScript program for this task.
    sinks (List[TaskPointer]): TaskPointers for this task's outgoing edges.
  """
  def __init__(self, job_id: int, task_id: int, worker_id: int,
               client_address: str, program: str, sinks: List['TaskPointer']):
    self['job_id'] = job_id
    self['task_id'] = task_id
    self['worker_id'] = worker_id
    self['client_address'] = client_address
    self['program'] = program
    self['sinks'] = sinks


class TaskPointer(dict):
  """
  All the information needed to locate a task. Created by joining a Task with
  the list of Workers.

  Keys:
    job_id (int): Job to which this task belongs.
    task_id (int): Task's ID within the job.
    worker_id (int): Worker to which this task is assigned.
    worker_address (str): IP address and port of the worker.
  """
  def __init__(self, task: Task, workers: List['Worker']):
    self['job_id'] = task['job_id']
    self['task_id'] = task['task_id']
    self['worker_id'] = task['worker_id']
    self['worker_address'] = workers[task['worker_id']]['address']


class Job(dict):
  """
  Representation of a Job.

  Keys:
    job_id (int): Job's ID.
    tasks (List[Task]): List of Tasks making up this Job.
  """
  def __init__(self, job_id: int, tasks: List[Task]):
    self['job_id'] = job_id
    self['tasks'] = tasks


class Worker(dict):
  """
  Tracks the status of a Worker.

  Keys:
    worker_id (int): ID assigned to the worker.
    n_cores (int): Number of cores on the worker.
    address (str): IP address and port of the worker.
    active_tasks (List[Union[TaskPointer, Task]]): Currently running tasks.
    pending_tasks (List[Task]): Tasks to be sent to the worker.
  """
  def __init__(self, worker_id: int, n_cores: int, address: str):
    self['worker_id'] = worker_id
    self['n_cores'] = n_cores
    self['address'] = address
    self['active_tasks'] = []
    self['pending_tasks'] = []
    self.heartbeat_lock: Lock = Lock()

  def availability(self) -> int:
    """
    Returns:
      (int): Number of free cores on the worker.
    """
    return self['n_cores'] - len(self['active_tasks']) - len(self['pending_tasks'])

  def heartbeat(self, active_tasks: List[TaskPointer]) -> List[Task]:
    """
    Args:
      active_tasks (List[TaskPointer]): List of still-running tasks.

    Returns:
      (List[Task]): List of new tasks for the worker to run.
    """
    self.heartbeat_lock.acquire()
    n_send: int = self['n_cores'] - len(active_tasks)
    send: List[Task] = self['pending_tasks'][:n_send]
    self['active_tasks'] = active_tasks + send
    self['pending_tasks'] = self['pending_tasks'][n_send:]
    self.heartbeat_lock.release()
    return send
