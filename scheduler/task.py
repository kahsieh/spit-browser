#!/usr/bin/env python3

"""
SPIT-Browser Scheduler: Task Management Library
"""

from threading import Lock, Timer
from typing import Callable, List, Tuple

TIMEOUT: int = 60

class Task(dict):
  """
  Representation of a Task.

  Keys:
    client_id (str): Peer ID of initiating client.
    task_id (int): Task's ID within the job.
    worker_id (str): Peer ID of assigned worker.
    program (str): JavaScript program for this task.
    contacts (List[TaskPointer]): TaskPointers for this task's contacts.
  """
  def __init__(self, client_id: str, task_id: int, worker_id: str,
               program: str, contacts: List['TaskPointer']):
    dict.__init__(self)
    self['client_id'] = client_id
    self['task_id'] = task_id
    self['worker_id'] = worker_id
    self['program'] = program
    self['contacts'] = contacts


class TaskPointer(dict):
  """
  Partial representation of a Task that is sufficient to locate its instance.

  Keys:
    client_id (str): Peer ID of initiating client.
    task_id (int): Task's ID within the job.
    worker_id (str): Peer ID of assigned worker.
  """
  def __init__(self, task: Task):
    dict.__init__(self)
    self['client_id'] = task['client_id']
    self['task_id'] = task['task_id']
    self['worker_id'] = task['worker_id']


class Worker(dict):
  """
  Tracks the status of a Worker.

  Keys:
    worker_id (str): Peer ID of worker.
    n_cores (int): Number of cores on the worker.
    active_tasks (List[Task]): Currently running tasks.
    pending_tasks (List[Task]): Tasks to be sent to the worker.
  """
  def __init__(self, worker_id: int, n_cores: int, deregister: Callable[[int], None]):
    dict.__init__(self)
    self['worker_id'] = worker_id
    self['n_cores'] = n_cores
    self['active_tasks'] = []
    self['pending_tasks'] = []
    self.heartbeat_lock: Lock = Lock()
    self.deregister: Callable[[int], None] = deregister
    self.timer: Timer = Timer(TIMEOUT, deregister, [worker_id])
    if 'immortal' not in worker_id:
      self.timer.start()

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

    # Remove completed tasks.
    active_set: Set[Tuple[str, int]] = set(
      (ptr['client_id'], ptr['task_id'])
      for ptr in active_tasks
    )
    self['active_tasks'] = list(filter(
      lambda task: (task['client_id'], task['task_id']) in active_set,
      self['active_tasks']
    ))

    # Create list of new tasks.
    n_send: int = self['n_cores'] - len(self['active_tasks'])
    send: List[Task] = self['pending_tasks'][:n_send]

    # Update fields.
    self['active_tasks'] += send
    self['pending_tasks'] = self['pending_tasks'][n_send:]
    self.heartbeat_lock.release()
    self.timer.cancel()
    self.timer = Timer(TIMEOUT, self.deregister, [self['worker_id']])
    if 'immortal' not in self['worker_id']:
      self.timer.start()
    return send
