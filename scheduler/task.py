#!/usr/bin/env python3

"""
SPIT-Browser Scheduler: Task Management Library
"""

from threading import Lock, Timer
from typing import Callable, List, Set

TIMEOUT: int = 60

class Task(dict):
  """
  Representation of a Task.

  Keys:
    task_id (str): Task's ID: {client_id}~{vertex_id}~{worker_id}.
    client_id (str): Initiating client's ID.
    vertex_id (int): Task's index within the job.
    worker_id (str): Assigned worker's ID.
    program (str): JavaScript program for this task.
    contacts (List[str]): ID's for this task's contacts.
    update (bool): Whether this task needs to be updated on the worker.
    cancel (bool): Whether this task needs to be cancelled on the worker.
  """
  def __init__(self, client_id: str, vertex_id: int, worker_id: str,
               program: str, contacts: List[str]):
    dict.__init__(self)
    self['task_id'] = f'{client_id}~{vertex_id}~{worker_id}'
    self['client_id'] = client_id
    self['vertex_id'] = vertex_id
    self['worker_id'] = worker_id
    self['program'] = program
    self['contacts'] = contacts
    self['update'] = False
    self['cancel'] = False


class Worker(dict):
  """
  Tracks the status of a Worker.

  Keys:
    worker_id (str): ID of worker.
    n_cores (int): Number of cores on the worker.
    active_tasks (List[Task]): Currently running tasks.
    pending_tasks (List[Task]): Tasks to be sent to the worker.
  """
  def __init__(self, worker_id: int, n_cores: int,
               deregister: Callable[[int], None]):
    dict.__init__(self)
    self['worker_id'] = worker_id
    self['n_cores'] = n_cores
    self['active_tasks'] = []
    self['pending_tasks'] = []
    self.deregister: Callable[[int], None] = deregister
    self.timer: Timer = Timer(TIMEOUT, deregister, [worker_id])
    if 'immortal' not in worker_id:
      self.timer.start()
    self.heartbeat_lock: Lock = Lock()

  def availability(self) -> int:
    """
    Returns:
      (int): Number of free cores on the worker.
    """
    return self['n_cores'] - len(self['active_tasks']) \
                           - len(self['pending_tasks'])

  def heartbeat(self, active_tasks: List[str]) -> List[Task]:
    """
    Args:
      active_tasks (List[str]): List of ID's of still-running tasks.

    Returns:
      (List[Task]): List of new tasks for the worker to run.
    """
    self.heartbeat_lock.acquire()

    # Remove completed tasks.
    active_set: Set[str] = set(active_tasks)
    self['active_tasks'] = list(filter(
      lambda task: task['task_id'] in active_set,
      self['active_tasks']
    ))

    # Create list of new tasks.
    n_send: int = self['n_cores'] - len(self['active_tasks'])
    send: List[Task] = self['pending_tasks'][:n_send]
    send_copy: List[Task] = [task.copy() for task in send]
    for task in send:
      task['update'] = False

    # Update fields.
    self['active_tasks'] += send
    self['pending_tasks'] = self['pending_tasks'][n_send:]
    self.timer.cancel()
    self.timer = Timer(TIMEOUT, self.deregister, [self['worker_id']])
    if 'immortal' not in self['worker_id']:
      self.timer.start()
    self.heartbeat_lock.release()
    return send_copy
