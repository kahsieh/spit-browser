#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
"""

from flask import abort, Flask, jsonify, request, Response
from threading import Lock
from typing import Any, Dict, List, Set, Tuple
from task import *

app: Flask = Flask(__name__)

# Client tracker maps client peer ID to an ordered list of tasks.
clients: Dict[str, List[Task]] = {}

# Worker tracker maps worker peer ID to the corresponding object.
workers: Dict[str, Worker] = {}

@app.route('/')
def root() -> Response:
  """
  Returns the state of the scheduler.

  Returns (JSON):
    clients (Dict[str, List[Task]]): All clients registered via /allocate.
    workers (Dict[str, Worker]): All workers registered via /register.
  """
  return jsonify({
    'clients': clients,
    'workers': workers,
  })


@app.route('/program')
def program() -> Response:
  """
  Returns a client-submitted program.

  Args (GET):
    client_id (str): Peer ID of initiating client.
    task_id (int): Task's ID within the job.

  Returns:
    A JavaScript file.
  """
  try:
    client_id: str = str(request.args.get('client_id'))
    task_id: int = int(str(request.args.get('task_id')))
    program: str = clients[client_id][task_id]['program']
    return Response(program, mimetype="text/javascript")
  except ValueError:
    abort(400)
  except (KeyError, IndexError):
    abort(404)


@app.route('/register', methods=['POST'])
def register() -> Response:
  """
  Registers a worker.

  Args (JSON):
    worker_id (str): Peer ID of worker.
    n_cores (int): Number of cores on the worker.

  Returns (JSON):
    success (bool): Whether registration succeeded.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    if req['worker_id'] in workers:
      abort(403)
    workers[req['worker_id']] = Worker(req['worker_id'], req['n_cores'], deregister)
    return jsonify({
      'success': True,
    })
  except KeyError:
    abort(404)


@app.route('/heartbeat', methods=['POST'])
def heartbeat() -> Response:
  """
  Processes a heartbeat from a worker.

  Args (JSON):
    worker_id (str): Peer ID of worker.
    active_tasks (List[TaskPointer]): List of still-running tasks.

  Returns (JSON):
    new_tasks (List[Task]): List of new tasks for the worker to run.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    return jsonify({
      'new_tasks': workers[req['worker_id']].heartbeat(req['active_tasks']),
    })
  except KeyError:
    abort(404)


@app.route('/allocate', methods=['POST'])
def allocate() -> Response:
  """
  Allocates workers for a new job.

  Args (JSON):
    client_id (str): Peer ID of client.
    new_tasks (List[NewTask]): Ordered list of tasks to allocate resources for.
      Each NewTask shall contain the keys 'program' (str) and 'contacts'
      (List[int] of task indices that this task should be able to contact).

  Returns (JSON):
    task_pointers (List[TaskPointers]): TaskPointers to each allocation, in the
      same order as the input. Empty if not enough resources.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    # Allocate new tasks.
    tasks: List[Task] = []
    for worker in workers.values():
      worker.heartbeat_lock.acquire()
      while len(tasks) < len(req['new_tasks']) and worker.availability() > 0:
        tasks.append(Task(req['client_id'],
                          len(tasks),
                          worker['worker_id'],
                          req['new_tasks'][len(tasks)]['program'],
                          []))
        worker['pending_tasks'].append(tasks[-1])

    # Undo if not enough resources.
    if len(tasks) < len(req['new_tasks']):
      for task in tasks:
        worker[task['worker_id']]['pending_tasks'].remove(task)
      tasks = []

    # Update contact lists.
    for task in tasks:
      task['contacts'] = [TaskPointer(tasks[contact_id])
          for contact_id in req['new_tasks'][task['task_id']]['contacts']]

    # Finalize.
    clients[req['client_id']] = tasks
    for worker in workers.values():
      worker.heartbeat_lock.release()

    return jsonify({
      'task_pointers': [TaskPointer(task) for task in tasks],
    })
  except KeyError:
    abort(404)


@app.route('/allocation')
def allocation() -> Response:
  """
  Gets the allocation for a client.

  Args (GET):
    client_id (str): Peer ID of client.

  Returns (JSON):
    task_pointers (List[TaskPointers]): TaskPointers to each allocation, in the
      same order as the original input. Empty if not enough resources.
  """
  try:
    client_id: str = str(request.args.get('client_id'))
    return jsonify({
      'task_pointers': [TaskPointer(task) for task in clients[client_id]],
    })
  except ValueError:
    abort(400)
  except KeyError:
    abort(404)


def deregister(worker_id: str):
  print(f'Deregistering {worker_id}.')
  dead = workers.pop(worker_id)
  realloc: List[Task] = dead['active_tasks'] + dead['pending_tasks']

  # Reallocate tasks.
  i: int = 0
  for worker in workers.values():
    worker.heartbeat_lock.acquire()
    while i < len(realloc) and worker.availability() > 0:
      realloc[i]['worker_id'] = worker['worker_id']
      worker['pending_tasks'].append(realloc[i])
      i += 1

  # Cancel jobs if not enough resources. (Stops outgoing tasks but doesn't kill
  # already-active tasks. They'll have to die on their own.)
  if i < len(realloc):
    for client_id in set(task['client_id'] for task in realloc):
      for task in clients[client_id]:
        try:
          workers[task['worker_id']]['pending_tasks'].remove(task)
        except (KeyError, ValueError):
          pass
      clients[client_id] = []

  # Update contact lists.
  realloc_map: Dict[Tuple[str, int], str] = {
    (task['client_id'], task['task_id']): task['worker_id']
    for task in realloc
  }
  for client in clients.values():
    for task in client:
      contacts_changed: bool = False
      for contact in task['contacts']:
        key: Tuple[str, int] = (contact['client_id'], contact['task_id'])
        if key in realloc_map:
          contact['worker_id'] = realloc_map[key]
          contacts_changed = True
      if contacts_changed and task in workers[task['worker_id']]['active_tasks']:
        workers[task['worker_id']]['active_tasks'].remove(task)
        workers[task['worker_id']]['pending_tasks'].append(task)

  # Finalize.
  for worker in workers.values():
    worker.heartbeat_lock.release()
