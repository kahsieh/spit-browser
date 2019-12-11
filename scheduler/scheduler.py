#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
"""

from flask import abort, Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS
from threading import Lock
from typing import Any, Dict, List, Set, Tuple
from task import *

app: Flask = Flask(__name__)
CORS(app)

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


@app.route('/worker/program/<task_id>')
def program(task_id) -> Response:
  """
  Returns a client-submitted program.

  Args (URL):
    task_id (str): Task's ID: {client_id}~{vertex_id}~{worker_id}.

  Returns:
    A JavaScript file.
  """
  try:
    client_id, vertex_id, _ = str(task_id).split('~')
    program: str = clients[client_id][int(vertex_id)]['program']
    return Response(program, mimetype="text/javascript")
  except ValueError:
    abort(400)
  except (KeyError, IndexError):
    abort(404)


@app.route('/worker/<path:path>')
def send_js(path):
  """
  Serves static worker files.

  Args (URL):
    path (str): Path to static file.
  """
  return send_from_directory('../worker/', path)


@app.route('/register', methods=['POST'])
def register() -> Response:
  """
  Registers a worker.

  Args (JSON):
    worker_id (str): ID of worker.
    n_cores (int): Number of cores on the worker.

  Returns (JSON):
    success (bool): Whether registration succeeded.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    worker_id, n_cores = req['worker_id'], req['n_cores']
    if worker_id in workers:
      abort(403)
    workers[worker_id] = Worker(worker_id, n_cores, deregister)
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
    worker_id (str): ID of worker.
    active_tasks (List[str]): List of ID's of still-running tasks.

  Returns (JSON):
    new_tasks (List[Task]): List of new tasks for the worker to run.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    worker_id, active_tasks = req['worker_id'], req['active_tasks']
    return jsonify({
      'new_tasks': workers[worker_id].heartbeat(active_tasks),
    })
  except KeyError:
    abort(404)


@app.route('/allocate', methods=['POST'])
def allocate() -> Response:
  """
  Allocates workers for a new job.

  Args (JSON):
    client_id (str): ID of client.
    new_tasks (List[NewTask]): Ordered list of vertices to allocate resources
      for. Each NewTask shall contain the keys 'program' (str) and 'contacts'
      (List[int] of task indices that this task should be able to contact).

  Returns (JSON):
    task_ids (List[str]): Task ID's for each allocation, in the same order as
      the input. Empty if not enough resources.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  try:
    client_id, new_tasks = req['client_id'], req['new_tasks']

    # Allocate new tasks.
    tasks: List[Task] = []
    for worker_id, worker in workers.items():
      worker.heartbeat_lock.acquire()
      while len(tasks) < len(new_tasks) and worker.availability() > 0:
        tasks.append(Task(client_id, len(tasks), worker_id,
                          new_tasks[len(tasks)]['program'], []))
        worker['pending_tasks'].append(tasks[-1])

    # Undo if not enough resources.
    if len(tasks) < len(new_tasks):
      for task in tasks:
        worker[task['worker_id']]['pending_tasks'].remove(task)
      tasks = []

    # Update contact lists.
    else:
      for task in tasks:
        task['contacts'] = [tasks[contact_id]['task_id']
          for contact_id in new_tasks[task['vertex_id']]['contacts']]

    # Finalize.
    clients[client_id] = tasks
    for worker in workers.values():
      worker.heartbeat_lock.release()

    return jsonify({
      'task_ids': [task['task_id'] for task in tasks],
    })
  except KeyError:
    abort(404)


@app.route('/allocation')
def allocation() -> Response:
  """
  Gets the allocation for a client.

  Args (GET):
    client_id (str): ID of client.

  Returns (JSON):
    task_ids (List[str]): Task ID's for each allocation, in the same order as
      the original input. Empty if not enough resources.
  """
  try:
    client_id: str = str(request.args.get('client_id'))
    return jsonify({
      'task_ids': [task['task_id'] for task in clients[client_id]],
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
  for worker_id, worker in workers.items():
    worker.heartbeat_lock.acquire()
    while i < len(realloc) and worker.availability() > 0:
      realloc[i]['task_id_old'] = realloc[i]['task_id']
      client_id, vertex_id, _ = realloc[i]['task_id_old'].split('~')
      realloc[i]['task_id'] = f'{client_id}~{vertex_id}~{worker_id}'
      realloc[i]['worker_id'] = worker_id
      worker['pending_tasks'].append(realloc[i])
      i += 1

  # Cancel jobs if not enough resources. (Stops outgoing tasks but doesn't kill
  # already-active tasks. They'll have to die on their own.)
  if i < len(realloc):
    for client_id in set(task['client_id'] for task in realloc):
      for task in clients[client_id]:
        try:
          if task['cancel']:
            continue  # already cancelled
          elif task in workers[task['worker_id']]['pending_tasks']:
            workers[task['worker_id']]['pending_tasks'].remove(task)
          elif task in workers[task['worker_id']]['active_tasks']:
            workers[task['worker_id']]['active_tasks'].remove(task)
            task['cancel'] = True
            workers[task['worker_id']]['pending_tasks'].append(task)
        except KeyError:
          pass
      clients[client_id] = []

  # Update contact lists.
  else:
    realloc_map: Dict[Tuple[str, int], str] =\
      {task['task_id_old']: task['task_id'] for task in realloc}
    for client in clients.values():
      for task in client:
        for i, contact in enumerate(task['contacts']):
          if contact in realloc_map:
            task['contacts'][i] = realloc_map[contact]
            task['update'] = True
        if task['update'] and task in workers[task['worker_id']]['active_tasks']:
          workers[task['worker_id']]['active_tasks'].remove(task)
          workers[task['worker_id']]['pending_tasks'].append(task)

  # Finalize.
  for worker in workers.values():
    worker.heartbeat_lock.release()
