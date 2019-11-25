#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
"""

from flask import Flask, jsonify, request, Response
from threading import Lock
from typing import Any, Dict, List, Tuple
from task import *

app: Flask = Flask(__name__)
jobs: List[Job] = []
jobs_lock: Lock = Lock()
workers: List[Worker] = []
workers_lock: Lock = Lock()

@app.route('/')
def root() -> Response:
  """
  Returns the state of the scheduler.

  Returns:
    jobs (List[Job]): All jobs sent via /allocate.
    workers (List[Worker]): All workers registered via /register.
  """
  return jsonify({
    'jobs': jobs,
    'workers': workers,
  })


@app.route('/register', methods=['POST'])
def register() -> Response:
  """
  Registers a worker.

  Args:
    n_cores (int): Number of cores on the worker.

  Returns:
    worker_id (int): ID assigned to the worker.
  """
  req: Dict[str, Any] = request.get_json(force=True)

  # Construct new Worker.
  workers_lock.acquire()
  worker_id: int = len(workers)
  n_cores: int = req['n_cores']
  address: str = request.environ['REMOTE_ADDR'] + ':' +\
                 str(request.environ['REMOTE_PORT'])
  workers.append(Worker(worker_id, n_cores, address))
  workers_lock.release()

  return jsonify({
    'worker_id': worker_id,
  })


@app.route('/heartbeat', methods=['POST'])
def heartbeat() -> Response:
  """
  Processes a heartbeat from a worker.

  Args:
    worker_id (int): ID assigned to the worker.
    active_tasks (List[TaskPointer]): List of still-running tasks.

  Returns:
    new_tasks (List[Task]): List of new tasks for the worker to run.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  return jsonify({
    'new_tasks': workers[req['worker_id']].heartbeat(req['active_tasks']),
  })


@app.route('/allocate', methods=['POST'])
def allocate() -> Response:
  """
  Args:
    new_tasks (List[NewTask]): Ordered list of tasks to allocate resource for,
      where each NewTask contains the keys 'program' (str) and 'sinks'
      (List[int]).

  Returns:
    task_pointers (List[TaskPointers]): TaskPointers to each allocation, in the
      same order as the input.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  client_address: str = request.environ['REMOTE_ADDR'] + ':' +\
                        str(request.environ['REMOTE_PORT'])

  # Construct new Job.
  jobs_lock.acquire()
  job_id: int = len(jobs)
  jobs.append(Job(job_id, []))
  tasks = jobs[-1]['tasks']
  jobs_lock.release()

  # Allocate new tasks.
  task_id: int = 0
  for worker in workers:
    while task_id < len(req['new_tasks']) and worker.availability() > 0:
      worker_id: int = worker['worker_id']
      program: str = req['new_tasks'][task_id]['program']
      task: Task = Task(job_id, task_id, worker_id, client_address, program, [])
      worker['pending_tasks'].append(task)
      tasks.append(task)
      task_id += 1
  
  # Update sink lists.
  for i, task in enumerate(tasks):
    task['sinks'] = [TaskPointer(tasks[sink_id], workers)
                     for sink_id in req['new_tasks'][i]['sinks']]

  return jsonify({
    'task_pointers': [TaskPointer(task, workers) for task in tasks],
  })
