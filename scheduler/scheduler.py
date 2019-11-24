#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
"""

from flask import Flask, jsonify, request, Response
from typing import Any, Dict, List, Tuple
from worker import *

app: Flask = Flask(__name__)
jobs: List[Job] = []
workers: List[Worker] = []

@app.route('/')
def root() -> Response:
  return jsonify({
    'jobs': jobs,
    'workers': workers,
  })


@app.route('/register', methods=['POST'])
def register() -> Response:
  """
  Args:
    n_cores (int)

  Returns:
    worker_id (int)
  """
  req: Dict[str, Any] = request.get_json(force=True)
  worker_id: int = len(workers)
  n_cores: int = req['n_cores']
  address: str = request.environ['REMOTE_ADDR'] + ':' + str(request.environ['REMOTE_PORT'])
  workers.append(Worker(worker_id, n_cores, address))
  return jsonify({
    'worker_id': worker_id,
  })


@app.route('/heartbeat', methods=['POST'])
def heartbeat() -> Response:
  """
  Args:
    worker_id (int)
    active_tasks (List[TaskReference])
      Each TaskReference contains:
        job_id (int)
        task_id (int)
        worker_id (int)
        worker_address (str)

  Returns:
    new_tasks (List[Task])
      Each Task contains:
        job_id (int)
        task_id (int)
        worker_id (int)
        program (str)
        sinks (List[TaskReference])
  """
  req: Dict[str, Any] = request.get_json(force=True)
  return jsonify({
    'new_tasks': workers[req['worker_id']].heartbeat(req['active_tasks']),
  })


@app.route('/allocate', methods=['POST'])
def allocate() -> Response:
  """
  Args:
    new_tasks (List[NewTask])
      Each NewTask contains:
        program (str)
        sinks (List[int])

  Returns:
    task_references (List[TaskReference])
      Each TaskReference contains:
        job_id (int)
        task_id (int)
        worker_id (int)
        worker_address (str)
  """
  req: Dict[str, Any] = request.get_json(force=True)
  jobs.append(Job(len(jobs), []))
  job = jobs[-1]
  i = 0
  for worker in workers:
    while i < len(req['new_tasks']) and worker.availability() > 0:
      job_id: int = job['job_id']
      task_id: int = len(job['tasks'])
      worker_id: int = worker['worker_id']
      program: str = req['new_tasks'][i]['program']
      job['tasks'].append(Task(job_id, task_id, worker_id, program))
      i += 1
  for i, task in enumerate(job['tasks']):
    task['sinks'] = [TaskReference(job['tasks'][sink_id], workers) for sink_id in req['new_tasks'][i]['sinks']]
  return jsonify({
    'task_references': [TaskReference(task, workers) for task in job],
  })
