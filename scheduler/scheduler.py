#!/usr/bin/env python3

"""
SPIT-Browser Scheduler
"""

from typing import Any, Dict, List, Tuple
from flask import Flask
from flask import jsonify
from flask import request
from flask import Response
from worker import Worker

app: Flask = Flask(__name__)
workers: List[Worker] = []

@app.route('/register', methods=['POST'])
def register() -> Response:
  """
  Args:
    n_cores (int): Number of cores that the worker has available.

  Returns:
    id (int): ID assigned to the worker.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  address: str = request.environ['REMOTE_ADDR'] + ':' + str(request.environ['REMOTE_PORT'])
  workers.append(Worker(address, req['n_cores']))
  return jsonify({
    'id': len(workers) - 1,
  })


@app.route('/heartbeat', methods=['POST'])
def heartbeat() -> Response:
  """
  Args:
    id (int): Worker's ID.
    n_active (int): Number of active tasks on the worker.

  Returns:
    tasks (List[str]): A list of new programs for the worker to run.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  return jsonify({
    'tasks': workers[req['id']].heartbeat(req['n_active']),
  })


@app.route('/allocate', methods=['POST'])
def allocate() -> Response:
  """
  Args:
    tasks (List[str]): A list of new programs to be assigned to workers.

  Returns:
    nodes (List[Tuple[int, str]]): List of pairs, each of which contains the ID
      and address of a worker that the respective program will be sent to.
  """
  req: Dict[str, Any] = request.get_json(force=True)
  nodes: List[Tuple[int, str]] = []
  for i, worker in enumerate(workers):
    while req['tasks'] and worker.usage() < 1:
      worker.push(req['tasks'].pop(0))
      nodes.append((i, worker.address()))
  return jsonify({
    'nodes': nodes,
  })
