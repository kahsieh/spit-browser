#!/usr/bin/env python3

"""
SPIT-Browser Scheduler: Tester
"""

import json
from scheduler import app
import unittest

class SchedulerTest(unittest.TestCase):
  def setUp(self):
    self.app = app.test_client()
    self.app.testing = True
    self.maxDiff = None

  def test_pipeline(self):
    res = self.app.post('/register', data=json.dumps({
      "worker_id": "worker0",
      "n_cores": 2
    }))
    self.assertEqual(json.loads(res.data), {
      "success": True,
    })

    res = self.app.post('/register', data=json.dumps({
      "worker_id": "worker1",
      "n_cores": 2
    }))
    self.assertEqual(json.loads(res.data), {
      "success": True,
    })

    res = self.app.post('/allocate', data=json.dumps({
      "client_id": "client1",
      "new_tasks": [
        {
          "program": "program0",
          "contacts": [1]
        },
        {
          "program": "program1",
          "contacts": [2]
        },
        {
          "program": "program2",
          "contacts": []
        }
      ]
    }))
    self.assertEqual(json.loads(res.data), {
      "task_pointers": [
        {
          "client_id": "client1",
          "task_id": 0,
          "worker_id": "worker0"
        },
        {
          "client_id": "client1",
          "task_id": 1,
          "worker_id": "worker0"
        },
        {
          "client_id": "client1",
          "task_id": 2,
          "worker_id": "worker1"
        }
      ]
    })

    res = self.app.post('/heartbeat', data=json.dumps({
      "worker_id": "worker0",
      "active_tasks": []
    }))
    self.assertEqual(json.loads(res.data), {
      "new_tasks": [
        {
          "client_id": "client1",
          "contacts": [
            {
              "client_id": "client1",
              "task_id": 1,
              "worker_id": "worker0"
            }
          ],
          "program": "program0",
          "task_id": 0,
          "worker_id": "worker0"
        },
        {
          "client_id": "client1",
          "contacts": [
            {
              "client_id": "client1",
              "task_id": 2,
              "worker_id": "worker1"
            }
          ],
          "program": "program1",
          "task_id": 1,
          "worker_id": "worker0"
        }
      ]
    })

    res = self.app.post('/heartbeat', data=json.dumps({
      "worker_id": "worker1",
      "active_tasks": []
    }))
    self.assertEqual(json.loads(res.data), {
      "new_tasks": [
        {
          "client_id": "client1",
          "contacts": [],
          "program": "program2",
          "task_id": 2,
          "worker_id": "worker1"
        }
      ]
    })


if __name__ == '__main__':
  unittest.main()
