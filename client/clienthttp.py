#!/usr/bin/env python3

"""
SPIT-Browser Client
each node in the graph needs a .js file
they also need to send out the information flow
client sends this to scheduler via json
number of workers needed
scheduler returns first worker to the client
client sends that worker its stream of data
and then last worker will send data back to the Client

"""
import os
import sys
import time
import argparse
import requests
from flask import abort, Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS

app: Flask = Flask(__name__)
CORS(app)
IP = '127.0.0.1'
PORT = 8888

@app.route('/')
def root() -> Response:
  return jsonify('hello')

@app.route('/receive', methods=['POST'])
def receive():
  req = request.get_data()
  print(str(req))
  return 'thanks'

@app.route('/send')
def send():
  buf = sys.stdin.buffer.read(100)
  buff = buf.decode('utf-8', 'ignore')
  app.logger.info(f'Send: {buff!r}')
  return jsonify({
    'data': buff,
  })

def create_payload(graph_file, folder):
  payload = {'new_tasks': []}
  with open(graph_file, 'r') as file:
    for line in file:
      items = line.split()
      file_path = os.path.join(folder, items[0])
      with open(file_path, 'r') as js_file:
        program_string = js_file.read()
        program_string_mod = 'const PORT = "{}"\n'.format(PORT) + \
                             'const IP = "{}"\n'.format(IP) + program_string
        payload['new_tasks'].append({'program': program_string_mod, \
                                     'contacts':[int(i) for i in items[1:]]})
        payload['client_id'] = IP + ':' + str(PORT)
  return payload

if __name__ == '__main__':
  #Parsing CL arguments
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, help="Directory of .js files", default = 'test')
  #parser.add_argument("--workers", type=int, help="number of workers needed")
  parser.add_argument("--graph", type=str, help="graph_file",\
                                default='graph_file.txt')
  parser.add_argument("--scheduler_url", type=str, help="scheduler address",\
                                      default="http://127.0.0.1:5000/allocate")
  FLAGS = parser.parse_args()
  folder = FLAGS.folder
  #num_workers = FLAGS.workers
  graph_file = FLAGS.graph
  scheduler_url = FLAGS.scheduler_url

  #making request payload
  payload = create_payload(graph_file, folder)
  print(payload)
  response = requests.post(scheduler_url, json=payload)
  if response.status_code != 200:
    print('Failure Error Code: {}'.format(response.status_code))
    print('Exiting please try again')
    exit(1)
  app.run(host=IP, port= PORT)
