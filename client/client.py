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
scheduler_url = "http://127.0.0.1:5000/register"

from flask import Flask, request, jsonify

app = Flask(__name__)

#receive starting node IP here
@app.route('/')
def index():
    #need to parse the info and then redirect to send_data
    session['my_var'] = 'my_value'
    return redirect(url_for('send_data'))

@app.route('/send_data')
def send_data():
  my_var = session.get('my_var', None)
  k = 0
  try:
      buff = ''
      while True:
          buff += sys.stdin.read(1)
          if buff.endswith('\n'):
              print(buff[:-1])
              buff = ''
              k = k + 1
  except KeyboardInterrupt:
     sys.stdout.flush()
     pass
  print(k)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, help="Directory of .js files")
  parser.add_argument("--workers", type=int, help="number of workers needed")
  parser.add_argument("--graph", type=str, help="graph_file")
  FLAGS = parser.parse_args()
  folder = FLAGS.folder
  num_workers = FLAGS.workers
  graph_file = FLAGS.graph
  file_list = os.listdir(folder)
  file_paths = [os.path.join(folder, filename)\
              for filename in file_list]
  multipart_form_data = {'file{}'.format(num): \
                          (file_list[num], open(file_paths[num], 'rb'))\
                          for num in range(len(file_paths))}

  multipart_form_data['num_workers'] = ('', str(num_workers))
  multipart_form_data['num_files'] = ('', str(len(file_list)))

  response = requests.post(scheduler_url, files=multipart_form_data)
  print(response.status_code)
  app.run(host='127.0.0.1', port=8000)
