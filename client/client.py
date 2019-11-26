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
from multiprocessing import Process
import asyncio

scheduler_url = "http://127.0.0.1:5000/allocate"
IP = '127.0.0.1'
PORT = 8888

def parse_graph(graph_file):
  return 1

def wait_for_answers(ip, port):
  async def tcp_answer_client():
      reader, writer = await asyncio.open_connection(
          ip, port)
      try:
        while True:
          data = await reader.read(10)
          print(f'Received: {data.decode()!r}')
      except KeyboardInterrupt:
        print('Stopped Reading Data')
        print('Close the connection')
        writer.close()
        await writer.wait_closed()
        pass

  asyncio.run(tcp_answer_client())


async def handle_streaming(reader, writer):
  try:
    buff = ''
    while True:
        buff = sys.stdin.read(10)
        if buff == '':
          break
        print(f'Send: {buff!r}')
        writer.write(buff.encode())
        await writer.drain()
  except KeyboardInterrupt:
    print("Close the connection stop streaming")
    writer.close()

async def stream():
    server = await asyncio.start_server(
        handle_streaming, IP, PORT)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

if __name__ == '__main__':
  #Parsing CL arguments
  parser = argparse.ArgumentParser()
  parser.add_argument("--folder", type=str, help="Directory of .js files")
  parser.add_argument("--workers", type=int, help="number of workers needed")
  parser.add_argument("--graph", type=str, help="graph_file")
  FLAGS = parser.parse_args()
  folder = FLAGS.folder
  num_workers = FLAGS.workers
  graph_file = FLAGS.graph

  #extracting graph from graph file
  graph = parse_graph(graph_file)

  #creating initial request to scheduler
  file_list = os.listdir(folder)
  file_paths = [os.path.join(folder, filename)\
              for filename in file_list]
  multipart_form_data =[('files', open(file_paths[num], 'rb'))\
                          for num in range(len(file_paths))]
  multipart_form_data.append(('num_workers',num_workers))
  multipart_form_data.append(('num_files',len(file_list)))
  multipart_form_data.append(('graph',graph))
  #{'new_tasks': [{'program': <file_contents>, 'sinks': [1]}, {'program': <file_contents>, 'sinks': []}]}
  #making request
  response = requests.post(scheduler_url, files=multipart_form_data)
  #response = requests.post(scheduler_url, data={'num_workers':2})
  print(response)
  if response.status_code != 200:
    print('Failure Error Code: {}'.format(response.status_code))
    print('Exiting please try again')
    exit(1)

  #successful response start client and server to start streaming data and
  #waiting for answers.
  info = response.json()
  print(info)
  end_ip = info['ip']
  end_port = info['port']
  p = Process(target=wait_for_answers, args=(end_ip, end_port))
  asyncio.run(stream())
