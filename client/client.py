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

IP = '127.0.0.1'
PORT = 8888

def create_payload(graph_file, folder):
  payload = {'new_tasks': []}
  with open(graph_file, 'r') as file:
    for line in file:
      items = line.split()
      file_path = os.path.join(folder, items[0])
      with open(file_path, 'r') as js_file:
        payload['new_tasks'].append({'program': js_file.read(), 'contacts':[int(i) for i in items[1:]]})
        payload['client_id'] = IP + ':' + str(PORT)
  return payload


'''
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
  '''

async def wait_answer(reader):
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

async def stream_data(writer):
  try:
    buff = ''
    while True:
        buff = sys.stdin.read(1)
        if buff == '':
          break
        print(f'Send: {buff!r}')
        writer.write(buff.encode())
        await writer.drain()
  except KeyboardInterrupt:
    print("Close the connection stop streaming")
    writer.close()

async def handle_streaming(reader, writer):
  await stream_data(writer)
  await wait_answer(reader)

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
  parser.add_argument("--folder", type=str, help="Directory of .js files", default = 'test')
  #parser.add_argument("--workers", type=int, help="number of workers needed")
  parser.add_argument("--graph", type=str, help="graph_file", default='graph_file.txt')
  parser.add_argument("--scheduler_url", type=str, help="scheduler address", default="http://127.0.0.1:5000/allocate")
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

  #successful response start client and server to start streaming data and
  #waiting for answers.
  info = response.json()
  print(info)
  #ip_port = info['task_pointers'][-1]['worker_address']
  #end_ip = ip_port.split(':')[0]
  #end_port = int(ip_port.split(':')[1])
  #p = Process(target=wait_for_answers, args=(end_ip, end_port))
  asyncio.run(stream())
