console.log("Running Client")
//send scheduler an http request with the graph as well as your id.
//get the ip of the last item in the chain from which to retrieve data.

// Constants
const API_KEY = 'lwjd5qra8257b9';
const BATCH_DELAY_MS = 1000;

function register(graph, scheduler_addr) {
  // Create Peer for incoming connections
  //peer = peerJs({key: API_KEY});
  const peer = new Peer({'key': API_KEY});
  peer.on('open', function(id) {
    // Register worker with scheduler
    console.log(peer.id)
    const requestParams = {json:
      {
        'new_tasks': JSON.stringify(graph),
        'client_id': JSON.stringify(peer.id)
      }
    }
    console.log(requestParams);
    request.post(scheduler_addr + "/allocate", requestParams,
    (error, res, body) => {
      if (error) {
        console.error(error)
        return
      }
      console.log(`statusCode: ${res.statusCode}`)
      console.log(body)
    });
  /*fetch(scheduler_addr + "/allocate", requestParams)
    .then(function(data){
      console.log("Successfully registered client")
      var start_id = data['task_pointers'][0]['worker_id']
      setupClient(start_id)
    })
    .catch(function(error){
      console.error("Failed to allocate")
      console.error(error)
    });*/
  });
}

function setupClient(start_id) {
  // Add incoming messages to queue
  peer.on('connection', function(conn) {
    conn.on('data', function(data) {
      console.log('Received', data)
    })
  });
  var conn = peer.connect(start_id);
  conn.on('open', function() {
  // Send messages
  process.stdin.on('readable', () => {
    setInterval(function() { sendData(conn) }, BATCH_DELAY_MS)
    });
  });
}

function sendData(conn) {
  var response = process.stdin.read()
  conn.send(response)

}

function readFilesSync(dir) {
  var files = {};
  fs.readdirSync(dir).forEach(filename => {
    const name = path.parse(filename).name;
    const filepath = path.resolve(dir, filename);
    const stat = fs.statSync(filepath);
    const isFile = stat.isFile();
    if (isFile) files[name] = fs.readFileSync(filepath, 'utf8');
  });
  return files;
}

function readGraphSync(graph_file, files) {
  var graph = [];
  var graph_lines = fs.readFileSync(graph_file, 'utf8').trim().split('\n');
  for (var i = 0; i < graph_lines.length; i++) {
    var line = graph_lines[i].split(' ');
    var name = line[0];
    var nums = []
    line.slice(1).forEach(strnum => {nums.push(parseInt(strnum, 10))})
    graph.push({'program': files[name], 'contacts': nums});
  }
  return graph;
}

const minimist = require('minimist');
const fs = require('fs');
const path = require('path');
//const peerJs = require('peerjs-nodejs');
import Peer from 'peerjs';
const request = require('request');

process.stdin.setEncoding('utf8');

const args = minimist(process.argv.slice(2), {
  default: {
    f: "test",
    s: "http://127.0.0.1:5000",
    g: "graph_file.txt"
  },
});

const files = readFilesSync(args.f + '/');
const graph = readGraphSync(args.g, files);

var scheduler_addr = args.s
//console.log(graph, scheduler_addr)
register(graph, scheduler_addr);
