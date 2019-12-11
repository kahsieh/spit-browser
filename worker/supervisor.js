console.log("Running Supervisor")
// Constants
const SCHEDULER_ADDR = "http://127.0.0.1:5000";
const API_KEY = 'lwjd5qra8257b9';
const TASK_SCRIPT = "program/";
const BATCH_DELAY_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 5000;
const NUM_CORES = window.navigator.hardwareConcurrency;

// Message Queues
var inQueue = {}
var outQueue = {}

var tasks = {}
var contacts = {}
var peer;
var my_id;


function register() {
  // Create Peer for incoming connections
  peer = new Peer({key: API_KEY});
  peer.on('open', function(id) {
    // Register worker with scheduler
    my_id = id;
    const requestParams = {
      'headers': {
        'content-type': 'application/json'
      },
      'method': 'POST',
      'body': JSON.stringify({
        'n_cores': NUM_CORES,
        'worker_id': my_id
      })
    }
    fetch(SCHEDULER_ADDR + "/register", requestParams)
      .then(function(data){
        console.log("Successfully registered")
        setupWorker()
      })
      .catch(function(error){
        console.error("Failed to register worker:")
        console.error(error)
      });
  });
}

function setupWorker() {
  // Add incoming messages to queue
  peer.on('connection', function(conn) {
    conn.on('data', function(data) {
      recieveMessages(data['messages'])

    })
  });

  // Schedule recurring tasks
  setInterval(deliverMessages, BATCH_DELAY_MS)
  setInterval(sendMessages, BATCH_DELAY_MS)
  setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS)
}

function sendHeartbeat() {
  const requestParams = {
    'headers': {
      'content-type': 'application/json'
    },
    'method': 'POST',
    'body': JSON.stringify({
      'active_tasks': Object.keys(tasks),
      'worker_id': my_id
    })
  }
  fetch(SCHEDULER_ADDR + "/heartbeat", requestParams)
    .then(function(data){
      data.json().then(json => {
        newTasks = json["new_tasks"]
        for (const task of newTasks) {
          registerTask(task['task_id'], task['contacts'])
        }
      })
    })
    .catch(function(error){
      console.error("Failed to send heartbeat:")
      console.error(error)
    });
}

function recieveMessages(all_messages) {
  for (const task_id in all_messages) {
    messages = all_messages[task_id]
    inQueue[task_id] = inQueue[task_id].concat(messages)
  }
}

function registerTask(taskId, contacts) {
  // Create webworker to run task
  var task = new Worker(TASK_SCRIPT + taskId)
  // Add outgoing messages to queue
  task.addEventListener('message', function(e) {
    for (const outTask of contacts) {
      outTaskWorkerId = outTask.split("~")[2]
      if (my_id === outTaskWorkerId) {
        inQueue[outTask].push(e.data)
      } else {
        outQueue[outTaskWorkerId][outTask].push(e.data)
      }
    }
  }, false)

  // Setup queues for incoming and outgoing messages
  tasks[taskId] = task
  inQueue[taskId] = []
  for (const outTask of contacts) {
    outTaskWorkerId = outTask.split("~")[2]
    if (my_id === outTaskWorkerId) {
      break;
    }
    if (!outQueue.hasOwnProperty(outTaskWorkerId)) {
      outQueue[outTaskWorkerId] = {}
    }
    if (!outQueue[outTaskWorkerId].hasOwnProperty(outTask)) {
      outQueue[outTaskWorkerId][outTask] = []
    }
  }
}

function deliverMessages() {
  for (var id in inQueue) {
    tasks[id].postMessage(inQueue[id])
    inQueue[id] = []
  }
}

function sendMessages() {
  for (const workerId in outQueue) {
    conn = peer.connect(workerId)
    conn.on('open', function(){
      conn.send({'messages': outQueue[workerId]});
      for (const taskId in outQueue[workerId]) {
        outQueue[workerId][taskId] = []
      }
    })
  }
}

register();
