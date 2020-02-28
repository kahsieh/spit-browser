console.log("Running Supervisor")
// Constants
const SCHEDULER_ADDR = "http://127.0.0.1:5000";
const CONNECTION_ADDR = "ws://127.0.0.1:1337"
const REGISTER_API = "/register";
const TASK_SCRIPT = "program/";
const BATCH_DELAY_MS = 1000;
const RESEND_DELAY_MS = 500;
const HEARTBEAT_INTERVAL_MS = 5000;
const NUM_CORES = 1;//window.navigator.hardwareConcurrency;

// Message Queues
var inQueue = {};
var outQueue = {};
var outPending = {};

// Communication Info
var contacts = {};  // Running Task -> Downstream Task
var addresses = {}; // Downstream Task -> Hosting Supervisor
var connection;

var tasks = {};
var my_id;


function register() {
  postToServer(
    { 'n_cores': NUM_CORES, },
    REGISTER_API
  ).then(function(data){
    data.json().then(json => {
      my_id = json["worker_id"];
      console.log("Successfully registered. Worker ID: " + my_id);
      setupWorker();
    });
  })
  .catch(function(error){
    console.error("Failed to register worker:")
    console.error(error)
  });
}

function postToServer(jsonData, api) {
  const requestParams = {
    'headers': {
      'content-type': 'application/json'
    },
    'method': 'POST',
    'body': JSON.stringify(jsonData)
  }
  return fetch(SCHEDULER_ADDR + api, requestParams)
}

function setupWorker() {
  connection = new Connection(CONNECTION_ADDR, my_id, function(data) {
    if (data.hasOwnProperty('ack')) {
      clearInterval(outPending[data['ack']]);
      delete outPending[data['ack']];
      return;
    }
    // for (id in data['messages'])
    //   console.log("Recieved: " + data['messages'][id])
    recieveMessages(data['messages']);
    connection.send(data['id'], {'ack': data['tag']});
  });

  // Schedule recurring tasks
  setInterval(deliverMessages, BATCH_DELAY_MS)
  setInterval(sendMessages, BATCH_DELAY_MS)
  setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS)
}

function sendHeartbeat() {
  postToServer({
      'active_tasks': Object.keys(tasks),
      'worker_id': my_id
    },
    "/heartbeat"
  ).then(function(data){
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
    try {
      inQueue[task_id] = inQueue[task_id].concat(messages)
    }
    catch(error) {
      inQueue[task_id] = messages
    }

  }
}

function registerTask(taskId, contacts) {
  var task;
  // Create webworker to run task
  if (!(taskId in tasks)) {
    task = new Worker(TASK_SCRIPT + taskId)
    tasks[taskId] = task
    inQueue[taskId] = []
  } else {
    task = tasks[taskId]
  }

  // Add/update outgoing addresses
  for (const outTask of contacts) {
    outTaskWorkerId = outTask.split("~")[2]
    addresses[getWorkerIndependentTaskUID(outTask)] = outTaskWorkerId;
  }

  // Add outgoing messages to queue
  task.onmessage = function(e) {
    for (const outTask of contacts) {
      outTaskWorkerId = outTask.split("~")[2]
      if (my_id === outTaskWorkerId) {
        inQueue[outTask].push(e.data)
      } else {
        if (!outQueue[outTaskWorkerId].hasOwnProperty(outTask)) {
          outQueue[outTaskWorkerId][outTask] = []
        }
        outQueue[outTaskWorkerId][outTask].push(e.data)
      }
    }
  }

  // Setup queues for incoming and outgoing messages
  for (const outTask of contacts) {
    outTaskWorkerId = outTask.split("~")[2]
    if (my_id === outTaskWorkerId) {
      break;
    }
    if (!outQueue.hasOwnProperty(outTaskWorkerId)) {
      outQueue[outTaskWorkerId] = {}
    }
  }
}

function deliverMessages() {
  for (var id in inQueue) {
    try {
      if (inQueue[id].length > 0) {
        tasks[id].postMessage(inQueue[id])
      }
    } catch(error) {
      continue;
    }
    inQueue[id] = []
  }
}

function sendMessages() {
  for (const workerId in outQueue) {
    if (Object.getOwnPropertyNames(outQueue[workerId]).length > 0) {
      connection.connect(workerId)
      var timestamp = new Date().getTime();
      connection.send(workerId, {'messages': outQueue[workerId], 'tag': timestamp, 'id': my_id});
      addToPending(outQueue[workerId], workerId, timestamp);
      outQueue[workerId] = {}
    }
  }
}

function getWorkerIndependentTaskUID(taskId) {
  return taskId.substring(0,taskId.lastIndexOf("~"));
}

function addToPending(messages, workerId, tag) {
  var interval = setInterval(function() {
    // Check that tasks have not been assigned to new workers
    for (const pendingTask in messages) {
      taskId = getWorkerIndependentTaskUID(pendingTask);
      newWorkerId = addresses[taskId];
      if (newWorkerId !== workerId) {
        outTask = taskId + "~" + newWorkerId;
        if (my_id === newWorkerId) {
          inQueue[outTask] = inQueue[outTask].concat(messages[pendingTask])
        } else {
          if (!outQueue[newWorkerId].hasOwnProperty(outTask)) {
            outQueue[newWorkerId][outTask] = []
          }
          outQueue[newWorkerId][outTask] = outQueue[newWorkerId][outTask].concat(messages[pendingTask])
        }
        delete messages[pendingTask];
      }
    }
    if (Object.getOwnPropertyNames(messages).length <= 0) {
      clearInterval(outPending[tag]);
      delete outPending[tag];
      return;
    }
    connection.send(workerId, {'messages': messages, 'tag': tag, 'id': my_id});
  }, RESEND_DELAY_MS);
  outPending[tag] = interval;
}



register()
