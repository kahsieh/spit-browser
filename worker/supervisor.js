console.log("Running Supervisor")
var inQueue = {}
var outQueue = {}
var workers = {}

function register() {

}

function recieveMessage(message) {
  inQueue[message[0]].push(message[1]);
}

function registerWorker(id, script, outIds) {
  var worker = new Worker("./worker.js")
  worker.postMessage({"type":"script", "script":script})
  worker.addEventListener('message', function(e) {
    for (const outId of outIds) {
      outQueue[outId].push(e.data)
    }
  }, false)
  workers[id] = worker
  inQueue[id] = []
  for (const outId of outIds) {
    if (!outQueue.hasOwnProperty(outId)) {
      outQueue[outId] = []
    }
  }
}

function deliverMessages() {
  for (var id in inQueue) {
    workers[id].postMessage({"type":"message", "messages":inQueue[id]})
    inQueue[id] = []
  }
}

function sendMessages() {
  for (var id in outQueue) {
    if (outQueue[id].length) {
      console.log("Sending to " + id + ": " + outQueue[id])
      outQueue[id] = []
    }
  }
}

registerWorker("1", "./simpleVertex.js", ["4","5"])
registerWorker("2", "./runningAvg.js", ["3","5"])
setInterval(function() { recieveMessage(["1", Math.random()]) }, 200)
setInterval(function() { recieveMessage(["2", Math.random()]) }, 200)
setInterval(deliverMessages, 1000)
setInterval(sendMessages, 1000)
