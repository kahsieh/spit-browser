console.log("Worker Running")

var vertex;

onmessage = function(e) {
  if (e.data["type"] === "script") {
    self.importScripts(e.data["script"])
    vertex = new Vertex(self.postMessage)
    return
  }
  for (const message of e.data["messages"]) {
    vertex.process(message)
  }
}
