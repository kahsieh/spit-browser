console.log("Worker Running")

var vertex;

onmessage = function(e) {
  if (e.data["type"] === "script") {
    self.importScripts(e.data["script"])
    vertex = new Vertex()
    return
  }
  for (const message of e.data["messages"]) {
    self.postMessage(vertex.process(message))
  }
}
