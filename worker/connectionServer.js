var webSocketsServerPort = 1337;
var webSocketServer = require('websocket').server;
var http = require('http');
var clients = {}
var server = http.createServer(function(request, response) {
});
server.listen(webSocketsServerPort, function() {
  console.log((new Date()) + " Server is listening on port "
      + webSocketsServerPort);
});
var wsServer = new webSocketServer({
  httpServer: server
});
wsServer.on('request', function(request) {
  console.log((new Date()) + ' Connection from origin '
      + request.origin + '.');

  var connection = request.accept(null, request.origin);
  connection.on('message', function(message) {
    console.log(message);
    message = JSON.parse(message.utf8Data);
    switch(message.type) {
      case "register":
        clients[message["id"]] = connection;
        break;
      case "message":
      case "signal":
        if (message["dest"] in clients) {
          clients[message["dest"]].sendUTF(JSON.stringify(message))
        }
        break;
    }
  });
});
