class Connection {
  constructor(url, myId, onmessage) {
    this.myId = myId;
    this.peers = {};
    this.pending = {};
    this.onmessagecb = onmessage;
    this.server = new WebSocket(url);
    this.server.onopen = (function(event) {
      this.server.send(JSON.stringify({
        "type": "register",
        "id": myId,
      }));
    }).bind(this);
    this.server.onmessage = (function(event) {
      var msg = JSON.parse(event.data);
      switch(msg.type) {
        case "message":
          this.connect(msg.id);
          this.onmessagecb(msg.data);
          break;
        case "signal":
          if (!(msg["id"] in this.pending)) {
            this.connect(msg["id"]);
          }
          this.pending[msg["id"]].signal(msg.data);
          break;
      }
    }).bind(this);
  }


  connect(peerId) {
    if ((peerId in this.pending) || (peerId in this.peers)) {
      return;
    }
    const newPeer = new SimplePeer({
      initiator: peerId > this.myId,
      trickle: true
    });
    newPeer.on('error', err => {
      console.log('error', err);
      delete this.pending[peerId];
    });
    newPeer.on('signal', data => {
      this.server.send(JSON.stringify({
        "dest": peerId,
        "type": "signal",
        "id": this.myId,
        "data": data,
      }));
    });
    newPeer.on('connect', () => {
        this.peers[peerId] = newPeer;
    })
    newPeer.on('data', data => {
      this.onmessagecb(JSON.parse(data));
    });
    newPeer.on('close', () => {
      delete this.peers[peerId];
    })
    this.pending[peerId] = newPeer;
  }

  send(peerId, data) {
    if (peerId in this.peers) {
      this.peers[peerId].send(JSON.stringify(data));
    } else {
      this.connect(peerId);
      this.server.send(JSON.stringify({
        "id": this.myId,
        "type": "message",
        "dest": peerId,
        "data": data,
      }))
    }
  }

}
