

onmessage = function(e) {
        for (const msg of e.data){
                console.log(msg)
                self.postMessage(msg)
        }
}
