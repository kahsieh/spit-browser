process.stdin.resume();
var fs = require('fs');
const buff = Buffer.from([]);
var response = fs.readSync(process.stdin.fd, buff, 0, "utf8");
console.log(response)
process.stdin.pause();
