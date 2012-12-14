var http = require('http');
var port = 9876;
if (process.argv.length > 2) port = parseInt(process.argv[2],10);
if (isNaN(port)) port = 9876;
console.log('starting sequence server with port: '+port);
var sequence = 0;
http.createServer(function (request, response) {
  sequence++;
  response.writeHead(200,{"seq-id":new Date().valueOf()+"-"+sequence});
  response.end();
}).listen(port);

