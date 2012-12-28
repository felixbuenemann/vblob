var http = require('http');
var fs = require('fs');
var exec = require('child_process').exec;
var default_port = 9876;
var argv = process.argv;
var default_epoch_file_path = './epoch_file';
var default_root_path = './fs_root';
var port=null;
var epoch_file_path = null;
var root_path=null;
var MAX_TRIES = 5;

for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--port') {
    if (ii+1 < argv.length) {
      port = parseInt(argv[ii+1],10);
    }
  } else if (argv[ii] === '--epoch') {
    if (ii+1 < argv.length) {
      epoch_file_path = argv[ii+1];
    }
  } else if (argv[ii] === '--root') {
    if (ii+1 < argv.length) {
      root_path = argv[ii+1];
    }
  }
}
if (!port || isNaN(port)) port = default_port;
if (!epoch_file_path) epoch_file_path = default_epoch_file_path;
if (!root_path) root_path = default_root_path;
console.log('starting sequence server with port: '+port);
console.log('looking for epoch file '+epoch_file_path);

var epoch = new Date().valueOf();
try {
  var last_epoch = parseInt(fs.readFileSync(epoch_file_path),10);
  if (!isNaN(last_epoch))
    if (last_epoch > epoch) epoch = last_epoch + 3600 * 1000; //set current epoch to 1 hour later
} catch (e) {
  console.error('missing epoch_file_path: '+epoch_file_path);
}

var sync_cnt=0, failed_cnt=0;
var tmp_file = epoch_file_path+"-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000);
while (sync_cnt < MAX_TRIES) {
  try {
    fs.writeFileSync(tmp_file,""+epoch);
  } catch (e) {
    failed_cnt++;
  }
  sync_cnt++;
  if (failed_cnt < sync_cnt) break;
}

if (failed_cnt >= sync_cnt) {
  fs.unlink(tmp_file,function(err){});
  console.error('cannot write new epoch, terminate');
  return;
}

exec('mv '+tmp_file+" "+epoch_file_path, function (error, stdout, stderr) {
  if (error) {
    sync_cnt=0;
    while (sync_cnt < MAX_TRIES) { try { fs.unlinkSync(tmp_file);} catch (e) { }; sync_cnt++; };
    console.error('cannot mv to epoch file, terminate');
    return;
  }
  console.log('listening to port ' + port + ' with epoch ' + epoch);
  var sequence = 0;
  var bucket_seq = {};
  http.createServer(function (request, response) {
    if (request.headers["op"] == 'GET') {
      if (request.headers["bucket"] &&
          bucket_seq[request.headers["bucket"]])
        response.writeHead(200,{"seq-id":bucket_seq[request.headers["bucket"]]});
      else response.writeHead(200,{"seq-id":epoch+"-"+sequence});
      response.end();
      return;
    } else if (request.headers["op"] == 'DELETE') {
      if (request.headers["bucket"] &&
          (!bucket_seq[request.headers["bucket"]] ||
            bucket_seq[request.headers["bucket"]] == request.headers["seq-id"]))
      {
        sequence++;
        bucket_seq[request.headers["bucket"]] = epoch+"-"+sequence;
        var old_path = root_path+"/"+request.headers["bucket"];
        var ts = new Date().valueOf();
        var new_path = root_path+"/"+request.headers["bucket"]+".delete."+ts;
        try { 
          fs.renameSync(old_path,new_path);
        } catch (err) {
          response.statusCode=409;
          response.end();
          return;
        }
        delete bucket_seq[request.headers["bucket"]];
        response.writeHead(200,{"seq-id":epoch+"-"+sequence, "location":request.headers["bucket"]+".delete."+ts});
        response.end();
      } else {
        response.statusCode = 409;
        response.end();
      }
      return;
    }
    sequence++;
    if (request.headers["bucket"]) bucket_seq[request.headers["bucket"]] = epoch+"-"+sequence;
    response.writeHead(200,{"seq-id":epoch+"-"+sequence});
    response.end();
  }).listen(port);
});
