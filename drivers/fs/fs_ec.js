/*
Copyright (c) 2011-2012 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;
var crypto = require('crypto');
var zlib = require('zlib');

var BATCH_NUM = 10;

//identical to the ones in blob_fs
function get_key_md5_hash(filename)
{
  var md5_name = crypto.createHash('md5');
  md5_name.update(filename);
  return md5_name.digest('hex');
}

//<md5 hash of the key>-<prefix of the key>-<suffix of the key>
function get_key_fingerprint(filename)
{
  var digest = get_key_md5_hash(filename);
  var prefix, suffix;
  var file2 = filename.replace(/(\+|=|\^|#|\{|\}|\(|\)|\[|\]|%|\||,|:|!|;|\/|\$|&|@|\*|`|'|"|<|>|\?|\\)/g, "_"); //replacing all special chars with "_"
  if (file2.length < 8) {
    while (file2.length < 8) file2 += '0';
    prefix = file2.substr(0,8);
    suffix = file2.substr(file2.length - 8);
  } else {
    prefix = file2.substr(0,8);
    suffix = file2.substr(file2.length-8);
  }
  return digest+'-'+prefix+'-'+suffix;
}
var PREFIX_LENGTH = 2;
var PREFIX_LENGTH2 = 1;
var MAX_WRITE_TRIES = 3;
var MAX_DELETE_TRIES = 5;
var argv = process.argv;
var root_path = argv[2];
var tmp_path = '/tmp';
var long_running = false;
var long_running_interval = 300; //check every 300ms
var long_running_flush_interval = 10 * 1000; //write image to disk every 10 seconds

for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--tmp') {
    if (ii+1 < argv.length) {
      tmp_path = argv[ii+1];
    }
  } else if (argv[ii] === '--long_running_interval') {
    if (ii+1 < argv.length) {
      long_running = true;
      try { long_running_interval = parseInt(argv[ii+1],10); } catch (e) { }
    }
  } else if (argv[ii] === '--long_running_flush_interval') {
    if (ii+1 < argv.length) {
      long_running = true;
      try { long_running_flush_interval = parseInt(argv[ii+1],10); } catch (e) { }
    }
  }
}

var buck = new events.EventEmitter();
var containers;
var global_enum_base = {};
var global_quota_map = {};
var global_objects_map = {};
var processed_logs = {};
var flush_map = {};
var job_done = true;

function flush_base(bucket, enum_base, enum_dir) {
    //UPDATE BASE
    //TODO: race condition
    //in a deployment, restrict ec to single instance for now
    zlib.deflate(JSON.stringify(enum_base), function(err,buffer) {
      if (err) {
        flush_event.counter--;
        if (flush_event.counter == 0) flush_event.emit('done');
        return; //give up
      }
      var sync_cnt = 0;
      var temp_name = enum_dir+"/base-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
      var failed_cnt = 0;
      while (sync_cnt < MAX_WRITE_TRIES) { try { fs.writeFileSync(temp_name,buffer); } catch (e) {failed_cnt++;}; sync_cnt++; if (failed_cnt < sync_cnt) break; }
      buffer = null;
      if (failed_cnt >= sync_cnt) { 
        sync_cnt=0;
        while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name);} catch (e) { }; sync_cnt++; };
        flush_event.counter--;
        if (flush_event.counter == 0) flush_event.emit('done');
        return;//can't write, give up
      }
      exec('mv '+temp_name+" "+enum_dir+"/base", function (error, stdout, stderr) {
        if (error) {
          sync_cnt=0;
          while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name);} catch (e) { }; sync_cnt++; };
          flush_event.counter--;
          if (flush_event.counter == 0) flush_event.emit('done');
          return;
        }
        var temp_name2 = enum_dir+"/quota-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
        sync_cnt=0;
        failed_cnt = 0;
        while (sync_cnt<MAX_WRITE_TRIES) { try { fs.writeFileSync(temp_name2,"{\"storage\":"+global_quota_map[bucket]+",\"count\":"+global_objects_map[bucket]+"}"); } catch (e) {failed_cnt++;}; sync_cnt++; if (failed_cnt<sync_cnt) break;}
        if (failed_cnt >= sync_cnt) {
          //can't write, give up
          sync_cnt = 0;
          while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name2);} catch (e) { }; sync_cnt++; };
          flush_event.counter--;
          if (flush_event.counter == 0) flush_event.emit('done');
          return;
        }
        exec('mv '+temp_name2+" "+enum_dir+"/quota",function(error,stdout,stderr) {
          if (error) {
            sync_cnt = 0;
            while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name2);} catch (e) { }; sync_cnt++; };
            flush_event.counter--;
            if (flush_event.counter == 0) flush_event.emit('done');
            return;
          }
          var logs = Object.keys(processed_logs[bucket]);
          for (var idx3=0; idx3<logs.length; idx3++) {
            sync_cnt=0;
            while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(logs[idx3]);} catch (e) { }; sync_cnt++; };
          }
          processed_logs[bucket] = null;
          flush_map[bucket] = new Date().valueOf();
          flush_event.counter--;
          if (flush_event.counter == 0) process.nextTick(function() { flush_event.emit('done');} );
        }); //end of exec renaming quota
      }); //end of exec renaming base 
    })// end of zip
}

var flush_event = new events.EventEmitter();

flush_event.on('flush',function() {
  flush_event.counter = containers.length;
  for (var idx = 0; idx < containers.length; idx++) {
    var bucket = containers[idx];
    var enum_dir = root_path + "/" + bucket + "/~enum";
    if (processed_logs[bucket]) {
      if (!flush_map[bucket]) flush_map[bucket] = new Date().valueOf() - 2 * long_running_flush_interval;
      if (long_running == false ||  new Date().valueOf() - flush_map[bucket] > long_running_flush_interval)
        flush_base(bucket,global_enum_base[bucket], enum_dir);
      else {
        flush_event.counter--;
        if (flush_event.counter == 0) process.nextTick(function() { flush_event.emit('done'); } );
      }
    } else {
      flush_event.counter--;
      if (flush_event.counter == 0) process.nextTick(function() { flush_event.emit('done'); } );
    }
  }
});

flush_event.on('done', function() {
//finish here
  job_done = true;
  console.log('job finished ' + new Date());
});

buck.on('compact',function(buck_idx) {
    var enum_dir = root_path + "/" + containers[buck_idx] + "/~enum";
    var version_dir = root_path + "/" + containers[buck_idx] + "/versions";
    var blob_dir = root_path + "/" + containers[buck_idx] + "/blob";
    var enum_base = {};
    var enum_base_raw = '{}';
    var _used_quota = 0;
    var _objects = 0; 
    var temp_file = tmp_path+"/"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
    var child = exec('find '+ enum_dir +"/ -type f -name \"delta-*\" >"+temp_file,
      function (error, stdout, stderr) {
        if (!error) {
          var versions = {};
          try {
            versions = fs.readFileSync(temp_file).toString().split("\n");
            versions = versions.sort(); //by time stamp
          } catch (e) {
            fs.writeFileSync(temp_file+"-error"," "+e);
            fs.unlink(temp_file,function() {});
            flush_event.counter--;
            if (flush_event.counter == 0) flush_event.emit('flush');
            return;
          }
          var deleted = 0;
          while (deleted < MAX_DELETE_TRIES) {
            try {
              fs.unlinkSync(temp_file);
            } catch (e) { };
              deleted++;
          }
          if (versions.length === 0 || versions.length === 1 && versions[0] === '') {
            flush_event.counter--;
            if (flush_event.counter == 0) flush_event.emit('flush');
            return;
          }
          var closure = function(bucket) {
            var ivt_enum = {};
            var enum_base = global_enum_base[bucket];
            var keys1 = Object.keys(global_enum_base[bucket]);
            for (var nIdx1=0; nIdx1<keys1.length; nIdx1++) {
              var ver1 = get_key_fingerprint(keys1[nIdx1]);
              ivt_enum[keys1[nIdx1]] = ver1;
              _objects += enum_base[keys1[nIdx1]].length;
              for (var nIdx2=0; nIdx2<enum_base[keys1[nIdx1]].length; nIdx2++) {
                _used_quota += enum_base[keys1[nIdx1]][nIdx2].size;
              }
            }
            var evt2 = new events.EventEmitter();
            evt2.counter = versions.length;
            evt2.on('next',function(idx2) {
              var file1 = versions[idx2];
                evt2.counter--;
                if (processed_logs[containers[buck_idx]] && processed_logs[containers[buck_idx]][file1]) {
                  if (evt2.counter > 0) { evt2.emit('next',idx2+1); return; }
                  flush_event.counter--;
                  if (flush_event.counter == 0) flush_event.emit('flush');
                  return;
                }
                fs.readFile(file1,function(err2,data) {
                  var update_evt = new events.EventEmitter();
                  update_evt.on('done',function() {
                    if (evt2.counter > 0) { evt2.emit('next',idx2+1); return; }
                    global_quota_map[containers[buck_idx]] = _used_quota;
                    global_objects_map[containers[buck_idx]] = _objects;
                    flush_event.counter--;
                    if (flush_event.counter == 0) flush_event.emit('flush');
                  });//end of update_evt done
                  if (!err2) {
                    var obj = {};
                    try { obj = JSON.parse(data); } catch (e) { } //in case this file is truncated or corrupted
                    var keys = Object.keys(obj);
                    for (var key_idx = 0; key_idx < keys.length; key_idx++) {
                        var current_key = keys[key_idx];
                        var filename;
                        if (ivt_enum[current_key]) filename = ivt_enum[current_key];
                        else
                          filename = get_key_fingerprint(current_key);
                        //CALC FINGERPRINT AND READ META
                        ivt_enum[current_key] = filename;
                        var pref1 = filename.substr(0,PREFIX_LENGTH), pref2 = filename.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
                        for (var ver_idx=0; ver_idx < obj[current_key].length; ver_idx++) {
                            //right now don't support versioning
                            var obj2 = obj[current_key][ver_idx];
                            var old_size = enum_base[current_key]?enum_base[current_key][0].size:-1;
                            if (old_size == -1 || enum_base[current_key][0].seq <= obj2.vblob_seq_id) {
                            if (old_size != -1 && enum_base[current_key][0].seq == obj2.vblob_seq_id) continue;
                            if (old_size != -1) {
                            }
                            //unlink version and blob of current version
                            if (old_size != -1) fs.unlink(version_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+enum_base[current_key][0].seq, function(err) {} );
                            if (old_size != -1) fs.unlink(blob_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+enum_base[current_key][0].seq, function(err) {} );
                            if (old_size == -1) { old_size = 0; _objects += 1; }
                            enum_base[current_key] = [{}];
                            enum_base[current_key][0].size = obj2.vblob_file_size;
                            enum_base[current_key][0].etag = obj2.vblob_file_etag;
                            enum_base[current_key][0].lastmodified = obj2.vblob_update_time;
                            enum_base[current_key][0].seq = obj2.vblob_seq_id;
                            _used_quota += enum_base[current_key][0].size - old_size;
                            } else {
                              //unlink obj2 version
                            fs.unlink(version_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+obj2.vblob_seq_id, function(err) {} );
                            fs.unlink(blob_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+obj2.vblob_seq_id, function(err) {} );
                            }
                          } //end of for ver_idx
                    }//end of for key_idx
                    if (!processed_logs[containers[buck_idx]]) processed_logs[containers[buck_idx]] = { };
                    processed_logs[containers[buck_idx]][file1]=1; 
                    update_evt.emit('done'); 
                    return; 
                  } else {
                    //?
                    update_evt.emit('done');
                  }
                }); //end of readfile
            }); //end of next
            evt2.emit('next',0);
          } // end of closure function
          if (!global_enum_base[containers[buck_idx]]) {
            try {
              enum_base_raw = fs.readFileSync(enum_dir+"/base");
            } catch (e) {
              flush_event.counter--;
              if (flush_event.counter == 0) flush_event.emit('flush');
              return;
            }
            zlib.unzip(enum_base_raw,function(err,buffer) {
              if (!err) {
                try {
                  global_enum_base[containers[buck_idx]] = JSON.parse(buffer);
                } catch (e) {
                }
              } else if (enum_base_raw != '{}') { //work around for initial base creation
                console.log('unzip fail');
                flush_event.counter--;
                if (flush_event.counter == 0) flush_event.emit('flush');
                return;
              } else global_enum_base[containers[buck_idx]] = {};
              buffer = null;
              //call closure
              closure(containers[buck_idx]);
            });// end of unzip
          } else closure(containers[buck_idx]);
        } else {
          try {
            fs.unlinkSync(temp_file);
          } catch (e) { }
          flush_event.counter--;
          if (flush_event.counter == 0) flush_event.emit('flush');
        }
      }
    ); //end of exec callback
});//end of on compact event


function run_once() {
  console.log('job starting ' + new Date());
  containers = fs.readdirSync(root_path);
  flush_event.counter = containers.length;
  for (var i = 0; i < containers.length; i++)
    buck.emit('compact',i);
}

if (long_running != true)
  run_once();
else {
  setInterval(function() {
    if (job_done != true) return;
    job_done = false;
    run_once();
  }, long_running_interval);
}
