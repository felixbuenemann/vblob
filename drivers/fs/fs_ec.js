/*
Copyright (c) 2011-2012 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;
var crypto = require('crypto');
var zlib = require('zlib');
var express = require('express');

function seq_id_cmp(seq1, seq2)
{
  var epoch1, epoch2, cnt1, cnt2;
  epoch1 = parseInt(seq1.substr(0, seq1.indexOf('-')),10);
  epoch2 = parseInt(seq2.substr(0, seq2.indexOf('-')),10);
  if (epoch1 < epoch2) return -1;
  if (epoch1 > epoch2) return 1;
  cnt1 = parseInt(seq1.substring(seq1.indexOf('-')+1), 10);
  cnt2 = parseInt(seq2.substring(seq2.indexOf('-')+1), 10);
  if (cnt1 < cnt2) return -1;
  if (cnt1 > cnt2) return 1;
  return 0;
}

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
var PURGE_EXPIRATION = 24 * 3600 * 1000; //24 hrs to purge deleted entries
var argv = process.argv;
var root_path = argv[2];
var tmp_path = '/tmp';
var long_running = false;
var long_running_interval = 300; //check every 300ms
var long_running_flush_interval = 10 * 1000; //write image to disk every 10 seconds
var port = 9877;

for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--tmp') {
    if (ii+1 < argv.length) {
      tmp_path = argv[ii+1];
    }
  } else if (argv[ii] === '--long_running_interval') {
    if (ii+1 < argv.length) {
      long_running = true;
      long_running_interval = parseInt(argv[ii+1],10);
      if (isNaN(long_running_interval)) long_running_interval = 300;
    }
  } else if (argv[ii] === '--long_running_flush_interval') {
    if (ii+1 < argv.length) {
      long_running = true;
      long_running_flush_interval = parseInt(argv[ii+1],10);
      if (isNaN(long_running_flush_interval)) long_running_flush_interval = 10*1000;
    }
  } else if (argv[ii] === '--port') {
    if (ii+1 < argv.length) {
      long_running = true;
      port = parseInt(argv[ii+1],10);
      if (isNaN(port)) port = 9877;
    }
  }
}

var buck = new events.EventEmitter();
var containers;
var global_enum_base = {};
var global_quota_map = {};
var global_objects_map = {};
var global_purge_list = {};
var processed_logs = {};
var flush_map = {};
var job_done = true;
var purge_done = true;

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
          if (global_enum_base[containers[buck_idx]] &&
              (flush_map[containers[buck_idx]] && new Date().valueOf() - flush_map[containers[buck_idx]] < PURGE_EXPIRATION) //it's within PURGE_EXPIRATION since last flush, don't force a purge scan
              ) 
            if (versions.length === 0 || versions.length === 1 && versions[0] === '') {
              flush_event.counter--;
              if (flush_event.counter == 0) flush_event.emit('flush');
              return;
            }
          var closure = function(bucket) {
            var ivt_enum = {};
            var enum_base = global_enum_base[bucket];
            var keys1 = Object.keys(global_enum_base[bucket]);
            var current_ts = new Date().valueOf();
            for (var nIdx1=0; nIdx1<keys1.length; nIdx1++) {
              var ver1 = get_key_fingerprint(keys1[nIdx1]);
              ivt_enum[keys1[nIdx1]] = ver1;
              _objects += enum_base[keys1[nIdx1]].length;
              //TODO: need to change for versionings
              if (enum_base[keys1[nIdx1]].length==1&&!enum_base[keys1[nIdx1]][0].etag) {
                _objects--;
                //check if we need to purge delete markers
                if (new Date(enum_base[keys1[nIdx1]][0].lastmodified).valueOf() + PURGE_EXPIRATION < current_ts) {
                  //PURGE_EXPIRATION old delete marker, add to purge list
                  if (!global_purge_list[containers[buck_idx]]) global_purge_list[containers[buck_idx]] = {};
                  global_purge_list[containers[buck_idx]][keys1[nIdx1]] = 1;
                }
              }
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
                  //console.log('processing delta ' + data);
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
                    try { obj = JSON.parse(data); } catch (e) {update_evt.emit('done'); return; } //in case this file is truncated or corrupted
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
                            if (old_size == -1 || seq_id_cmp(enum_base[current_key][0].seq , obj2.vblob_seq_id) <= 0) {
                            if (old_size != -1 && enum_base[current_key][0].seq == obj2.vblob_seq_id) continue;
                            //unlink version and blob of current version
                            if (old_size != -1) fs.unlink(version_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+enum_base[current_key][0].seq, function(err) {} );
                            if (old_size != -1) fs.unlink(blob_dir+"/"+pref1+"/"+pref2+"/"+filename+"-"+enum_base[current_key][0].seq, function(err) {} );
                            if (old_size != -1 && enum_base[current_key][0].etag && !obj2.vblob_file_etag) _objects--; //deleting an object
                            if (old_size == -1) { old_size = 0; _objects += 1; if (!obj2.vblob_file_etag) _objects--; }
                            enum_base[current_key] = [{}];
                            enum_base[current_key][0].size = obj2.vblob_file_size;
                            if (obj2.vblob_file_etag) enum_base[current_key][0].etag = obj2.vblob_file_etag;
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
            if (evt2.counter > 0)
              evt2.emit('next',0);
            else {
              flush_event.counter--;
              if (flush_event.counter == 0) flush_event.emit('flush');
              return;
            }
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
  containers = fs.readdirSync(root_path);
  //console.log('running once: ' + containers);
  flush_event.counter = containers.length;
  var keys = Object.keys(global_enum_base);
  for (var x=0;x<keys.length;x++) {
    var found = false;
    for (var xx=0;xx<containers.length;xx++)
      if (keys[x] == containers[xx]) {found = true; break; }
    if (!found) delete global_enum_base[keys[x]];
    if (!found) delete global_purge_list[keys[x]];
  }
  if (flush_event.counter > 0) {
    for (var i = 0; i < containers.length; i++)
      buck.emit('compact',i);
  } else job_done = true;
}

function purge_once() {
  var keys = Object.keys(global_purge_list);
  //console.log('starting to purge');
  for (var i = 0; i < keys.length; i++) {
    if (!global_enum_base[keys[i]]) { global_purge_list[keys[i]] = null; continue; }
    var files;
    try {
      files = fs.readdirSync(root_path+"/"+keys[i]+"/~tmp");
    } catch (e)
    {
      if (e.code == 'ENOENT') delete global_purge_list[keys[i]]; //no such bucket
      continue;
    }
    var min_seq = null;
    for (var idx=0; idx< files.length; idx++) {
      var filename = files[idx];
      var ftype = filename.charAt(filename.length-1);
      if (ftype == 'b') continue; //temp blob has no epoch
      var test_nfs = filename.substr(0,4); //nfs renames file to .nfsxxxxx, we need to skip such files
      if (test_nfs == '.nfs') continue;
      var filename2 = filename;
      var epoch,cnt,seq_id;
      if (ftype == 'e' || ftype == 'p') {
        filename2 = filename2.substr(0,filename2.lastIndexOf('-'));  //remove delete/nop
      }
      cnt = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get cnt
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove cnt
      epoch = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get epoch
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove epoch
      seq_id = epoch+"-"+cnt;
      if (!min_seq || seq_id_cmp(seq_id, min_seq) < 0) min_seq = seq_id;
    }
    files = null;
    var keys2 = Object.keys(global_purge_list[keys[i]]);
    var version_dir = root_path+"/"+keys[i]+"/versions";
    var blob_dir = root_path+"/"+keys[i]+"/blob";
    for (var idx2=0; idx2<keys2.length; idx2++) {
      var vec = global_enum_base[keys[i]][keys2[idx2]];
      if (!vec) continue; //non-exists
      if (vec.length > 1) continue; //multi-versions
      if (vec[0].etag) continue; //not delete marker
      if (min_seq && seq_id_cmp(min_seq,vec[0].seq) < 0) continue; //don't trim, there may be other versions in between
      var fp = get_key_fingerprint(keys2[idx2]);
      var pref1 = fp.substr(0,PREFIX_LENGTH), pref2 = fp.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
      fs.unlink(blob_dir+"/"+pref1+"/"+pref2+"/"+fp+"-"+vec[0].seq,function(err){});
      fs.unlink(version_dir+"/"+pref1+"/"+pref2+"/"+fp+"-"+vec[0].seq,function(err){});
      delete global_enum_base[keys[i]][keys2[idx2]];
      delete global_purge_list[keys[i]][keys2[idx2]];
      processed_logs[keys[i]] = {}; //trigger flush
    }
    keys2 = Object.keys(global_purge_list[keys[i]]);
    if (keys2.length == 0) delete global_purge_list[keys[i]];
  }
  //console.log('purge done');
  purge_done = true;
}

if (long_running == true) {
var app = express.createServer();
app.get('/:container/:objname',function(req,res) {
  var bucket = req.params.container;
  var file = req.params.objname;
  if (!global_enum_base[bucket]) { res.statusCode=404; res.end(); return;}
  if (!global_enum_base[bucket][file]) { res.statusCode=404; res.end(); return; }
  res.statusCode = 200;
  res.setHeader("seq-id",global_enum_base[bucket][file][0].seq);
  res.end();
});
app.listen(port);
}

if (long_running != true)
  run_once();
else {
  setInterval(function() {
    if (job_done != true) return;
    job_done = false;
    run_once();
  }, long_running_interval);
  setInterval(function() {
    if (purge_done != true) return;
    purge_done = false;
    purge_once();
  }, PURGE_EXPIRATION);
}
