/*
Copyright (c) 2011 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;

var argv = process.argv;

var BATCH_NUM = 1;
var root_path = argv[3];
var tmp_path = '/tmp';
for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--tmp') {
    if (ii+1 < argv.length) {
      tmp_path = argv[ii+1];
    }
    break;
  }
}
var PREFIX_LENGTH = 2;
var PREFIX_LENGTH2 = 1;
var MAX_TRIES = 5;
var gc_hash = JSON.parse(fs.readFileSync(argv[2]));
var buck = new events.EventEmitter();
var containers = Object.keys(gc_hash); //first level key: container_name
buck.on('gc',function(buck_idx) {
  try {
    var trashes = Object.keys(gc_hash[containers[buck_idx]]); //second level key: file fingerprint
    var tmp_dir = root_path + "/" + containers[buck_idx] + "/~tmp";
    var enum_dir = root_path + "/" + containers[buck_idx] + "/~enum";
    var enum_delta = {};

    for (var j = 0; j < trashes.length; j++)
     enum_delta[gc_hash[containers[buck_idx]][trashes[j]].fn] = gc_hash[containers[buck_idx]][trashes[j]].meta;
    //WRITE ENUM DELTA
    var suffix = new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
    var enum_delta_file = enum_dir + "/delta-" + suffix;
    var enum_delta_tmp_file = enum_dir + "/tmp-" + suffix;
    var sync_cnt = 0;
    var failed_cnt = 0;
    while (sync_cnt < MAX_TRIES) {
      try { fs.writeFileSync(enum_delta_tmp_file, JSON.stringify(enum_delta)); } catch (e) {failed_cnt++;}
      sync_cnt++;
      if (failed_cnt < sync_cnt) break;
    }
    if (failed_cnt >= sync_cnt) {
      fs.unlink(enum_delta_tmp_file,function(err){});
      //can't proceed
      return;
    }
    sync_cnt = 0; failed_cnt = 0;
    while (sync_cnt < MAX_TRIES) {
      try { fs.renameSync(enum_delta_tmp_file, enum_delta_file); } catch (e) {failed_cnt++;}
      sync_cnt++;
      if (failed_cnt < sync_cnt) break;
    }
    if (failed_cnt >= sync_cnt) {
      fs.unlink(enum_delta_tmp_file,function(err){});
      //can't proceed
      return;
    }
    enum_delta = null;
    for (var j = 0; j < trashes.length; j++) {
      var filename = trashes[j];
      for (var xx = 0; xx < gc_hash[containers[buck_idx]][filename].ver.length; xx++) {
        var fn = gc_hash[containers[buck_idx]][filename].ver[xx];
        fs.unlink(tmp_dir+"/"+ fn, function(err) {});
      }
    }
  } catch (err) {
    console.error(err);
  }
});//end of on gc event
for (var i = 0; i < containers.length; i++)
  buck.emit('gc',i);
