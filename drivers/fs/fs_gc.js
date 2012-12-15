/*
Copyright (c) 2011 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;

var argv = process.argv;

var MAX_TIMEOUT = 30 * 1000; //30 seconds
var gc_timestamp = new Date().valueOf() - MAX_TIMEOUT;
var tmp_path = '/tmp';
for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--tmp') {
    if (ii+1 < argv.length) {
      tmp_path = argv[ii+1];
    }
    break;
  }
}
for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--ts') {
    if (ii+1 < argv.length) {
      try {
        if (isNaN(gc_timestamp=parseInt(argv[ii+1],10))) throw 'NaN';
      } catch(err) {
        gc_timestamp = new Date().valueOf(); //current time
      }
    } else {
      gc_timestamp = new Date().valueOf(); //the time this is executed
    }
    break;
  }
}
var BATCH_NUM = 1;
var root_path = argv[2];
var PREFIX_LENGTH = 2;
var PREFIX_LENGTH2 = 1;
var MAX_TRIES = 5;
var containers = fs.readdirSync(root_path);
console.log(containers);
var buck = new events.EventEmitter();
buck.on('gc',function(buck_idx) {
  try {
    var trashes = fs.readdirSync(root_path + "/" + containers[buck_idx] + "/~gc");
    var to_delete = {};
    var trash_dir = root_path + "/" + containers[buck_idx] + "/~gc";
    var enum_delta = {};
    var enum_dir = root_path + "/" + containers[buck_idx] + "/~enum";
    var enum_delta_file = enum_dir + "/delta-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
    var evt = new events.EventEmitter();
    evt.Container = containers[buck_idx];
    evt.Batch = BATCH_NUM; evt.Counter = 0;
    evt.on('next',function(idx) {
      var filename = trashes[idx]; //hash-pref-suff-ts-rand1-rand[-ts-cnt][-delete]
      var filename2 = filename;
      var delete_marker = false;
      if (filename2[filename2.length-1] == 'e') {
        delete_marker = true;
        filename2 = filename2.substr(0,filename2.lastIndexOf('-'));  //remove -delete
      }
      filename2 = filename2.substr(0,filename2.lastIndexOf('-'));  //remove cnt
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove ts
      var seq_id = filename.substring(filename2.length+1);
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove rand1
      var fingerprint = filename2.substring(0,filename2.lastIndexOf('-',filename2.lastIndexOf('-')-1));
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove rand1
      filename2 = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get ts
      console.log('filename: '+filename+"  fingerprint: " + fingerprint+"  seq_id: " + seq_id);
      if (gc_timestamp) { //specified timestamp, check stats here
        var stats = null;
        try { stats = parseInt(filename2,10); }
        catch (err) {}
        if (!stats || stats  > gc_timestamp) {
          evt.Counter++; evt.Batch--;
          if (evt.Batch === 0) {  evt.Batch = BATCH_NUM; evt.emit('nextbatch'); }
          return;
        }
      }
      var prefix1 = filename.substr(0,PREFIX_LENGTH), prefix2 = filename.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
      var fdir_path = root_path + "/" + evt.Container + "/versions/" + prefix1 + "/" + prefix2;
      var file1 = fdir_path + "/" + fingerprint + "-" + seq_id;
      fs.readFile(file1,function(err,data) {
        if (!err) {
          to_delete[filename] = 1;
          var obj = JSON.parse(data);
          if (!enum_delta[obj.vblob_file_name])
            enum_delta[obj.vblob_file_name]=[];
          var obj2 = {};
          if (delete_marker) {
            obj2.vblob_update_time = new Date(parseInt(filename2,10)).toUTCString().replace(/UTC/ig,"GMT");
            obj2.vblob_seq_id = seq_id;
            obj2.vblob_file_size = 0;
          } else {
            obj2.vblob_file_etag=obj.vblob_file_etag;
            obj2.vblob_update_time=obj.vblob_update_time;
            obj2.vblob_seq_id=obj.vblob_seq_id;
            obj2.vblob_file_size=obj.vblob_file_size;
          }
          enum_delta[obj.vblob_file_name].push(obj2);
          obj2 = null;
          evt.Counter++; evt.Batch--;
          if (evt.Batch === 0) {
            evt.Batch = BATCH_NUM; evt.emit('nextbatch');
          }
        } else {
          evt.Counter++; evt.Batch--;
          if (evt.Batch === 0) { evt.Batch = BATCH_NUM; evt.emit('nextbatch'); }
        }
      });
    });
    evt.on('nextbatch',function() {
      console.log('counter ' + evt.Counter);
      if (evt.Counter + BATCH_NUM > trashes.length) evt.Batch = trashes.length - evt.Counter;
      for (var i = evt.Counter; i < trashes.length && i < evt.Counter + BATCH_NUM; i++) {
        evt.emit('next', i);
      }
      if (evt.Counter >= trashes.length) {
        //write to delta file and unlink to_delete list
        enum_delta_file = enum_dir + "/delta-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
        var sync_cnt = 0;
        var failed_cnt = 0;
        while (sync_cnt < MAX_TRIES) {
          try {
            fs.writeFileSync(enum_delta_file, JSON.stringify(enum_delta));
          } catch (e) { failed_cnt++; }
          sync_cnt++;
          if (failed_cnt < sync_cnt) break;
        }
        if (failed_cnt < sync_cnt) {
          //now safely remove the gc files
          var keys = Object.keys(to_delete);
          for (var idx=0; idx<keys.length; idx++)
            fs.unlink(trash_dir+"/"+keys[idx],function(e) {});
        }
      }
    });
    evt.emit('nextbatch');
  } catch (err) {
    console.error(err);
  }
});//end of on gc event
for (var i = 0; i < containers.length; i++)
  buck.emit('gc',i);
