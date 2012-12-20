/*
Copyright (c) 2011 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;

var argv = process.argv;
var force = false;
var gc_timestamp = null;
for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--force') { force = true; continue; } //if force, gc every file
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
    continue;
  }
}
var root_path = argv[2];
var PREFIX_LENGTH = 2;
var PREFIX_LENGTH2 = 1;
var MAX_TIMEOUT = 15 * 60 * 1000; //15 minutes for blob upload
var MAX_TIMEOUT2 = 60 * 1000; //1 min for committing a transaction
var MAX_TRIES = 5;
var containers = fs.readdirSync(root_path);
console.log(containers);
var buck = new events.EventEmitter();
var current_ts = new Date().valueOf();
buck.on('gc',function(buck_idx) {
  try {
    var trashes = fs.readdirSync(root_path + "/" + containers[buck_idx] + "/~tmp");
    var trash_dir = root_path + "/" + containers[buck_idx] + "/~tmp";
    //var gc_dir = root_path + "/" + containers[buck_idx] + "/~gc";
    var evt = new events.EventEmitter();
    var to_delete = {};
    var enum_delta = {};
    var enum_dir = root_path + "/" + containers[buck_idx] + "/~enum";
    evt.Container = containers[i];
    evt.Counter = 0;
    evt.on('next',function(idx) {
      var filename = trashes[idx]; //hash-pref-suff-ts-rand1-rand [-blob]/[-epoch-cnt]/[-epoch-cnt-delete]/[-epoch-cnt-nop]
      var ftype = filename.charAt(filename.length-1);
      var key_fingerprint;
      var seq_id;
      var epoch;
      var ts; //timestamp in filename
      var test_nfs = filename.substr(0,4); //nfs renames file to .nfsxxxxx, we need to skip such files
      if (test_nfs == '.nfs') {
        evt.Counter++;
        evt.emit('nextbatch');
        return;
      }
      var filename2 = filename;
      if (ftype == 'b' || ftype == 'e' || ftype == 'p') {
        filename2 = filename2.substr(0,filename2.lastIndexOf('-'));  //remove blob/delete/nop
      }
      if (ftype != 'b') {
        cnt = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get cnt
        filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove cnt
        epoch = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get epoch
        filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove epoch
        seq_id = epoch+"-"+cnt;
      }
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove rand2
      filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove rand1
      ts = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get ts
      key_fingerprint = filename2.substr(0,filename2.lastIndexOf('-')); //get fingerprint
      //console.log(filename);
      var prefix1 = filename.substr(0,PREFIX_LENGTH), prefix2 = filename.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
      var fdir_path = root_path + "/" + evt.Container + "/blob/" + prefix1 + "/" + prefix2;
      var fver_path = root_path + "/" + evt.Container + "/versions/" + prefix1 + "/" + prefix2;
      var mtime = parseInt(ts);
      //initial filtering using the ts in filename
      if ( !force 
           &&
           (
             ((ftype == 'b' || ftype == 'p') &&  //for blob or nop file, check MAX_TIMEOUT window
               (gc_timestamp && gc_timestamp < mtime //created later than the specified timestamp
                || 
                !gc_timestamp && current_ts < mtime + MAX_TIMEOUT
               )
             )  
           ||((ftype != 'b' && ftype != 'p') &&        //for meta file or delete, check MAX_TIMEOUT2 window
               (gc_timestamp && gc_timestamp < mtime //created later than the specified timestamp
                || 
                !gc_timestamp && current_ts < mtime + MAX_TIMEOUT2
               )
             )
           )
         )
      {
        evt.Counter++; 
        evt.emit('nextbatch');
        return;
      }
      fs.stat(trash_dir+"/"+filename, function(err,stats) {
        if (err) {
          console.log(err);
          evt.Counter++;
          evt.emit('nextbatch');
          return;
        }
        //check stats.mtime to see if it's being written recently
        var mtime = new Date(stats.mtime).valueOf();
        if ( !force 
             &&
             (
               ((ftype == 'b' || ftype == 'p') &&  //for blob or nop file, check MAX_TIMEOUT window
                 (gc_timestamp && gc_timestamp < mtime //created later than the specified timestamp
                  || 
                  !gc_timestamp && current_ts < mtime + MAX_TIMEOUT
                 )
               )  
             ||((ftype != 'b' && ftype != 'p') &&        //for meta file or delete, check MAX_TIMEOUT2 window
                 (gc_timestamp && gc_timestamp < mtime //created later than the specified timestamp
                  || 
                  !gc_timestamp && current_ts < mtime + MAX_TIMEOUT2
                 )
               )
             )
           )
        {
          evt.Counter++;
          evt.emit('nextbatch');
          return;
        }
        if (ftype == 'b') {
          //console.log('cleaning up ' + filename);
          //temp blob
          fs.unlink(trash_dir+"/"+filename, function(err) {
            evt.Counter++;
            evt.emit('nextbatch');
          });
        } else if (ftype == 'p') { //nop file
          fs.readFile(trash_dir+"/"+ filename, function(err,fn) {
            if (err) {
              evt.Counter++;
              evt.emit('nextbatch');
              return;
            }
            if (stats.nlink >= 2) { //remove blob and meta and this file
              //fs.writeFile(gc_dir+"/"+filename,"nop link=2 "+filename+" "+fn,function(err) {});
              fs.unlink(trash_dir+"/"+fn,function(err) {
                fs.unlink(fdir_path+"/"+key_fingerprint+"-"+seq_id,function(err) {} );
                fs.unlink(fver_path+"/"+key_fingerprint+"-"+seq_id,function(err) {
                  fs.unlink(trash_dir+"/"+filename, function(err) {
                    evt.Counter++;
                    evt.emit('nextbatch');
                  });
                });
              });
            } else {
              fs.link(trash_dir+"/"+filename, fver_path+"/"+key_fingerprint+"-"+seq_id, function(err) {
                if (!err) {
                  //fs.writeFile(gc_dir+"/"+filename,"nop relink successful "+filename+" "+fn,function(err) {});
                  fs.unlink(trash_dir+"/"+fn,function(err) {
                    fs.unlink(fdir_path+"/"+key_fingerprint+"-"+seq_id,function(err) {} );
                    fs.unlink(fver_path+"/"+key_fingerprint+"-"+seq_id,function(err) {
                      fs.unlink(trash_dir+"/"+filename, function(err) {
                        evt.Counter++;
                        evt.emit('nextbatch');
                      });
                    });
                  });
                } else {
                  //check error code
                  if (err.code == 'EEXIST') { //already there
                    fs.stat(fver_path+"/"+key_fingerprint+"-"+seq_id, function(err2,stats2) {
                      if (!err2) {
                        if (stats.ino == stats2.ino) //linked by others, still consider succeeded
                        {
                          //fs.writeFile(gc_dir+"/"+filename,"nop linked by others "+filename+" "+fn,function(err) {});
                          fs.unlink(trash_dir+"/"+fn,function(err) {
                            fs.unlink(fdir_path+"/"+key_fingerprint+"-"+seq_id,function(err) {} );
                            fs.unlink(fver_path+"/"+key_fingerprint+"-"+seq_id,function(err) {
                              fs.unlink(trash_dir+"/"+filename, function(err) {
                                evt.Counter++;
                                evt.emit('nextbatch');
                              });
                            });
                          });
                        } else { //others already linked ver, remove nop only
                          fs.unlink(trash_dir+"/"+filename,function(err){
                            evt.Counter++;
                            evt.emit('nextbatch');
                          });
                        }
                      } else {
                        //may be some temp errors, leave to next round
                        evt.Counter++;
                        evt.emit('nextbatch');
                      }
                    });
                  } else {
                    //may be some temp errors, leave to next round
                    evt.Counter++;
                    evt.emit('nextbatch');
                  }
                }
              });
            }
          });
        } else if (ftype == 'e') { //delete file
          //always redo
          fs.readFile(trash_dir+"/"+filename, function(err,data) {
            if (!err) {
              to_delete[filename] = 1;
              var obj = JSON.parse(data);
              if (!enum_delta[obj.vblob_file_name])
                enum_delta[obj.vblob_file_name]=[];
              var obj2 = {};
              obj2.vblob_update_time = new Date(mtime).toUTCString().replace(/UTC/ig,"GMT");
              obj2.vblob_seq_id = seq_id;
              obj2.vblob_file_size = 0;
              enum_delta[obj.vblob_file_name].push(obj2);
              obj2 = null;
              evt.Counter++;
              evt.emit('nextbatch');
            } else {
              evt.Counter++;
              evt.emit('nextbatch');
            }
          });
        } else { //normal put 
          if (stats.nlink < 2) { //gen nop, try remove
            //uncommitted upload, remove blob
            //nop = fingerprint-ts-rand1-rand2-seq_id-nop
            var fn = key_fingerprint+"-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*1000)+"-"+seq_id+"-nop";
            fs.writeFile(trash_dir+"/"+fn,""+filename,function(err) {
              if (err) {
                evt.Counter++;
                evt.emit('nextbatch');
                return;
              }
              fs.link(trash_dir+"/"+fn,fver_path+"/"+key_fingerprint+"-"+seq_id, function(err) {
                if (!err) {
                  //fs.writeFile(gc_dir+"/"+filename,"successfully linked "+fn,function(err) {});
                  fs.unlink(trash_dir+"/"+filename, function(err) {
                    fs.unlink(fdir_path+"/"+key_fingerprint+"-"+seq_id,function(err) {} );
                    fs.unlink(fver_path+"/"+key_fingerprint+"-"+seq_id,function(err) {
                      fs.unlink(trash_dir+"/"+fn, function(err) {
                        evt.Counter++;
                        evt.emit('nextbatch');
                      });
                    });
                  });
                } else {
                  //check error code
                  if (err.code == 'EEXIST') { //already there
                    fs.stat(trash_dir+"/"+fn,function(err3,stats3) {
                    if (!err3)
                    fs.stat(fver_path+"/"+key_fingerprint+"-"+seq_id, function(err2,stats2) {
                      if (!err2) {
                        if (stats3.ino == stats2.ino) //linked by others, still consider succeeded
                        {
                          //fs.writeFile(gc_dir+"/"+filename,"linked by others "+fn+" "+stats3+" "+stats2,function(err) {});
                          fs.unlink(trash_dir+"/"+filename, function(err) {
                            fs.unlink(fdir_path+"/"+key_fingerprint+"-"+seq_id,function(err) {} );
                            fs.unlink(fver_path+"/"+key_fingerprint+"-"+seq_id,function(err) {
                              fs.unlink(trash_dir+"/"+fn, function(err) {
                                evt.Counter++;
                                evt.emit('nextbatch');
                              });
                            });
                          });
                        } else { //others already linked ver, remove nop only
                          fs.unlink(trash_dir+"/"+fn,function(err){
                            evt.Counter++;
                            evt.emit('nextbatch');
                          });
                        }
                      } else {
                        //may be some temp errors, leave to next round
                        evt.Counter++;
                        evt.emit('nextbatch');
                      }
                    }); else { //err3, leave it
                       evt.Counter++;
                       evt.emit('nextbatch');
                    }
                    });
                  } else {
                    //may be some temp errors, leave to next round
                    evt.Counter++;
                    evt.emit('nextbatch');
                  }
                }
              }); 
            });
          } else { //read meta 
            //redo
            fs.readFile(trash_dir+"/"+filename, function(err,data) {
              if (!err) {
                to_delete[filename] = 1;
                var obj = JSON.parse(data);
                if (!enum_delta[obj.vblob_file_name])
                  enum_delta[obj.vblob_file_name]=[];
                var obj2 = {};
                obj2.vblob_file_etag=obj.vblob_file_etag;
                obj2.vblob_update_time=obj.vblob_update_time;
                obj2.vblob_seq_id=obj.vblob_seq_id;
                obj2.vblob_file_size=obj.vblob_file_size;
                enum_delta[obj.vblob_file_name].push(obj2);
                obj2 = null;
                evt.Counter++;
                evt.emit('nextbatch');
              } else {
                evt.Counter++;
                evt.emit('nextbatch');
              }
            });
          }
        }
      });
    }); //end of next
    evt.on('nextbatch',function() {
      if (evt.Counter < trashes.length) 
        evt.emit('next', evt.Counter);
      else {
        //write to delta file and unlink to_delete list
        var keys = Object.keys(to_delete);
        if (keys.length < 1) return;
        var suffix = new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
        var enum_delta_file = enum_dir + "/delta-"+ suffix;
        var enum_delta_tmp_file = enum_dir + "/tmp-" + suffix;
        var sync_cnt = 0;
        var failed_cnt = 0;
        while (sync_cnt < MAX_TRIES) {
          try {
            fs.writeFileSync(enum_delta_tmp_file, JSON.stringify(enum_delta));
          } catch (e) { failed_cnt++; }
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
        if (failed_cnt < sync_cnt) {
          //now safely remove the gc files
          for (var idx=0; idx<keys.length; idx++)
            fs.unlink(trash_dir+"/"+keys[idx],function(e) {});
        }
        enum_delta = null;
      }
    });
    evt.emit('nextbatch');
  } catch (err) {
    console.error(err);
  }
});
for (var i = 0; i < containers.length; i++)
  buck.emit('gc',i);
