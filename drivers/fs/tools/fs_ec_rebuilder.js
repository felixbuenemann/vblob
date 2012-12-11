/*
Copyright (c) 2011-2012 VMware, Inc.
*/

/*
  Right now this is just a simple tool to manually recover list information by extracting from the actual blobs
  Start this by "node <this file> <blob root>"
  Then leave your seat, take a walk, and grab a cup of coffee :-)
*/
 
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;
var crypto = require('crypto');
var zlib = require('zlib');

var BATCH_NUM = 40;
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
for (var ii = 0; ii < argv.length; ii++) {
  if (argv[ii] === '--tmp') {
    if (ii+1 < argv.length) {
      tmp_path = argv[ii+1];
    }
    break;
  }
}
var buck = new events.EventEmitter();
var containers = fs.readdirSync(root_path);
console.log(containers);
buck.on('compact',function(buck_idx) {
    var enum_dir = root_path + "/" + containers[buck_idx] + "/~enum";
    var meta_dir = root_path + "/" + containers[buck_idx] + "/meta";
    var enum_base = {};
    var enum_base_raw = '{}';
    var _used_quota = 0;
    var temp_file = tmp_path+"/"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
    var child = exec('find '+ meta_dir +"/ -type f >"+temp_file,
      function (error, stdout, stderr) {
        if (!error) {
          var versions = {};
          try {
            versions = [temp_file];
          } catch (e) {
            fs.writeFileSync(temp_file+"-error"," "+e);
            fs.unlink(temp_file,function() {});
            return;
          }
          var evt2 = new events.EventEmitter();
          evt2.counter = versions.length;
          evt2.on('next',function(idx2) {
            var file1 = versions[idx2];
              evt2.counter--;
              fs.readFile(file1,function(err2,data) {
                var update_evt = new events.EventEmitter();
                update_evt.on('done',function() {
                  if (evt2.counter > 0) { evt2.emit('next',idx2+1); return; }
                  //UPDATE BASE
                  //TODO: race condition
                  //in a deployment, restrict ec to single instance for now
                  zlib.deflate(JSON.stringify(enum_base), function(err,buffer) {
                    if (err) {
                      fs.writeFileSync(temp_file+"-error"," "+err);
                    }
                    var sync_cnt = 0;
                    var temp_name = enum_dir+"/base-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
                    var failed_cnt = 0;
                    while (sync_cnt < MAX_WRITE_TRIES) { try { fs.writeFileSync(temp_name,buffer); } catch (e) {failed_cnt++;}; sync_cnt++; if (failed_cnt < sync_cnt) break; }
                    buffer = null;
                    if (failed_cnt >= sync_cnt) { 
                      sync_cnt=0;
                      while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name);} catch (e) { }; sync_cnt++; };
                      return;//can't write, give up
                    }
                    exec('mv '+temp_name+" "+enum_dir+"/base", function (error, stdout, stderr) {
                      var temp_name2 = enum_dir+"/quota-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
                      var obj_cnt = Object.keys(enum_base).length;
                      sync_cnt=0;
                      while (sync_cnt<MAX_WRITE_TRIES) { try { fs.writeFileSync(temp_name2,"{\"storage\":"+_used_quota+",\"count\":"+obj_cnt+"}"); } catch (e) {}; sync_cnt++; }
                      exec('mv '+temp_name2+" "+enum_dir+"/quota",function(error,stdout,stderr) {
                        for (var idx3=0; idx3<versions.length; idx3++) {
                          sync_cnt=0;
                          while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(versions[idx3]);} catch (e) { }; sync_cnt++; };
                        }
                      }); //end of exec renaming quota
                    }); //end of exec renaming base 
                  })// end of zip
                });//end of update_evt done
                if (!err2) {
                  var keys = [];
                  try { keys=data.toString().split("\n"); } catch (e) { }
                  console.log(keys.length+' files to process');
                  var collect_evt = new events.EventEmitter();
                  collect_evt.on('next_batch', function(current_idx) {
                    console.log('current batch for bucket '+containers[buck_idx]+' '+current_idx+' file processed');
                    if (current_idx >= keys.length) { update_evt.emit('done'); return; }
                    var batch_evt = new events.EventEmitter();
                    batch_evt.cnt = 0; batch_evt.target=(BATCH_NUM<keys.length-current_idx)?BATCH_NUM:(keys.length-current_idx);
                    batch_evt.on('next', function(current_key) {
                      if (current_key == "") {
                          batch_evt.cnt++; if (batch_evt.cnt >= batch_evt.target) { collect_evt.emit('next_batch',current_idx+batch_evt.target); } 
                      }
                      //CALC FINGERPRINT AND READ META
                      var filename = current_key;
                      fs.readFile(filename, function(err3,data2) {
                        try {
                          if (err3) throw 'error';
                          var obj2 = JSON.parse(data2);
                          var fn = obj2.vblob_file_name;
                          enum_base[fn] = {};
                          enum_base[fn].size = obj2.vblob_file_size;
                          enum_base[fn].etag = obj2.vblob_file_etag;
                          enum_base[fn].lastmodified = obj2.vblob_update_time;
                          //enum_base[fn].version = obj2.vblob_file_version; //debug purpose only, can disable to save space
                          _used_quota += enum_base[fn].size;
                        } catch (e) {
                        }
                        batch_evt.cnt++; if (batch_evt.cnt >= batch_evt.target) { collect_evt.emit('next_batch',current_idx+batch_evt.target); }
                      }); //end of readFile for meta_dir/pref1/pref2/filename
                    }); //end of batch_evt next
                    for (var i = 0; i < batch_evt.target; i++) {
                      var key = keys[i+current_idx];
                      batch_evt.emit('next',key);
                    } //end of for
                  }); //end of collect_evt next_batch
                  collect_evt.emit('next_batch',0);
                } else {
                  //?
                  update_evt.emit('done');
                }
              }); //end of readfile
          }); //end of next
          evt2.emit('next',0);
        } else {
          try {
            fs.unlinkSync(temp_file);
          } catch (e) { }
          console.error('error!' + error);
        }
      }
    ); //end of exec callback
});//end of on compact event
for (var i = 0; i < containers.length; i++)
  buck.emit('compact',i);
