/*
Copyright (c) 2011-2012 VMware, Inc.
*/
var fs = require('fs');
var events = require("events");
var exec = require('child_process').exec;
var crypto = require('crypto');
var zlib = require('zlib');

var BATCH_NUM = 3;
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
            return;
          }
          var deleted = 0;
          var failed_cnt = 0;
          while (deleted < MAX_DELETE_TRIES) {
            try {
              fs.unlinkSync(temp_file);
            } catch (e) {failed_cnt++; };
              deleted++;
            if (failed_cnt < deleted) break;
          }
          if (versions.length === 0 || versions.length === 1 && versions[0] === '') return;
          try {
            enum_base_raw = fs.readFileSync(enum_dir+"/base");
          } catch (e) {
            return;
          }
          zlib.unzip(enum_base_raw,function(err,buffer) {
            if (!err) {
              try {
                enum_base = JSON.parse(buffer);
              } catch (e) {
                return; //parse failed
              }
            } else {
              return; //unzip failed
            }
            buffer = null;
            var ivt_enum = {};
            var keys1 = Object.keys(enum_base);
            for (var nIdx1=0; nIdx1<keys1.length; nIdx1++) {
              var ver1 = enum_base[keys1[nIdx1]].version;
              if (ver1) { 
                ver1 = ver1.substr(0,ver1.lastIndexOf('-'));  //remove rand2
                ver1 = ver1.substr(0,ver1.lastIndexOf('-')); //remove rand1
                ver1 = ver1.substr(0,ver1.lastIndexOf('-')); //remove ts
              } else ver1 = get_key_fingerprint(keys1[nIdx1]); //in case version is not available
              ivt_enum[ver1] = keys1[nIdx1];
              _used_quota += enum_base[keys1[nIdx1]].size;
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
                        return; //can't deflate, give up
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
                        if (error) {
                          sync_cnt=0;
                          while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name);} catch (e) { }; sync_cnt++; };
                          return;//can't write, give up
                        }
                        var temp_name2 = enum_dir+"/quota-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
                        var obj_cnt = Object.keys(enum_base).length;
                        sync_cnt=0;
                        failed_cnt = 0;
                        while (sync_cnt<MAX_WRITE_TRIES) { try { fs.writeFileSync(temp_name2,"{\"storage\":"+_used_quota+",\"count\":"+obj_cnt+"}"); } catch (e) {failed_cnt++;}; sync_cnt++; if (failed_cnt < sync_cnt) break;}
                        if (failed_cnt >= sync_cnt) {
                          sync_cnt=0;
                          while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name2);} catch (e) { }; sync_cnt++; };
                          return;//can't write, give up
                        }
                        exec('mv '+temp_name2+" "+enum_dir+"/quota",function(error,stdout,stderr) {
                          if (error) {
                            sync_cnt=0;
                            while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(temp_name2);} catch (e) { }; sync_cnt++; };
                            return;//can't write, give up
                          }
                          for (var idx3=0; idx3<versions.length; idx3++) {
                            sync_cnt=0;
                            while (sync_cnt < MAX_DELETE_TRIES) { try { fs.unlinkSync(versions[idx3]);} catch (e) { }; sync_cnt++; };
                          }
                        }); //end of exec renaming quota
                      }); //end of exec renaming base 
                    })// end of zip
                  });//end of update_evt done
                  if (!err2) {
                    var obj = {};
                    try { obj = JSON.parse(data); } catch (e) { } //in case this file is truncated or corrupted
                    var keys = Object.keys(obj);
                    var collect_evt = new events.EventEmitter();
                    collect_evt.on('next_batch', function(current_idx) {
                      if (current_idx >= keys.length) { update_evt.emit('done'); return; }
                      var batch_evt = new events.EventEmitter();
                      batch_evt.cnt = 0; batch_evt.target=(BATCH_NUM<keys.length-current_idx)?BATCH_NUM:(keys.length-current_idx);
                      batch_evt.on('next', function(current_key) {
                        var got_hash = false;
                        if (obj[current_key] == 0) {
                          if (ivt_enum[current_key]) current_key = ivt_enum[current_key]; //look up file name by fingerprint
                          else {
                            got_hash = true;
                          }
                        }
                        //CALC FINGERPRINT AND READ META
                        var filename = got_hash?current_key:get_key_fingerprint(current_key);
                        ivt_enum[filename] = current_key;
                        var pref1 = filename.substr(0,PREFIX_LENGTH), pref2 = filename.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
                        fs.readFile(meta_dir+"/"+pref1+"/"+pref2+"/"+filename, function(err3,data2) {
                          try {
                            if (err3) throw err3;
                            var obj2 = JSON.parse(data2);
                            ivt_enum[filename] = obj2.vblob_file_name;
                            current_key = obj2.vblob_file_name;
                            var old_size = enum_base[current_key]?enum_base[current_key].size:0;
                            enum_base[current_key] = {};
                            enum_base[current_key].size = obj2.vblob_file_size;
                            enum_base[current_key].etag = obj2.vblob_file_etag;
                            enum_base[current_key].lastmodified = obj2.vblob_update_time;
                            //enum_base[key].version = obj2.vblob_file_version; //debug purpose only, can disable to save space
                            _used_quota += enum_base[current_key].size - old_size;
                          } catch (e) {
                            if (e.code == 'ENOENT' && enum_base[current_key]) {
                              _used_quota -= enum_base[current_key].size;
                              delete enum_base[current_key];
                            }
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
          });// end of unzip
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
