/*
Copyright (c) 2011-2012 VMware, Inc.
Author: wangs@vmware.com
*/
var fs = require('fs');
var Path = require('path');
var crypto = require('crypto');
var util = require('util');
var events = require("events");
var exec = require('child_process').exec;
var zlib = require('zlib');
var http = require('http');
var seq_id_cmp = require('./utils').seq_id_cmp;
var get_key_fingerprint = require('./utils').get_key_fingerprint;
var hex2base64 = require('./utils').hex2base64;
var PREFIX_LENGTH = 2; //how many chars we use for hash prefixes
var PREFIX_LENGTH2 = 1; //second level prefix length
var MAX_LIST_LENGTH = 1000; //max number of files to list
var TEMP_FOLDER = "~tmp";
var ENUM_FOLDER = "~enum";
var MAX_COPY_RETRY = 2;
var MAX_READ_RETRY = 2;
var MAX_DEL_RETRY = 6;
var gc_hash = {}; //for caching gc info;
var gc_counter = 0; //counter for gc info
var MAX_GC_QUEUE_LENGTH = 1600;
var enum_cache = {};
var enum_expire = {};
var enum_queue = {};

function common_header()
{
  var header = {};
  header.Server = "FS";
  return header;
}

function error_msg(statusCode,code,msg,resp)
{
  resp.resp_code = statusCode;
  resp.resp_header = common_header();
  resp.resp_body = {"Error":{
    "Code": code,
    "Message" : ( msg && msg.toString )? msg.toString() : ""
  }};
  //no additional info for now
}

function start_collector(option,fb)
{
  var node_exepath = option.node_exepath ? option.node_exepath : process.execPath;
  var ec_exepath = option.ec_exepath ? option.ec_exepath : __dirname+"/fs_ec.js";
  var ec_interval;
  try { if (isNaN(ec_interval = parseInt(option.ec_interval,10))) throw 'isNaN'; } catch (err) { ec_interval = 300; }
  fb.node_exepath = node_exepath;
  fb.ec_exepath = ec_exepath;
  fb.ec_interval = ec_interval;
  var ec_status = 0;
  fb.ecid = setInterval(function() {
    if (ec_status === 1) return; //already a gc process running
    ec_status = 1;
    //node fs_ec.js <blob root> <global tmp>
    exec(node_exepath + " " + ec_exepath + " " + fb.root_path + " --tmp " + fb.tmp_path + " --long_running_interval "+ec_interval+" > /dev/null",
        function(error,stdout, stderr) {
          ec_status = 0; //finished set to 0
          if (error || stderr) {
            var msg = 'enumeration collector error: ';
            try {
              msg += error?error:''+'-- '+stderr?stderr:'';
            } catch (e) { }
            fb.logger.warn(msg);
          }
        } );
    }, ec_interval);

}

function start_gc(option,fb)
{
  gc_hash = null; gc_hash = {}; gc_counter = 0;
  var tmp_path = option.tmp_path ? option.tmp_path : "/tmp";
  var node_exepath = option.node_exepath ? option.node_exepath : process.execPath;
  var gcfc_exepath = option.gcfc_exepath ? option.gcfc_exepath : __dirname+"/fs_gcfc.js";
  var gcfc_interval;
  var gctmp_interval;
  try { if (isNaN(gcfc_interval = parseInt(option.gcfc_interval,10))) throw 'isNaN'; } catch (err) { gcfc_interval = 300; }
  try { if (isNaN(gctmp_interval = parseInt(option.gctmp_interval,10))) throw 'isNaN'; } catch (err) { gctmp_interval = 60000; }
  var gctmp_exepath = option.gctmp_exepath ? option.gctmp_exepath : __dirname+"/fs_gctmp.js";
  fb.node_exepath = node_exepath;
  fb.gcfc_exepath = gcfc_exepath;
  fb.gctmp_exepath = gctmp_exepath;
  fb.gcfc_interval = gcfc_interval;
  fb.gctmp_interval = gctmp_interval;
  //gc from cache
  var gcfc_status = 0;
  fb.gcfcid = setInterval(function() {
    if (gcfc_status === 1 || gc_hash === null || Object.keys(gc_hash).length === 0) return; //optimization to avoid empty loop
    gcfc_status = 1;
    var tmp_fn = tmp_path+"/gcfc-"+new Date().valueOf()+"-"+Math.floor(Math.random()*10000)+"-"+Math.floor(Math.random()*10000);
    var tmp_hash = gc_hash;
    gc_hash = null;
    gc_hash = {};
    gc_counter = 0;
    fs.writeFile(tmp_fn,JSON.stringify(tmp_hash), function(err) {
      tmp_hash = null;
      if (err) { gcfc_status = 0; return; }
      exec(node_exepath + " " + gcfc_exepath + " " + tmp_fn + " " +fb.root_path + " --tmp " + tmp_path + " > /dev/null",
        function(error,stdout, stderr) {
          gcfc_status = 0; //finished set to 0
          if (error || stderr) {
            var msg = 'light weight garbage collector error: ';
            try {
              msg += error?error:''+'-- '+stderr?stderr:'';
            } catch (e) { }
            fb.logger.warn(msg);
          }
          fs.unlink(tmp_fn,function() {} );
        } );
    });
   }, gcfc_interval);
  //gc tmp
  var gctmp_status = 0;
  fb.gctmpid = setInterval(function() {
    if (gctmp_status === 1) return; //already a gc process running
    gctmp_status = 1;
    exec(node_exepath + " " + gctmp_exepath + " " + fb.root_path + " > /dev/null",
        function(error,stdout, stderr) {
          if (error || stderr) {
            var msg = 'tmp garbage collector error: ';
            try {
              msg += error?error:''+'-- '+stderr?stderr:'';
            } catch (e) { }
            fb.logger.warn(msg);
          }
          gctmp_status = 0; //finished set to 0
        } );
    }, gctmp_interval);
}

function start_quota_gathering(fb)
{
  fs.readdir(fb.root_path, function(err, dirs) {
    if (err) {
      setTimeout(start_quota_gathering, 1000, fb);
      return;
    }
    var evt = new events.EventEmitter();
    var counter = dirs.length;
    var sum = 0, sum2 = 0;
    var used_quota = new Array(dirs.length);
    var obj_count = new Array(dirs.length);
    evt.on("Get Usage",function (dir_name, idx) {
      fs.readFile(fb.root_path+"/"+dir_name+"/~enum/quota", function(err,data) {
          if (err) { obj_count[idx] = null; used_quota[idx] = null; } else
          { try { var obj = JSON.parse(data); obj_count[idx] = parseInt(obj.count,10); used_quota[idx] = parseInt(obj.storage,10); } catch (e) { obj_count[idx] = null; used_quota[idx] = null; } }
          counter--; if (counter === 0) { evt.emit("Start Aggregate"); }
      });
    });
    evt.on("Start Aggregate", function () {
      for (var i = 0; i < dirs.length; i++) {
        if (used_quota[i] === null)  { continue; }
        sum += used_quota[i]; sum2 += obj_count[i];
      }
      fb.used_quota = sum; fb.obj_count = sum2;
      //console.log('usage: ' + sum +' count: '+sum2);
      setTimeout(start_quota_gathering,1000,fb);
    });
    if (dirs.length === 0) { evt.emit("Start Aggregate"); }
    for (var i = 0; i < dirs.length; i++)
    { evt.emit("Get Usage",dirs[i],i); }
  });
}

function FS_blob(option,callback)  //fow now no encryption for fs
{
  var this1 = this;
  this.root_path = option.root; //check if path exists here
  this.tmp_path = option.tmp_path ? option.tmp_path : "/tmp";
  this.logger = option.logger;
  if (option.quota) { this.quota = parseInt(option.quota,10); this.used_quota = 0; }
  else {this.quota = 100 * 1024 * 1024; this.used_quota=0;} //default 100MB
  if (option.obj_limit) { this.obj_limit = parseInt(option.obj_limit, 10); this.obj_count = 0; }
  else {this.obj_limit=100000; this.obj_count=0;} //default 100,000 objects
  if (option.seq_host) { this.seq_host = option.seq_host; } else this.seq_host = "localhost";
  if (option.seq_port) { this.seq_port = parseInt(option.seq_port,10); } else this.seq_port = 9876;
  if (option.meta_host) { this.meta_host = option.meta_host; } else this.meta_host = "localhost";
  if (option.meta_port) { this.meta_port = parseInt(option.meta_port,10); } else this.meta_port = 9877;
  if (option.read_direct) this.read_direct = option.read_direct;
  if (!this1.root_path) {
    this1.root_path = './fs_root'; //default fs root
    try {
      fs.mkdirSync(this1.root_path, "0775");
    } catch (err) {
      if (err.code != 'EEXIST') {
        this1.logger.error( ('default root folder creation error: '+err));
        if (callback) { callback(this1,err); }
        return;
      }
    }
  }
  fs.stat(this1.root_path, function(err,stats) {
    if (!err) {
      start_gc(option,this1);
      //set enumeration on by default
      if (option.single_node == true || option.collector == true) {
        this.collector = true;
        start_collector(option,this1);
        //as long as enumeration is on, quotas is enabled as well
        setTimeout(start_quota_gathering, 1000, this1);
      }
      if (option.single_node == true) {
        //start sequence server
        var node_exepath = option.node_exepath ? option.node_exepath : process.execPath;
        var seq_exepath =  __dirname+"/seq_server.js";
        exec(node_exepath+" "+seq_exepath+" --epoch "+this1.root_path+"/epoch_file" + " --root "+this1.root_path+" 2>&1 >/dev/null", function(error,stdout,stderr) {
        })
      }
    } else { this1.logger.error( ('root folder in fs driver is not mounted')); }
    if (callback) { callback(this1,err); }
  });
}

FS_blob.prototype.container_create = function(container_name,callback,fb)
{
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  fs.stat(fb.root_path+"/"+container_name+"/ts", function(err,stats) {
    if (stats) {fb.logger.debug("container_name "+container_name+" exists!");
      resp_code = 200;
      var header = common_header();
      header.Location = '/' + container_name;
      resp_header = header;
      callback(resp_code, resp_header, null, null);
      return;
    }
    var c_path = fb.root_path + "/" + container_name;
    try {
      if (Path.existsSync(c_path) === false)
      {
        fb.logger.debug("path "+c_path+" does not exist! Let's create one");
        fs.mkdirSync(c_path,"0775");
      } else
      {
        fb.logger.debug(("path "+c_path+" exists!"));
      }
      if (Path.existsSync(c_path+"/"+TEMP_FOLDER) === false)
      {
        fs.mkdirSync(c_path+"/"+TEMP_FOLDER,"0775");
      }
      if (Path.existsSync(c_path+"/"+ENUM_FOLDER) === false)
      {
        fs.mkdirSync(c_path+"/"+ENUM_FOLDER,"0775");
      }
      fs.writeFileSync(c_path+"/"+ENUM_FOLDER+"/base", "{}");
      if (Path.existsSync(c_path+"/ts") === false) //double check ts
      {
        fb.logger.debug( ("timestamp "+c_path+"/ts does not exist. Need to create one"));
        fs.writeFileSync(c_path+"/ts", "DEADBEEF");
      } else
      {
        fb.logger.debug( ("timestamp "+c_path+"/ts exists!"));
      }
    } catch (err1) {
      var resp = {};
      error_msg(500,"InternalError","Cannot create bucket because: "+err1,resp);
      callback(resp.resp_code, resp.resp_header, resp.resp_body,null);
      return;
    }
    resp_code = 200;
    var header = common_header();
    header.Location = '/'+container_name;
    resp_header = header;
    callback(resp_code, resp_header, null, null);
  });
};

//delete a container_name; fail if it's not empty
//deleting a container is generally considered rare, and we don't care too much about
//its performance or isolation
FS_blob.prototype.container_delete = function(container_name,callback,fb)
{
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  var c_path = fb.root_path + "/" + container_name+"/versions";
  if (Path.existsSync(c_path) === false)
  { //shortcut, remove directly
    var child = exec('rm -rf '+fb.root_path+"/"+container_name,
      function (error, stdout, stderr) {
        var header = common_header();
        resp_code = 204; resp_header = header;
        callback(resp_code, resp_header, null, null);
      }
    );
    return;
  }
  http.get({hostname:fb.seq_host, port:fb.seq_port, headers:{bucket:container_name,op:'GET'}},function(res) {
    var start_seq_id = res.headers["seq-id"];
    fs.readdir(fb.root_path + "/" + container_name + "/~tmp", function(err,files) {
      if (err) {
        var resp = {};
        error_msg(500,"InternalError",err,resp);
        resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
        callback(resp_code, resp_header, resp_body, null);
        return;
      }
      for (var idx1=0; idx1 < files.length; idx1++) {
        if (
            files[idx1].match("\-[0-9]+\-[0-9]+$") ||
            files[idx1].match("\-blob$")
           ) {
          var resp = {};
          error_msg(409,"BucketNotEmpty","The bucket you tried to delete is not empty.",resp);
          resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
          callback(resp_code, resp_header, resp_body, null);
          return;
        }
      }
      fs.readdir(fb.root_path+"/"+container_name+"/~enum",function(err,files2) {
        if (err) {
          var resp = {};
          error_msg(500,"InternalError",err,resp);
          resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
          callback(resp_code, resp_header, resp_body, null);
          return;
        }
        for (var idx2=0; idx2 < files2.length; idx2++) {
          if ( files2[idx2].match("^delta\-") ) {
            var resp = {};
            error_msg(409,"BucketNotEmpty","The bucket you tried to delete is not empty.",resp);
            resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
            callback(resp_code, resp_header, resp_body, null);
            return;
          }
        }
        //contact meta server for emptiness
        http.get({hostname:fb.meta_host, port:fb.meta_port, path:'/'+container_name}, function(res2) {
          //check if _objects = 0
          if (res2.statusCode != 200) {
            var resp = {};
            error_msg(500,"InternalError","Cannot get bucket information.",resp);
            resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
            callback(resp_code, resp_header, resp_body, null);
            return;
          }
          if (res2.headers["objects"] != 0) {
            var resp = {};
            error_msg(409,"BucketNotEmpty","The bucket you tried to delete is not empty.",resp);
            resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
            callback(resp_code, resp_header, resp_body, null);
            return;
          }
          //send to seq server do to ats delete
          http.get({hostname:fb.seq_host, port:fb.seq_port, headers:{op:"DELETE", bucket:container_name, "seq-id":start_seq_id}},function(res3) {
            if (res3.statusCode != 200) {
              var resp = {};
              error_msg(409,"BucketNotEmpty","The bucket you tried to delete is not empty.",resp);
              resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
              callback(resp_code, resp_header, resp_body, null);
              return;
            }
            var child = exec('rm -rf '+fb.root_path+"/"+res3.headers["location"],
              function (error, stdout, stderr) {
                var header = common_header();
                resp_code = 204; resp_header = header;
                callback(resp_code, resp_header, null, null);
              }
            ); //end of rm 
          }).on('error', function(err) {
            var resp = {};
            error_msg(500,"InternalError",err,resp);
            resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
            callback(resp_code, resp_header, resp_body, null);
            return;
          }); //http get seq for delete
        }).on('error', function(err) {
          var resp = {};
          error_msg(500,"InternalError",err,resp);
          resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
          callback(resp_code, resp_header, resp_body, null);
          return;
        }); //http.get meta
      }); //fs.readdir enum
    });//fs.readdir tmp
  }).on('error',function(err) { //http error
    var resp = {};
    error_msg(500,"InternalError",err,resp);
    resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
    callback(resp_code, resp_header, resp_body, null);
  });
};

//need to revisit sync operation on FS in this check
// currently necessary for PUT (to avoid losing events at the beginning of the request)
// not necessary for other operations - could call async version of this for better concurrency
// revisit for perf when perf is revisited
function container_exists(container_name, callback,fb)
{
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  var c_path = fb.root_path + "/" + container_name;
  if (!Path.existsSync(c_path)) {
    fb.logger.error( ("no such container_name"));
    var resp = {};
    error_msg(404,"NoSuchBucket","No such bucket on disk",resp);
    resp_code = resp.resp_code; resp_header = resp.resp_header; resp_body = resp.resp_body;
    callback(resp_code, resp_header, resp_body, null);
    return false;
  }
  return true;
}

function generate_version_id(key)
{
  var da = new Date().valueOf();
  return key+'-'+da+'-'+Math.floor(Math.random()*1000)+'-'+Math.floor(Math.random()*1000);
}

function create_prefix_folders(prefix_array, callback)
{
  var resp = {};
  error_msg(404,"NoSuchBucket","Bucket does not exist.",resp);
  var path_pref = null;
  for (var idx = 0; idx < prefix_array.length; idx++) {
    if (path_pref === null) path_pref = prefix_array[idx];
    else path_pref = path_pref + "/" + prefix_array[idx];
    if (!Path.existsSync(path_pref)) {
      try {
        fs.mkdirSync(path_pref,"0775");
      } catch(err) {
        if (err.code !== 'EEXIST') {
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          return false;
        }
        //EEXIST: OK to proceed
        //ENOENT: error response no such container
      }
    }
  }
  return true;
}

FS_blob.prototype.file_create = function (container_name,filename,create_options, create_meta_data, data,callback,fb)
{
  var resp = {};
  //throttle control
  if (gc_counter > MAX_GC_QUEUE_LENGTH) {
    error_msg(503,"SlowDown","Please reduce your request rate.",resp);
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    return;
  }
//step 1 check container existence
  var c_path = this.root_path + "/" + container_name;
  if (container_exists(container_name,callback,fb) === false) return;
  //QUOTA
  if (this.quota && this.used_quota + parseInt(create_meta_data["content-length"],10) > this.quota || this.obj_limit && this.obj_count >= this.obj_limit) {
    error_msg(500,"UsageExceeded","Usage will exceed the quota",resp);
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    return;
  }
//step2.1 calc unique hash for key
  var key_fingerprint = get_key_fingerprint(filename);
//step2.2 gen unique version id
  var version_id = generate_version_id(key_fingerprint);
  var prefix1 = key_fingerprint.substr(0,PREFIX_LENGTH), prefix2 = key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
  var prefix_path = prefix1 + "/" + prefix2 + "/";
  var temp_path = c_path + "/" + TEMP_FOLDER +"/" + version_id;
  var blob_path = c_path + "/blob/" + prefix_path + version_id;
  var temp_blob_path = temp_path + "-blob";
  var meta_json = { vblob_file_name : filename, vblob_file_path : "blob/"+prefix_path+version_id };
//step 3 synchronously creating folders needed (in order not to lose any data events)
  if (!create_prefix_folders([c_path+"/blob",prefix1,prefix2],callback)) return;
  if (!create_prefix_folders([c_path+"/versions", prefix1,prefix2,key_fingerprint],callback)) return;
//step 4 stream blob
  var stream = fs.createWriteStream(temp_blob_path);
  var md5_etag = crypto.createHash('md5');
  var md5_base64 = null;
  var file_size = 0;
  var upload_failed = false;
  var blob_fd = null;
  stream.on('open', function(fd) { blob_fd = fd; });
  stream.on("error", function (err) {
    upload_failed = true;
    fb.logger.error( ("write stream " + temp_blob_path+" "+err));
    if (resp !== null) {
      error_msg(500,"InternalError",err,resp);
      callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    }
    if (data) data.destroy();
    if (blob_fd) fs.close(blob_fd, function(err) { fs.unlink(temp_blob_path,function(err){});});
    else fs.unlink(temp_blob_path,function(err){});
  });
  data.on("error", function (err) {
    upload_failed = true;
    if (resp !== null) {
      error_msg(500,"InternalError",err,resp);
      callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    }
    fb.logger.error( ('input stream '+temp_blob_path+" "+err));
    if (data) data.destroy();
    if (blob_fd) fs.close(blob_fd, function(err) { fs.unlink(temp_blob_path,function(err){});});
    else fs.unlink(temp_blob_path,function(err){});
  });
  data.on("data",function (chunk) {
    md5_etag.update(chunk);
    file_size += chunk.length;
    if (!stream || stream.destroyed) return;
    if (stream.write(chunk) == false) {
      data.pause();
      stream.once('drain',function() {
        data.resume();
      });
    }
  });
  data.on("end", function () {
    fb.logger.debug( ('upload ends '+temp_blob_path));
    data.upload_end = true;
    stream.destroyed = true;
    stream.end();
    stream.destroySoon();
  });

  var closure1 = function(md5_etag) {
    var opts = {vblob_file_name: filename, vblob_file_path: "blob/"+prefix_path+version_id, vblob_file_etag : md5_etag, vblob_file_size : file_size, vblob_file_version : version_id, vblob_file_fingerprint : key_fingerprint};
    if (create_options['content-md5']) {
      //check if content-md5 matches
      md5_base64 = hex2base64(md5_etag);
      if (md5_base64 !== create_options['content-md5']) // does not match
      {
        if (resp !== null) {
          error_msg(400,"InvalidDigest","The Content-MD5 you specified was invalid.",resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
        }
        fb.logger.error( (filename+' md5 not match: uploaded: '+ md5_base64 + ' specified: ' + create_options['content-md5']));
        data.destroy();
        fs.unlink(temp_blob_path,function(err) {});
        return;
      }
    }
    var keys = Object.keys(create_meta_data);
    for (var idx = 0; idx < keys.length; idx++) {
      var obj_key = keys[idx];
      if (obj_key.match(/^x-amz-meta-/i)) {
        var sub_key = obj_key.substr(11);
        sub_key = "vblob_meta_" + sub_key;
        opts[sub_key] = create_meta_data[obj_key];
      } else if (obj_key.match(/^content-length$/i)) {
        continue; //actual file size is already calculated
      } else opts[obj_key] = create_meta_data[obj_key];
    }
    //step 5 starting to write meta and commit
    fb.file_create_meta(container_name,filename,temp_path,opts,callback,fb,!data.connection);
  };

  stream.once("close", function() {
    if (upload_failed) {
      if (resp !== null) {
        error_msg(500,"InternalError","upload failed",resp);
        callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
      }
      fs.unlink(temp_blob_path, function(err) { });
      return;
    }
    fb.logger.debug( ("close write stream "+filename));
    md5_etag = md5_etag.digest('hex');
    closure1(md5_etag);
  });

  if (data.connection) // copy stream does not have connection
  {
    data.connection.once('close',function() {
      fb.logger.debug( ('client disconnect'));
      if (data.upload_end === true) { return; }
      upload_failed = true;
      fb.logger.warn( ('interrupted upload: ' + filename));
      data.destroy();
      if (blob_fd) fs.close(blob_fd, function(err) { fs.unlink(temp_blob_path,function(err){});});
      else fs.unlink(temp_blob_path,function(err){});
    });
  }
};

FS_blob.prototype.file_create_meta = function (container_name, filename, temp_path, opt,callback,fb,is_copy, is_delete)
{
  var resp = {};
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
//step 5.5 update meta
  if (opt === undefined) { opt = null; }
  if (resp === undefined) { resp = null; }
  var doc = {};
  if (opt !== null) {
    for (var key in opt)
    { doc[key] = opt[key]; }
  }
  var dDate = new Date();
  doc.vblob_update_time = dDate.toUTCString().replace(/UTC/ig, "GMT"); //RFC 822
  doc.vblob_file_name = filename;
  var seq_id;
  //step 5.6 getting a globally ordered sequence(tx) id from sequence server
  http.get({hostname:fb.seq_host,port:fb.seq_port,headers:{bucket:container_name}}, function(res) {
  seq_id = res.headers["seq-id"];
  doc.vblob_seq_id = seq_id;
  doc.vblob_file_path = doc.vblob_file_path.substring(0,doc.vblob_file_path.lastIndexOf('/')+1)+doc.vblob_file_fingerprint+"-"+seq_id; 
  //step 5.7 writing to meta
  fs.writeFile(temp_path+"-"+seq_id,JSON.stringify(doc), function(err) {
    if (err) {
      fb.logger.error( ("In creating file "+filename+" meta in container_name "+container_name+" "+err));
      if (resp !== null) {
        error_msg(404,"NoSuchBucket",err,resp);
        callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
      }
      fs.unlink(temp_path+"-"+seq_id,function(err) {});
      fs.unlink(temp_path+"-blob",function(err){});
      return;
    }
    fb.logger.debug( ("Created meta for file "+filename+" in container_name "+container_name));
    var header = common_header();
    if (!is_delete) header.ETag = '"'+opt.vblob_file_etag+'"';
    resp.resp_code = is_delete?204:200; resp.resp_body = null;
    fb.logger.debug( ('is_copy: ' + is_copy));
    if (is_copy) {
      resp.resp_body = {"CopyObjectResult":{"LastModified":new Date(doc.vblob_update_time).toISOString(),"ETag":'"'+opt.vblob_file_etag+'"'}};
      resp.resp_header = header;
    } else {
      resp.resp_header = header;
    }
    //step 5.8 renaming blob from tmp folder to blob folder with correct seq-id in name
    fs.rename(temp_path+"-blob",fb.root_path+"/"+container_name + "/"+doc.vblob_file_path, function(err) {
      if (err) {
        fb.logger.error( ("In renaming file "+filename+" blob in container_name "+container_name+" "+err));
        if (resp !== null) {
          if (!is_delete) error_msg(500,"InternalError",err,resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
        }
        fs.unlink(temp_path+"-"+seq_id,function(err) {});
        fs.unlink(temp_path+"-blob",function(err){});
        return;
      }
      //step 6 hard link to versions. This is a commit, and after this step it's always going to be re-done in recovery
      var prefix1 = doc.vblob_file_version.substr(0,PREFIX_LENGTH), prefix2 = doc.vblob_file_version.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
        //link to version, so version link > 1, now gctmp will consider it a committed put
      fs.link(temp_path+"-"+seq_id, fb.root_path + "/"+container_name+"/versions/" + prefix1 + "/" + prefix2 + "/" + doc.vblob_file_fingerprint + "/" + doc.vblob_file_fingerprint+"-"+seq_id,function (err) {
        if (err) {
          fs.unlink(temp_path+"-"+seq_id,function(err) {});
          fs.unlink(fb.root_path+"/"+container_name+"/"+doc.vblob_file_path,function(err){});
          fb.logger.error( ("In creating file "+filename+" meta in container_name "+container_name+" "+err));
          if (resp !== null) {
            if (!is_delete) error_msg(500,"InternalError",err,resp);
            callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          }
          return;
        }
        //add to gc cache
        gc_counter++;
        if (!gc_hash[container_name]) gc_hash[container_name] = {};
        if (!gc_hash[container_name][doc.vblob_file_fingerprint]) { 
          gc_hash[container_name][doc.vblob_file_fingerprint] = {
            ver:[doc.vblob_file_version+"-"+seq_id],
            meta:[
                   {
                     vblob_file_etag:doc.vblob_file_etag,
                     vblob_update_time:doc.vblob_update_time,
                     vblob_seq_id:doc.vblob_seq_id,
                     vblob_file_size:doc.vblob_file_size
                   }
                 ],
            fn:doc.vblob_file_name
          }; 
        } else {
          gc_hash[container_name][doc.vblob_file_fingerprint].ver.push(doc.vblob_file_version+"-"+seq_id);
          gc_hash[container_name][doc.vblob_file_fingerprint].meta.push(
            {
              vblob_file_etag:doc.vblob_file_etag,
              vblob_update_time:doc.vblob_update_time,
              vblob_seq_id:doc.vblob_seq_id,
              vblob_file_size:doc.vblob_file_size
            }
          );
        }
        fb.logger.debug("file creation "+doc.vblob_file_version+" complete, now reply back...");
        callback(resp.resp_code, resp.resp_header, resp.resp_body,null);
      }); //end of linking temp to version
    }); //end of renaming temp blob to blob/version
  }); //end of write meta file
  }).on('error', function(err) {
    fs.unlink(temp_path+"-blob",function(e){});
    error_msg(500,"InternalError",err,resp);
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
  });//end of getting sequence
};

FS_blob.prototype.file_delete_meta = function (container_name, filename, callback, fb)
{
  var resp = {};
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  var c_path = fb.root_path + "/" + container_name;
  if (container_exists(container_name,callback,fb) === false) return;
//step2.1 calc unique hash for key
  var key_fingerprint = get_key_fingerprint(filename);
//step2.2 gen unique version id
  //generate a fake version, just a place holder to let gc know there are work to do
  var version_id = generate_version_id(key_fingerprint);
  var prefix1 = key_fingerprint.substr(0,PREFIX_LENGTH), prefix2 = key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
  if (!create_prefix_folders([c_path+"/blob",prefix1,prefix2],callback)) return;
  if (!create_prefix_folders([c_path+"/versions", prefix1,prefix2,key_fingerprint],callback)) return;
 //we explicitly generate a delete marker for both blob and meta, so delete and put follow the same procedure
  fs.writeFile(c_path+"/~tmp/"+version_id+"-blob", '', function(err) {
    if (err) {
      error_msg(500,"InternalError",err,resp);
      callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
      return;
    }
    var obj = {vblob_file_name: filename, vblob_file_path: "blob/"+prefix1+"/"+prefix2+"/"+version_id, vblob_file_size : 0, vblob_file_version : version_id, vblob_file_fingerprint : key_fingerprint};
    //ready to call file_create_meta
    fb.file_create_meta(container_name,filename, c_path+"/~tmp/"+version_id, obj, callback, fb, false, true);
  });
};

FS_blob.prototype.file_copy = function (container_name,filename,source_container,source_file,options, metadata, callback,fb, retry_cnt)
{
  var resp = {};
//step 1 check container existence
  var c_path = this.root_path + "/" + container_name;
  var src_path = this.root_path + "/" + source_container;
  if (container_exists(container_name,callback,fb) === false) return;
  if (container_exists(source_container,callback,fb) === false) return ;
//step2.1 calc unique hash for key
  var key_fingerprint = get_key_fingerprint(filename);
  var src_key_fingerprint = get_key_fingerprint(source_file);
//step2.2 gen unique version id
  var version_id = generate_version_id(key_fingerprint);
//step3 create meta file in ~tmp (probably create parent folders)
  var prefix1 = key_fingerprint.substr(0,PREFIX_LENGTH), prefix2 = key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
  var src_prefix1 = src_key_fingerprint.substr(0,PREFIX_LENGTH), src_prefix2 = src_key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2);
  var prefix_path = prefix1 + "/" + prefix2 + "/";
  var src_prefix_path = src_prefix1 + "/" + src_prefix2 + "/";
  var temp_path = c_path + "/" + TEMP_FOLDER +"/" + version_id;
  var blob_path = c_path + "/blob/" + prefix_path + version_id;
  var src_meta_path = src_path + "/versions/" + src_prefix_path + src_key_fingerprint;
  var seq_id;
  var etag_match=null, etag_none_match=null, date_modified=null, date_unmodified=null;
  var meta_dir=null;
  var keys = Object.keys(options);
  for (var idx = 0; idx < keys.length; idx++)
  {
    if (keys[idx].match(/^x-amz-copy-source-if-match$/i))
    { etag_match = options[keys[idx]]; }
    else if (keys[idx].match(/^x-amz-copy-source-if-none-match$/i))
    { etag_none_match = options[keys[idx]]; }
    else if (keys[idx].match(/^x-amz-copy-source-if-unmodified-since$/i))
    { date_unmodified = options[keys[idx]]; }
    else if (keys[idx].match(/^x-amz-copy-source-if-modified-since$/i))
    { date_modified = options[keys[idx]]; }
    else if (keys[idx].match(/^x-amz-metadata-directive$/i))
    { meta_dir = options[keys[idx]]; }
  }
  if (meta_dir === null) { meta_dir = 'COPY'; }
  else { meta_dir = meta_dir.toUpperCase(); }
  if ((meta_dir !== 'COPY' && meta_dir !== 'REPLACE') ||
      (etag_match && date_modified) ||
      (etag_none_match && date_unmodified) ||
      (date_modified && date_unmodified)  ||
      (etag_match && etag_none_match) ) {
    error_msg(400,"NotImplemented","The headers are not supported",resp); //same as S3 does
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    return;
  }

  var closure3 = function() {
    //read src meta here
    fs.readFile(src_meta_path+"-"+seq_id, function(err,data) {
      if (err) {
        if (!retry_cnt) retry_cnt = 0;
        if (retry_cnt < MAX_COPY_RETRY) { //suppress temporary failures from underlying storage
          setTimeout(function(fb1) { delete options.seq_id; fb1.file_copy(container_name, filename, source_container, source_file, options, metadata, callback,fb1, retry_cnt+1); }, Math.floor(Math.random()*1000) + 100,fb);
          return;
        }
        error_msg(404,"NoSuchFile",err,resp);
        callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
        return;
      }
      var obj = JSON.parse(data);
      //QUOTA
      if (source_container !== container_name || source_file !== filename) {
        if (fb.quota && fb.used_quota + obj.vblob_file_size > fb.quota ||
            fb.obj_limit && fb.obj_count >= fb.obj_limit) {
          error_msg(500,"UsageExceeded","Usage will exceed quota",resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          return;
        }
      }
      //check etag, last modified
      var check_modified = true;
      var t1,t2;
      if (date_modified) {
        t1 = new Date(date_modified).valueOf();
        t2 = new Date(obj.vblob_update_time).valueOf();
        check_modified = t2 > t1 || t1 >  new Date().valueOf();
      } else if (date_unmodified) {
        t1 = new Date(date_unmodified).valueOf();
        t2 = new Date(obj.vblob_update_time).valueOf();
        check_modified = t2 <= t1;
      }
      if ((etag_match && obj.vblob_file_etag !== etag_match) ||
          (etag_none_match && obj.vblob_file_etag === etag_none_match) ||
          check_modified === false)
      {
        error_msg(412,"PreconditionFailed","At least one of the preconditions you specified did not hold.",resp);
        callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
        return;
      }
      var keys,keys2;  var idx; //supress warning
      var dest_obj = {};
      //TODO: more meta to copy (cache-control, encoding, disposition, expires, etc.)
      dest_obj.vblob_file_etag = obj.vblob_file_etag;
      dest_obj.vblob_file_size = obj.vblob_file_size;
      if (obj["content-type"]) dest_obj["content-type"] = obj["content-type"];
      if (obj["cache-control"]) dest_obj["cache-control"] = obj["cache-control"];
      if (obj["content-disposition"]) dest_obj["content-disposition"] = obj["content-disposition"];
      if (obj["content-encoding"]) dest_obj["content-encoding"] = obj["content-encoding"];
      if (obj["expires"]) dest_obj["expires"] = obj["expires"];
      dest_obj.vblob_file_version = version_id;
      dest_obj.vblob_file_fingerprint = key_fingerprint;
      dest_obj.vblob_file_path = "blob/"+prefix_path+version_id;//blob_path;
      keys = Object.keys(obj);
      if (meta_dir === 'COPY') {
        if (source_container === container_name && source_file === filename) {
            error_msg(400,"NotImplemented","The headers are not supported",resp);
            callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
            return;
        }
        for (idx = 0; idx < keys.length; idx++) {
          var key = keys[idx];
          if (key.match(/^vblob_meta_/i)) {
            dest_obj[key] = obj[key];
          }
        }
      } else {
        keys = Object.keys(metadata);
        for (idx = 0; idx < keys.length; idx++) {
          var key = keys[idx];
          if (key.match(/^x-amz-meta-/i)) {
            var key2 = key.replace(/^x-amz-meta-/i,"vblob_meta_");
            dest_obj[key2] = metadata[key];
          } else if (!key.match(/^content-length/i)) dest_obj[key] = metadata[key];
        }
      }
      dest_obj.vblob_file_size = obj.vblob_file_size; //not to override content-length!!
      //new file meta constructed, ready to create links etc.
      if (!create_prefix_folders([c_path+"/blob",prefix1,prefix2],callback)) return;
      if (!create_prefix_folders([c_path+"/versions", prefix1,prefix2,key_fingerprint],callback)) return;
      fs.link(src_path+"/"+obj.vblob_file_path, c_path+"/~tmp/"+version_id+"-blob", function(err) {
        if (err) {
          if (!retry_cnt) retry_cnt = 0;
          if (retry_cnt < MAX_COPY_RETRY) { //suppress temporary failures from underlying storage
            setTimeout(function(fb1) { delete options.seq_id; fb1.file_copy(container_name, filename, source_container, source_file, options, metadata, callback,fb1, retry_cnt+1); }, Math.floor(Math.random()*1000) + 100,fb);
            return;
          }
          error_msg(500,"InternalError",err,resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          return;
        }
        //ready to call file_create_meta
        fb.file_create_meta(container_name,filename, c_path+"/~tmp/"+version_id, dest_obj, callback, fb, true);
      });
    });
  }; // end of closure3
  if (options.seq_id) { seq_id = options.seq_id; closure3(); }
  else http.get("http://"+fb.meta_host+":"+fb.meta_port+"/"+source_container+"/"+source_file, function (res) {
    if (res.statuCode == 404) { 
      error_msg(404,"NoSuchFile",err,resp); callback(resp.resp_code, resp.resp_header, resp.resp_body, null); return;
    } else {
      seq_id = res.headers["seq-id"];
      options.seq_id = seq_id;
      closure3();
    }
  }).on('error', function(err) {
    error_msg(500,"InternalError",err,resp);
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
  });
};

FS_blob.prototype.file_read = function (container_name, filename, options, callback, fb, retry_cnt)
{
  var range = options.range;
  var verb = options.method;
  var resp = {}; //for error_msg
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  var c_path = this.root_path + "/" + container_name;
  if (container_exists(container_name,callback,this) === false) return;
//step2.1 calc unique hash for key
  var key_fingerprint = get_key_fingerprint(filename);
//step2.2 gen unique version id
  var file_path = c_path + "/versions/" + key_fingerprint.substr(0,PREFIX_LENGTH)+"/"+key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2)+"/"+key_fingerprint+"/"+key_fingerprint; //complete representation: /container_name/filename
//    error_msg(404,"NoSuchFile","No such file",resp);resp.resp_end();return;
  var etag_match=null, etag_none_match=null, date_modified=null, date_unmodified=null;
  var keys = Object.keys(options);
  for (var idx = 0; idx < keys.length; idx++)
  {
    if (keys[idx].match(/^if-match$/i))
    { etag_match = options[keys[idx]]; }
    else if (keys[idx].match(/^if-none-match$/i))
    { etag_none_match = options[keys[idx]]; }
    else if (keys[idx].match(/^if-unmodified-since$/i))
    { date_unmodified = options[keys[idx]]; }
    else if (keys[idx].match(/^if-modified-since$/i))
    { date_modified = options[keys[idx]]; }
  }
  var seq_id;
  var closure2 = function() {
    //read meta here
    fs.readFile(file_path+"-"+seq_id,function (err, data) {
      if (err) {
        //suppress temporary failures from underlying storage
        if (!retry_cnt) retry_cnt = 0;
        if (retry_cnt < MAX_READ_RETRY) {
          delete options.seq_id;
          setTimeout(function(fb1) { fb1.file_read(container_name, filename, options, callback,fb1, retry_cnt+1); }, Math.floor(Math.random()*1000) + 100,fb);
          return;
        }
        error_msg(404,"NoSuchFile",err,resp); callback(resp.resp_code, resp.resp_header, resp.resp_body, null); return;
      }
      var obj = JSON.parse(data);
      var header = common_header();
  //    if (file_size !== obj.vblob_file_size) {
  //      error_msg(500,"InternalError","file corrupted",resp); resp.resp_end(); return;
  //    }
      var modified_since=true, unmodified_since=true;
      var t1,t2;
      if (date_modified) {
        t1 = new Date(date_modified).valueOf();
        t2 = new Date(obj.vblob_update_time).valueOf();
        modified_since = t2 > t1 || t1 > new Date().valueOf(); //make sure the timestamp is not in the future
      } else if (date_unmodified) {
        t1 = new Date(date_unmodified).valueOf();
        t2 = new Date(obj.vblob_update_time).valueOf();
        unmodified_since = t2 <= t1;
      }
      //412
      if (unmodified_since === false ||
          etag_match && etag_match !== obj.vblob_file_etag)
      {
        error_msg(412,"PreconditionFailed","At least one of the preconditions you specified did not hold.",resp); callback(resp.resp_code, resp.resp_header, resp.resp_body, null); return;
      }
      //304
      if (modified_since === false ||
          etag_none_match && etag_none_match === obj.vblob_file_etag)
      {
        error_msg(304,'NotModified','The object is not modified',resp);
        resp.resp_header.etag = obj.vblob_file_etag; resp.resp_header["last-modified"] = obj.vblob_update_time;
        callback(resp.resp_code, resp.resp_header, /*resp.resp_body*/ null, null); //304 should not have body
        return;
      }
      header["content-type"] = obj["content-type"] ? obj["content-type"] :  "binary/octet-stream";
      header["Content-Length"] = obj.vblob_file_size;
      header["Last-Modified"] = obj.vblob_update_time;
      header.ETag = '"'+obj.vblob_file_etag+'"';
      var keys = Object.keys(obj);
      for (var idx = 0; idx < keys.length; idx++) {
        var obj_key = keys[idx];
        if (obj_key.match(/^vblob_meta_/)) {
          var sub_key = obj_key.substr(11);
          sub_key = "x-amz-meta-" + sub_key;
          header[sub_key] = obj[obj_key];
        } else if (obj_key.match(/^vblob_/) === null) {
          //other standard attributes
          header[obj_key] = obj[obj_key];
        }
      }
      //override with response-xx
      keys = Object.keys(options);
      for (var idx2 = 0; idx2 < keys.length; idx2++) {
        var obj_key2 = keys[idx2];
        if (obj_key2.match(/^response-/)) {
          var sub_key2 = obj_key2.substr(9);
          header[sub_key2] = options[obj_key2];
        }
      }
      header["Accept-Ranges"] = "bytes";
      var st;
      if (range !== null && range !== undefined) {
        header["Content-Range"] = "bytes "+ (range.start!==undefined?range.start:"")+'-'+(range.end!==undefined?range.end.toString():"") + "/"+obj.vblob_file_size.toString();
        if (range.start === undefined) { range.start = obj.vblob_file_size - range.end; delete range.end; }
        if (range.end === undefined) { range.end = obj.vblob_file_size-1; }
        header["Content-Length"] = range.end - range.start + 1;
        //resp.writeHeader(206,header);
        resp_code = 206; resp_header = header;
        if (verb==="get") { //TODO: retry for range read?
          if (range.start < 0 || range.start > range.end ||
              range.start > obj.vblob_file_size-1 || range.end > obj.vblob_file_size-1)
          {
            error_msg(416,'InvalidRange','The requested range is not satisfiable',resp);
            callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
            return;
          }
          st = fs.createReadStream(c_path+"/"+obj.vblob_file_path, range);
          st.on('error', function(err) {
            st = null;
            error_msg(503,'SlowDown','The object is being updated too frequently, try later',resp);
            callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          });
          st.on('open', function(fd) {
            callback(resp_code, resp_header, null, st);
          });
        } else {
          if (range.start < 0 || range.start > range.end ||
              range.start > obj.vblob_file_size-1 || range.end > obj.vblob_file_size-1)
          {
            error_msg(416,'InvalidRange','The requested range is not satisfiable',resp);
            callback(resp.resp_code, resp.resp_header, null, null);
            return;
          }
          callback(resp_code, resp_header, null, null);
        }
      } else {
        resp_code = 200; resp_header = header;
        //resp.writeHeader(200,header);
        if (verb==="get") {
          st = fs.createReadStream(c_path+"/"+obj.vblob_file_path);
          st.on('error', function(err) {//RETRY??
            st = null;
            fb.logger.error( ("file "+obj.vblob_file_version+" is purged by gc already!"));
            //error_msg(508,'SlowDown','The object is being updated too frequently, try later',resp);
            //callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
            //suppress temporary failures from underlying storage
            if (!retry_cnt) retry_cnt = 0;
            if (retry_cnt < MAX_READ_RETRY) {
              delete options.seq_id;
              setTimeout(function(fb1) { fb1.file_read(container_name, filename, options, callback,fb1, retry_cnt+1); }, Math.floor(Math.random()*1000) + 100,fb);
              return;
            }
            error_msg(500,"InternalError",err,resp); callback(resp.resp_code, resp.resp_header, resp.resp_body, null); return;
          });
          st.on('open', function(fd) {
            callback(resp_code, resp_header, null, st);
          });
        }  else { callback(resp_code, resp_header, null, null);  }
      }
    });
  };//end of closure2
  if (options.seq_id) { seq_id = options.seq_id; closure2(); }
  else http.get("http://"+fb.meta_host+":"+fb.meta_port+"/"+container_name+"/"+filename, function (res) {
    if (res.statuCode == 404) { 
      error_msg(404,"NoSuchFile",err,resp); callback(resp.resp_code, resp.resp_header, resp.resp_body, null); return;
    } else {
      seq_id = res.headers["seq-id"];
      options.seq_id = seq_id;
      closure2();
    }
  }).on('error', function(err) {
    if (!fb.read_direct) {
      error_msg(500,"InternalError",err,resp);
      callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    } else {
      //read on-disk key folder directly
      fs.readdir(c_path + "/versions/" + key_fingerprint.substr(0,PREFIX_LENGTH)+"/"+key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2)+"/"+key_fingerprint+"/", function(err, files) {
        if (err) {
          if (err.code == 'ENOENT')
            error_msg(404,"NoSuchFile",err,resp);
          else error_msg(500,"InternalError",err,resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          return;
        }
        //find the max seq number, which should be the latest version
        var max_seq = null;
        for (var idx=0; idx<files.length;idx++) {
          var filename2 = files[idx];
          var epoch,cnt,seq_id2;
          cnt = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get cnt
          filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove cnt
          epoch = filename2.substr(filename2.lastIndexOf('-')+1,filename2.length); //get epoch
          filename2 = filename2.substr(0,filename2.lastIndexOf('-')); //remove epoch
          seq_id2 = epoch+"-"+cnt;
          if (!max_seq || seq_id_cmp(seq_id2, max_seq) > 0) max_seq = seq_id2;
        }
        if (!max_seq) {
          error_msg(404,"NoSuchFile","No such file.",resp);
          callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
          return;
        }
        //make sure this version is not left-over trash, check if blob file exists
        fs.stat(c_path + "/blob/" + key_fingerprint.substr(0,PREFIX_LENGTH)+"/"+key_fingerprint.substr(PREFIX_LENGTH,PREFIX_LENGTH2)+"/"+key_fingerprint+"-"+max_seq, function(err, stats) {
          if (err) {
            if (err.code == 'ENOENT')
              error_msg(404,"NoSuchFile",err,resp);
            else error_msg(500,"InternalError",err,resp);
            callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
            return;
          }
          seq_id = max_seq;
          options.seq_id = max_seq;
          closure2();
        });
      });
    }
  });
};

function query_files(container_name, options, callback, fb)
{
  var keys = null;
  keys = enum_cache[container_name].keys;
  if (!keys) {
    fb.logger.debug("sorting the file keys in container " + container_name);
    keys = Object.keys(enum_cache[container_name].tbl);
    keys = keys.sort();
    enum_cache[container_name].keys = keys;
  }
  var idx = 0;
  var low = 0, high = keys.length-1, mid;
  if (options.marker || options.prefix) {
    var st = options.marker;
    if (!st || st < options.prefix) st = options.prefix;
    while (low <= high) {
      mid = ((low + high) >> 1);
      if (keys[mid] === st) { low = mid; break; } else
      if (keys[mid] < st) low = mid + 1;
      else high = mid-1;
    }
    idx = low;
  }
  var idx2 = keys.length;
  if (options.prefix) { //end of prefix range
    var st2 = options.prefix;
    st2 = st2.substr(0,st2.length-1)+String.fromCharCode(st2.charCodeAt(st2.length-1)+1);
    low = idx; high = keys.length-1;
    while (low <= high) {
      mid = ((low + high) >> 1);
      if (keys[mid] === st2) { low = mid; break; } else
      if (keys[mid] < st2) low = mid + 1;
      else high = mid-1;
    }
    idx2 = low;
  }
  var limit1;
  try { limit1 = options["max-keys"] ? parseInt(options["max-keys"],10) : 1000; } catch (err) { limit1 = 1000; }
  var limit = limit1;
  if (limit > 1000) limit = 1000;
  var res_json = {};
  var res_contents = [];
  var res_common_prefixes = [];
  res_json["Name"] = container_name;
  res_json["Prefix"] = options.prefix ? options.prefix : {};
  res_json["Marker"] = options.marker ? options.marker : {};
  res_json["MaxKeys"] = ""+limit;
  if (options.delimiter) {
    res_json["Delimiter"] = options.delimiter;
  }
  var last_pref = null;
  for (var i = 0; i < limit && idx < idx2; ) {
    var key = keys[idx];
    idx++;
    if (options.delimiter) {
      var start = 0;
      if (options.prefix) start = options.prefix.length;
      var pos = key.indexOf(options.delimiter,start);
      if (pos >= 0) { //grouping together [prefix] .. delimiter
        var pref = key.substring(0, pos+1);
        if (pref === last_pref) continue;
        last_pref = pref;
        res_common_prefixes.push({"Prefix":pref});
        i++; continue;
      }
    }
    var doc = enum_cache[container_name].tbl[key][0]; //no search for versions yet
    if (doc.etag) { //in case this is a delete marker
      i++;
      res_contents.push({"Key":key, "LastModified":new Date(doc.lastmodified).toISOString(), "ETag":'"'+doc.etag+'"', "Size":doc.size, "Owner":{}, "StorageClass":"STANDARD"});
    }
  }
  if (i >= limit && idx < idx2 && limit <= limit1) res_json["IsTruncated"] = 'true';
  else res_json["IsTruncated"] = 'false';
  if (res_contents.length > 0) res_json["Contents"] =  res_contents; //files
  if (res_common_prefixes.length > 0) res_json["CommonPrefixes"] = res_common_prefixes; //folders
  var resp = {};
  resp.resp_code = 200; resp.resp_header = common_header(); resp.resp_body = {"ListBucketResult":res_json};
  res_json = null; res_contents = null; keys = null; res_common_prefixes = null;
  callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
}

FS_blob.prototype.file_list = function(container_name, options, callback, fb)
{
  if (options.delimiter && options.delimiter.length > 1) {
    var resp = {};
    error_msg(400,"InvalidArgument","Delimiter should be a single character",resp);
    callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
    return;
  }
  var c_path = this.root_path + "/" + container_name;
  if (container_exists(container_name,callback,this) === false) return;
  var now = new Date().valueOf();
  if (!enum_cache[container_name] || !enum_expire[container_name] || enum_expire[container_name] < now) {
    if (!enum_queue[container_name]) enum_queue[container_name]={state:'READY',queue:[]};
    if (enum_queue[container_name].state == 'INPROGRESS') {
      enum_queue[container_name].queue.push({cn:container_name, op:options, cb:callback}); //defer
      return;
    }
    enum_queue[container_name].state='INPROGRESS';
    var enum_raw = '{}';
    try {
      enum_raw = fs.readFileSync(fb.root_path+"/"+container_name+"/"+ENUM_FOLDER+"/base");
    } catch (e) {}
    zlib.unzip(enum_raw,function(err,buffer) {
      try {
        if (err) {if (enum_raw != '{}') throw err; else buffer = enum_raw; }
        enum_queue[container_name].state='READY';
        enum_cache[container_name] = null;
	enum_cache[container_name] = {tbl:JSON.parse(buffer)};
	enum_expire[container_name] = now + 1000 * 30;
	query_files(container_name, options,callback,fb);
        for (var idx=0; idx < enum_queue[container_name].queue.length; idx++) {
          process.nextTick(function () {
            if (enum_queue[container_name].queue.length > 0) {
              var obj=enum_queue[container_name].queue.shift();
              try {
                query_files(obj.cn,obj.op,obj.cb,fb);
              } catch (e) {
	        var resp = {};
	        error_msg(500,'InternalError',e,resp);
	        obj.cb(resp.resp_code, resp.resp_header, resp.resp_body, null);
              }
            }
          });
        }
        enum_raw = null;
      } catch (e) {
        enum_raw = null;
        enum_queue[container_name].state='READY';
        enum_cache[container_name] = null;
	var resp = {};
	error_msg(500,'InternalError',e,resp);
	callback(resp.resp_code, resp.resp_header, resp.resp_body, null);
        for (var idx=0; idx < enum_queue[container_name].queue.length; idx++) {
          process.nextTick(function () {
            if (enum_queue[container_name].queue.length > 0) {
              var obj=enum_queue[container_name].queue.shift();
              try {
                query_files(obj.cn,obj.op,obj.cb,fb);
              } catch (e) {
	        var resp = {};
	        error_msg(500,'InternalError',e,resp);
	        obj.cb(resp.resp_code, resp.resp_header, resp.resp_body, null);
              }
            }
          });
        }
      }
      buffer = null;
    }); // end of unzip
  } else query_files(container_name, options,callback,fb);
}

FS_blob.prototype.container_list = function()
{
  return  fs.readdirSync(this.root_path);
};

function render_containers(dirs,callback,fb)
{
  var resp_code, resp_header, resp_body;
  resp_code = resp_header = resp_body = null;
  var dates = new Array(dirs.length);
  var evt = new events.EventEmitter();
  var counter = dirs.length;
  evt.on("Get Date",function (dir_name, idx) {
    fs.stat(fb.root_path+"/"+dir_name+"/ts", function(err,stats) {
        if (err) dates[idx] = null; else
        dates[idx] = stats.ctime;
        counter--; if (counter === 0) { evt.emit("Start Render"); }
    });
  });
  evt.on("Start Render", function () {
    resp_code = 200;
    resp_header = common_header();
    resp_body = {ListAllMyBucketsResult : {Buckets: {Bucket: []}}};
    for (var i = 0; i < dirs.length; i++) {
      if (dates[i] === null)  { continue; }
      resp_body.ListAllMyBucketsResult.Buckets.Bucket.push({Name:dirs[i],CreationDate:new Date(dates[i]).toISOString()});
    }
    callback(resp_code, resp_header, resp_body, null);
  });
  if (dirs.length === 0) { evt.emit("Start Render"); }
  for (var i = 0; i < dirs.length; i++)
  { evt.emit("Get Date",dirs[i],i); }
}

//=======================================================
//this is interface file for abstraction
var FS_Driver = function(option,callback) {
  var this1 = this;
  this1.root_path = option.root;
  var client = new FS_blob(option, function(obj,err) {
    if (err) {this1.fs_err = err; this1.client = null; if (callback) {callback(this1);} return; }
    this1.client = obj;
    if (callback) { callback(this1); }
  });
};

function check_client(client,callback)
{
  if (client) return true;
  var resp_header = common_header();
  var resp_code = 500;
  var resp_body = {Code:500,Message:"fs root not mounted" };
  callback(resp_code, resp_header, resp_body, null);
  return false;
}

FS_Driver.prototype.container_list = function (callback) {
  if (check_client(this.client,callback) === false) return;
  var dirs = this.client.container_list();
  render_containers(dirs,callback,this.client);
};

FS_Driver.prototype.file_list = function(container_name,option,callback) {
  if (check_client(this.client,callback) === false) return;
  this.client.file_list(container_name,option, callback, this.client);
};

FS_Driver.prototype.file_read = function(container_name,file_key,options,callback){
  if (check_client(this.client,callback) === false) return;
  var range1 = null;
  if (options.range) {
    range1 = options.range;
    range1 = range1.substr(6);
    var m = range1.match(/^([0-9]*)-([0-9]*)$/);
    if (m[1]===m[2]&& m[1]==='') { range1=null; }
    else {
      range1 = {};
      if (m[1] !== '') { range1.start = parseInt(m[1],10); }
      if (m[2] !== '') { range1.end = parseInt(m[2],10); }
    }
    this.client.logger.debug( ("Final range: "+util.inspect(range1)));
    options.range = range1;
  }
  this.client.file_read(container_name, file_key, options, callback, this.client);
};

FS_Driver.prototype.file_create = function(container_name,file_key,options, metadata, data_stream,callback) {
  if (check_client(this.client,callback) === false) return;
  this.client.file_create(container_name,file_key,options,metadata, data_stream, callback,this.client);
};

FS_Driver.prototype.file_copy = function(container_name, file_key, source_container,source_file_key,options, metadata, callback)
{
  if (check_client(this.client,callback) === false) return;
  this.client.file_copy(container_name,file_key,source_container,source_file_key,options, metadata, callback,this.client);
};

FS_Driver.prototype.container_create = function(container_name,options,data_stream,callback) {
  if (check_client(this.client,callback) === false) return;
  this.client.container_create(container_name,callback,this.client);
};

FS_Driver.prototype.file_delete = function(container_name,file_key,callback) {
  if (check_client(this.client,callback) === false) return;
  this.client.file_delete_meta(container_name,file_key,callback,this.client);
};

FS_Driver.prototype.container_delete = function(container_name,callback) {
  if (check_client(this.client,callback) === false) return;
  this.client.container_delete(container_name,callback,this.client);
};

FS_Driver.prototype.get_config = function() {
  var obj = {}; var obj2 = {};
  obj.type = "fs";
  obj2.root= this.client.root_path;
  obj2.node_exepath = this.client.node_exepath;
  obj2.gcfc_exepath = this.client.gcfc_exepath;
  obj2.gcfc_interval = this.client.gcfc_interval;
  obj2.gctmp_exepath = this.client.gctmp_exepath;
  obj2.gctmp_interval = this.client.gctmp_interval;
  obj2.ec_exepath = this.client.ec_exepath;
  obj2.ec_interval = this.client.ec_interval;
  obj2.collector = this.client.collector;
  obj2.quota = this.client.quota;
  obj2.obj_limit = this.client.obj_limit;
  obj2.seq_host = this.client.seq_host;
  obj2.seq_port = this.client.seq_port;
  obj2.meta_host = this.client.meta_host;
  obj2.meta_port = this.client.meta_port;
  obj.option = obj2;
  return obj;
};

module.exports.createDriver = function(option,callback) {
  return new FS_Driver(option, callback);
};
