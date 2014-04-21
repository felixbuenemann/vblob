/*
Copyright (c) 2011-2012 VMware, Inc.

  Bucket in subdomain test for basic vblob features: 
  - get container
  - put file
  - get file
  - delete file
  Also creates and deletes test container through the host without bucket in subdomain to allow testing.
  Start a vblob gw instance WITHOUT "auth" : "enabled" at the end of the config file. This will allow anonymous access to apis. Then go ahead to test the above features in fs.

  Put it in another way:
  client -> gw 1 without auth -> fs driver
*/
var util = require('util');
var vows = require('vows');
var assert = require('assert');
var fs = require('fs');
var config = JSON.parse(require('./utils').execSync("curl http://localhost:9981/~config")); //must be the config you actually use for the vblob  instance
var test_date = new Date().valueOf();
var container_name = '/sonic-test'+test_date;
var host = container_name.substr(1) + '.' + config.host;
console.log(config);
var suite = vows.describe('testsubdomain: using container '+container_name+' against driver '+config['current_driver']+' on '+host+':'+config.port);
var parse_xml = require('./utils').parse_xml;
var assertStatus = require('./utils').assertStatus;
var api = require('./utils').api;

suite.addBatch({
  'PUT container (without subdomain)' : {
    topic: api.put(container_name),
    'should respond with a 200 OK':  assertStatus(200),
    'should respond with the location of the container': function (err,res) {
      assert.isString(res.headers['location']);
    } 
  }
}).addBatch({
  'PUT testbasic-1.txt': {
    topic: api.put_data('/testbasic-1.txt','./file1.txt',{'host': host}),
    'should respond with a 200 OK':  assertStatus(200),
    'should respond with the md5 hex of the file': function (err,res) {
      assert.isString(res.headers.etag);
    } 
  },
  'PUT A/B.txt': {
    topic: api.put_data('/A/B.txt','./file1.txt',{'host': host}),
    'should respond with a 200 OK':  assertStatus(200),
    'should respond with the md5 hex of the file': function (err,res) {
      assert.isString(res.headers.etag);
    } 
  },
  'PUT A/B/C.txt': {
    topic: api.put_data('/A/B/C.txt','./file1.txt',{'host': host}),
    'should respond with a 200 OK':  assertStatus(200),
    'should respond with the md5 hex of the file': function (err,res) {
      assert.isString(res.headers.etag);
    } 
  }
}).addBatch({
  'GET /': {
    topic: api.get('/',{'host': host}),
    'should respond with either 501 or 200 code':  function (err,res) {
      assert.include([200,501],res.statusCode);
    },
    'should respond with either a valid list or a valid error': function (err,res) {
      assert.isNotNull(res.resp_body);
      if (res.statusCode === 200) {
        assert.isNotNull(res.resp_body.ListBucketResult);
      } else {
        assert.isObject(res.resp_body.Error);
      }
    } 
  },
  'GET ?prefix=/&delimiter=/': {
    topic: api.get('/?prefix=/&delimiter=/',{'host': host}),
    'should respond with either 501 or 200 code':  function (err,res) {
      assert.include([200,501],res.statusCode);
    },
    'should respond with a valid list of prefixes or a valid error': function (err,res) {
      assert.isNotNull(res.resp_body);
      if (res.statusCode === 200) {
        assert.isNotNull(res.resp_body);
      } else {
        assert.isNotObject(res.resp_body.Error);
      }
    } 
  }
}).addBatch({
  'GET testbasic-1.txt': {
    topic: api.get_data('/testbasic-1.txt',{'host': host}),
    'should respond with a 200 OK':  assertStatus(200),
    'should respond with the content of the file': function (err,res) {
      var str1 = fs.readFileSync('./file1.txt');
      assert.equal(str1,res.resp_body);
    } 
  }
}).addBatch({
  'DELETE testbasic-1.txt' : {
    topic: api.del('/testbasic-1.txt',{'host': host}),
    'should respond with a 204 OK':  assertStatus(204)
  },
  'DELETE A/B.txt' : {
    topic: api.del('/A/B.txt',{'host': host}),
    'should respond with a 204 OK':  assertStatus(204)
  },
  'DELETE A/B/C.txt' : {
    topic: api.del('/A/B/C.txt',{'host': host}),
    'should respond with a 204 OK':  assertStatus(204)
  }
}).addBatch({
  'DELETE container (without subdomain)' : {
    topic: api.del(container_name),
    'should respond with a 204 OK':  assertStatus(204)
  }
});
suite.export(module);
