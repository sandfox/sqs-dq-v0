/**

 1. connect to SQS
 2. poll for X messages
 3. recieve Z messages
 4. if Z < X, poll again for (X-Z) messages
    if Z = X, stop polling
 5. put Z messages into processing queue
 6. 

 99. every T1 milliseconds check processing stack, find messages about to reach end of visibility window, (batch) renew for F seconds
 98. every T2 milliseconds check delete queue and batch remove messages

 congfigure with queue size and name
 + emitter / callback for job recieved
    function(job, finishedFunc)
    job is actual job
    finishedFunc takes an error object or nothing
 + error emitter

  TODO: replace reap with async cargo worker
  TODO: Logging and error `§emitting
  TODO: 
*/
var util = require("util");
var events = require("events");
var aws = require('aws-sdk');
var async = require('async');
var _ = require('lodash');
var debug = require('debug')('sqs-dq-v0');

var messageStates = [
  "recieved",
  "awaitingDeletion",
  "inDeletion"
]

var maxReapPerBatch = 10;


var Processor = module.exports = function(queueUrl, opts, worker) {

  opts = opts || {}

  this._queueUrl = queueUrl;
  this._sqs = new aws.SQS(opts.aws);
  this._q = null;
  // should be a getter for _q.concurrency
  // never > 10 as that is max batch size for SQS message fetch
  this.concurrency = opts.concurrency || 10; 
  //collection of messages we are holding onto
  this._messages = [];
  this._shouldFetchTasks = true;
  this._worker = worker;

}

util.inherits(Processor, events.EventEmitter);

Processor.prototype.start = function() {
  
  var that = this;

  that._q = async.queue(this._worker, that.concurrency)

  //loop for fetching
  async.whilst(
    function(){return that._shouldFetchTasks},
    function(cb){
      //get concurrency - num messages being processed
      var maxRecords = that.concurrency - that._q.running();
      if(maxRecords > 0) {
        that._fetch(maxRecords, cb)
      } else {
        setTimeout(cb, 1000)
      }
    },
    function(err){
      //log that we are stopping checking for messages
      if(err){
        debug("STOPPED FETCHING DUE TO ERROR")
        debug(err.name, err.message);
        that.emit('reapError', err);
        return;
      }

      debug("STOPPED FETCHING");
    }

    )

  //loop for reaping
  async.until(
    function(){return (that._shouldFetchTasks === false && that._q.idle() && that._messages.length == 0)},
    function(cb) {
      that._reap(cb)
    },
    function(err){
      //log that we are stopping reaping messages
      if(err){
        debug("STOPPED REAPING DUE TO ERROR")
        debug(err.name, err.message);
        that.emit('fetchError', err);
        return;
      }

      debug("STOPPED REAPING");
    }
    )

  // TODO: loop for refreshing visbility window


};

//TODO: doesn't really stop anything
Processor.prototype.stop = function() {
  this._shouldFetchTasks = false
};

Processor.prototype._reap = function(cb){

  var that = this;

  var reapList = [];

  this._messages.forEach(function(message){
    if( message.state == 'awaitingDeletion' && reapList.length < maxReapPerBatch){
      message.state = 'inDeletion';
      reapList.push({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      })
    }
  })

  if(reapList.length > 0) {
    this._sqs.deleteMessageBatch({
      Entries: reapList,
      QueueUrl: that._queueUrl
    }, function(err, data){
      if(err){
        // should probably log this somewhere
        debug("MESSAGE DELETION ERROR");
        debug(err.name, err.message);
        that.emit('deletionError', err);
        return cb();
      }
      // scan each message we have, if it's id is in the list of successfully
      // deleted message, remove it
      _.remove(that._messages, function(message){
        return _.find(data.Successful, {Id : message.MessageId})
      })
      // should mark as fucked/retry the failed ones...
      return cb();
    })
  }
  //if reap list < 0 block for a second then return
  setTimeout(cb, 1000)
}


//Fetches records, puts them somewhere, returns
Processor.prototype._fetch = function(numRecords, cb) {
  
  var that = this;

  var params = {
    QueueUrl: that._queueUrl,
    AttributeNames: [],
    MaxNumberOfMessages: numRecords
    //MessageAttributeNames: ['HttpRequestId'],
    //WaitTimeSeconds: 20,
    //VisibilityTimeout: 2
  }

  this._sqs.receiveMessage(params, function(err, data){

    //What should we do on poll error?
    if (err) {
      debug(err.name, err.message);
      return cb();
    }

    if(data.Messages) {
      data.Messages.forEach(function(message){
  
        //bolt on meta-data
        message.recievedAt = Date.now();
        message.state = "recieved"

        // put into collection of messages we hold
        that._messages.push(message)
        // add into async.q, and upon finishing proccessing, setState to awaiting deletion
        that._q.push(message.Body, function(){
          debug("MARKING MESSAGE FOR REAPING:", message.MessageId)
          message.state = "awaitingDeletion"
        })
      })
    }
    cb();
  })
};

