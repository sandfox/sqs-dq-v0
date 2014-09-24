# SQS-DQ-v0

A thing for dequeue-ing messages from AWS SQS and giving those messages to a worker function

_REALLY ALPHA QUALITY_ - API may change

Doesn't:
- change message visbility window if worker holds onto message for too long
- handle more than one queue
- shutdown very gracefully
- have any tests yet (verified working only via some hacky integration tests on my laptop)

Does:
- Expect SNS delivered messages to be in RAW format
- probably have bugs + race conditions
- batch up fetching and deleting messages
- need more internal operations visibilty

Other Problems:
- Concurrency is limited to 10... (because coupling between max SQS batch size and max concurrent workers) (fixable)

## Usage


```js
var DeQueue = require('sqs-dq-v0');

var opts = {
  "aws": {
    "accessKeyId": "XXXXXXXXXXXXXXXXXXX",
    "secretAccessKey": "YYYYYYYYYYYYYYYYYYYYYYYYY",
    "region": "eu-west-1"
  }
  "concurrency": 10 //optional
};

var myWorker = function(task, cb){
  console.log('GOT DIS:', task);
  setTimeout(cb, 500);
}

var datQueueProcessor = new DeQueue(
  "https://sqs.eu-west-1.amazonaws.com/312010819855/my-queue",
  opts,
  myWorker
  );




datQueueProcessor.start(myWorker);

```
