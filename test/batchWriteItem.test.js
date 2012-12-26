/*
 * batchWriteItem.test.js
 *
 * (C) 2012 Crosstalk Systems Inc.
 */
"use strict";

var CONFIG = require( './private.config.json' );

var ide = require( 'crosstalk-ide' )(),
    workerPath = require.resolve( '../index' );

var worker;

worker = ide.run( workerPath, {} );

var requestItems = {};
requestItems[ CONFIG.testTable ] = [{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.0"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.1"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.2"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.3"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.4"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.5"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.6"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.7"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.8"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.9"
    }
  }
},{
  putRequest : {
    item : {
      hash : "first hash",
      range : "0.0.10"
    }
  }
}, {
  putRequest : {
    item : {
      hash : "second hash",
      range : "0.0.0"
    }
  }
}];

var validBatchWriteItem = {
  awsAccessKeyId : CONFIG.awsAccessKeyId,
  region : CONFIG.region,
  requestItems : requestItems,
  secretAccessKey : CONFIG.secretAccessKey
};

worker.dontMockHttps = true;

worker.proxy = "~crosstalk.api.aws.signature.version4";
worker.crosstalkToken = CONFIG.crosstalkToken;

worker.send( 'api.aws.dynamodb.batchWriteItem@v1', validBatchWriteItem, 
   'public', true );