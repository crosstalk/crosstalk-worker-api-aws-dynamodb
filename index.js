/*
 * index.js: Crosstalk worker wrapping AWS DynamoDB API
 *
 * (C) 2012 Crosstalk Systems Inc.
 */

var async = require( 'async' ),
    https = require( 'https' ),
    logger = require( 'logger' );

var CONTENT_TYPE = "application/x-amz-json-1.0",
    HOST_PREFIX = "dynamodb.",
    HOST_POSTFIX = ".amazonaws.com",
    HTTP_REQUEST_METHOD = "POST",
    REQUEST_URI = "/",
    SERVICE = "dynamodb",
    TARGET_PREFIX = "DynamoDB_20111205.";

var attachSignatureToRequest = function attachSignatureToRequest ( dataBag, callback ) {

  dataBag.requestHeaders[ 'Authorization' ] = 
     dataBag.requestSignature.authorization;
  dataBag.requestHeaders[ 'X-Amz-Date' ] = dataBag.requestSignature.date;

  return callback( null, dataBag );

}; // attachSignatureToRequest

var batchWriteItem = function batchWriteItem ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      region = params.region,
      requestItems = params.requestItems,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } ); 
  if ( ! region ) return callback( { message : "missing region" } );
  if ( ! requestItems ) return callback( { message : "missing requestItems" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  var request = {
    RequestItems : {}
  };

  Object.keys( requestItems ).forEach( function ( tableName ) {

    request.RequestItems[ tableName ] = [];

    requestItems[ tableName ].forEach( function ( _request ) {

      if ( _request.putRequest ) {

        request.RequestItems[ tableName ].push( {
          PutRequest : {
            Item : toAwsTypedJson( _request.putRequest.item ) 
          }
        });

      } else if ( _request.deleteRequest ) {

        var key = {};

        key.HashKeyElement = _request.deleteRequest.key.hashKeyElement;
        if ( _request.deleteRequest.key.rangeKeyElement ) {
          key.RangeKeyElement = _request.deleteRequest.key.rangeKeyElement;
        }

        request.RequestItems[ tableName ].push( {
          DeleteRequest : {
            Key : toAwsTypedJson( key )
          }
        });

      } // else if ( _request.deleteRequest )

    }); // requesItems[ tableName ].forEach

  }); // Object.keys( requestItems ).forEach

  var requestHeaders = constructRequestHeaders( "BatchWriteItem", region );

  var body = JSON.stringify( request );

  logger.log( body );

  requestHeaders[ 'content-length' ] = Buffer.byteLength( body );

  return executeAction({
    actionType : "batch",
    awsAccessKeyId : awsAccessKeyId,
    body : body,
    queryString : "",
    region : region,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // batchWriteItem

var constructRequestHeaders = function constructRequestHeaders ( action, region ) {

  return {
    "host" : HOST_PREFIX + region + HOST_POSTFIX,
    "content-type" : CONTENT_TYPE,
    "x-amz-target" : TARGET_PREFIX + action
  };

}; // constructRequestHeaders

var deleteItem = function deleteItem ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional
  
  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      key = params.key,
      region = params.region,
      secretAccessKey = params.secretAccessKey,
      tableName = params.tableName;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } ); 
  if ( ! key ) return callback( { message : "missing key" } );
  if ( ! key.hashKeyElement ) return callback( { message : "missing key.hashKeyElement" } );
  if ( ! region ) return callback( { message : "missing region" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );
  if ( ! tableName ) return callback( { message : "missing tableName" } ); 

  var request = {
    Key : {
      HashKeyElement : toAwsTypedJson( key.hashKeyElement )
    },
    TableName : tableName
  };

  if ( key.rangeKeyElement ) {
    request.Key.RangeKeyElement = toAwsTypedJson( key.rangeKeyElement );
  }

  //
  // optional params
  //
  var expected = params.expected,
      returnValues = params.returnValues;

  if ( expected ) {

    Object.keys( expected ).forEach( function ( key ) {

      var attribute = expected[ key ];

      Object.keys( attribute ).forEach( function ( key ) {

        if ( key.toLowerCase() == "value" ) {
          attribute[ key ] = toAwsTypedJson( attribute[ key ] );
        }

      }); // Object.keys( attribute ).forEach

    }); // Object.keys( expected ).forEach

    request[ 'Expected' ] = expected;

  } // if ( expected )
  if ( returnValues ) request[ 'ReturnValues' ] = returnValues;

  var requestHeaders = constructRequestHeaders( "DeleteItem", region );

  var body = JSON.stringify( request );

  requestHeaders[ 'content-length' ] = Buffer.byteLength( body );

  return executeAction( {
    awsAccessKeyId : awsAccessKeyId,
    body : body,
    queryString : "",
    region : region,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );  

}; // deleteItem

var executeAction = function executeAction ( dataBag, callback ) {

  async.waterfall([

    // bootstrap dataBag
    function ( _callback ) {
      return _callback( null, dataBag );
    }, // bootstrap dataBag

    getRequestSignature,

    attachSignatureToRequest,

    makeRequest,

    parseResponse

  ], function ( error, result ) {

    if ( error ) { return callback( error ); }

    return callback( null, result );

  }); // async.waterfall

}; // executeAction

var fromAwsTypedJson = function fromAwsTypedJson( value ) {

  if ( typeof( value ) === "object" ) {

    if ( value.B ) return value.B;
    if ( value.N ) return value.N;
    if ( value.S ) return value.S;
    if ( value.BB ) return value.BB;
    if ( value.NN ) return value.NN;
    if ( value.SS ) return value.SS;

    Object.keys( value ).forEach( function ( key ) {
      value[ key ] = fromAwsTypedJson( value[ key ] );
    });

  } // if ( typeof( value ) === "object" )

  return value;

}; // fromAwsTypedJson

var getItem = function getItem ( params, callback ) {

  if ( ! callback ) { return; } // nothing to do

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      key = params.key,
      region = params.region,
      secretAccessKey = params.secretAccessKey,
      tableName = params.tableName;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } ); 
  if ( ! key ) return callback( { message : "missing key" } );
  if ( ! key.hashKeyElement ) return callback( { message : "missing key.hashKeyElement" } );
  if ( ! region ) return callback( { message : "missing region" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );
  if ( ! tableName ) return callback( { message : "missing tableName" } ); 

  var request = {
    Key : {
      HashKeyElement : toAwsTypedJson( key.hashKeyElement )
    },
    TableName : tableName
  };

  if ( key.rangeKeyElement ) {
    request.Key.RangeKeyElement = toAwsTypedJson( key.rangeKeyElement );
  }

  //
  // optional params
  //
  var attributesToGet = params.attributesToGet,
      consistentRead = params.consistentRead;

  if ( attributesToGet ) request[ 'AttributesToGet' ] = attributesToGet;
  if ( consistentRead ) request[ 'ConsistentRead' ] = consistentRead;

  var requestHeaders = constructRequestHeaders( "GetItem", region );

  var body = JSON.stringify( request );

  requestHeaders[ 'content-length' ] = Buffer.byteLength( body );

  return executeAction( {
    awsAccessKeyId : awsAccessKeyId,
    body : body,
    queryString : "",
    region : region,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // getItem

var getRequestSignature = function getRequestSignature ( dataBag, callback ) {

  crosstalk.emit( '~crosstalk.api.aws.signature.version4', {
    awsAccessKeyId : dataBag.awsAccessKeyId,
    body : dataBag.body,
    headers : dataBag.requestHeaders,
    httpRequestMethod : HTTP_REQUEST_METHOD,
    queryString : dataBag.queryString,
    region : dataBag.region,
    secretAccessKey : dataBag.secretAccessKey,
    service : SERVICE
  }, '~crosstalk', function ( error, response ) {

    if ( error ) { return callback( error ); }

    dataBag.requestSignature = response;
    return callback( null, dataBag );

  }); // crosstalk.emit ~crosstalk.api.aws.signature.version4

}; // getRequestSignature

var makeRequest = function makeRequest ( dataBag, callback ) {

  var queryString = dataBag.queryString;

  var requestOptions = {
    headers : dataBag.requestHeaders,
    host : dataBag.requestHeaders.host,
    method : HTTP_REQUEST_METHOD,
    path : REQUEST_URI + ( queryString ? "?" + queryString : "" )
  };

  var req = https.request( requestOptions );

  req.on( 'response', function ( response ) {

    var responseBody = "";

    response.setEncoding( "utf8" );
    response.on( "data", function ( chunk ) {
      responseBody += chunk;
    });

    response.on( "end", function () {

      dataBag.responseBody = responseBody;
      dataBag.statusCode = response.statusCode;

      return callback( null, dataBag );

    }); // response.on "end"

  }); // req.on 'response'

  req.on( 'error', function ( error ) {
    return callback( error );
  });

  req.write( dataBag.body );

  req.end();

}; // makeRequest

var parseResponse = function parseResponse ( dataBag, callback ) {

  var response;

  try {
    response = JSON.parse( dataBag.responseBody );
  } catch ( exception ) {
    return callback( exception );
  }

  var parsedResponse = {};

  Object.keys( response ).forEach( function ( key ) {

    var parsedKey = key[ 0 ].toLowerCase() + key.slice( 1 );
    parsedResponse[ parsedKey ] = fromAwsTypedJson( response[ key ] );

    if ( dataBag.actionType === 'batch' ) {

      if ( parsedKey === 'responses' ) {

        // process metadata for each table
        Object.keys( parsedResponse.responses ).forEach( function ( table ) {

          var consumedCapacityUnits = 
             parsedResponse.responses[ table ].ConsumedCapacityUnits;
          delete parsedResponse.responses[ table ];
          parsedResponse.responses[ table ] = {
            consumedCapacityUnits : consumedCapacityUnits
          };

        }); // Object.keys( parsedResponse.responses )

      } // if ( parsedKey === 'responses' )

      if ( parsedKey === 'unprocessedItems' ) {

        var unprocessedItems = {};

        // process metadata for each table
        Object.keys( parsedResponse.unprocessedItems ).forEach( 
           function ( tableKey ) {

          var table = parsedResponse.unprocessedItems[ tableKey ];

          unprocessedItems[ tableKey ] = [];

          table.forEach( function ( unprocessedRequest ) {

            var request = {};

            if ( unprocessedRequest.PutRequest ) {

              request.putRequest = {
                item : fromAwsTypedJson( unprocessedRequest.PutRequest.Item )
              };

            } else if ( unprocessedRequest.DeleteRequest ) {

              var key = {};

              key.hashKeyElement = fromAwsTypedJson( unprocessedRequest
                 .DeleteRequest.Key.HashKeyElement );

              if ( unprocessedRequest.DeleteRequest.Key.RangeKeyElement ) {
                
                key.rangeKeyElement = fromAwsTypedJson( unprocessedRequest
                   .DeleteRequest.Key.RangeKeyElement );

              } // if ( unprocessedRequest.DeleteRequest.Key.RangeKeyElement )

              request.deleteRequest = { key : key };

            } // else if ( unprocessedRequest.DeleteRequest )

            unprocessedItems[ tableKey ].push( request );

          }); // table.forEach

        }); // Object.keys( parsedResponse.unprocessedItems )

        delete parsedResponse.unprocessedItems;
        parsedResponse.unprocessedItems = unprocessedItems;

      } // if ( parsedKey === 'unprocessedItems' )

    } // if ( dataBag.actionType === 'batch' )

  }); // Object.keys( response ).forEach

  if ( dataBag.statusCode > 299 ) {
    return callback( parsedResponse );
  }

  return callback( null, parsedResponse );

}; // parseResponse

var putItem = function putItem ( params, callback ) {
  
  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      item = params.item,
      region = params.region,
      secretAccessKey = params.secretAccessKey,
      tableName = params.tableName;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! item ) return callback( { message : "missing item" } );
  if ( ! region ) return callback( { message : "missing region" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );
  if ( ! tableName ) return callback( { message : "missing tableName" } );

  var awsJsonItem = toAwsTypedJson( item );

  var request = {
    Item : awsJsonItem,
    TableName : tableName
  };

  //
  // optional params
  //
  var expected = params.expected,
      returnValues = params.returnValues;

  if ( expected ) {

    Object.keys( expected ).forEach( function ( key ) {

      var attribute = expected[ key ];

      Object.keys( attribute ).forEach( function ( key ) {

        if ( key.toLowerCase() == "value" ) {
          attribute[ key ] = toAwsTypedJson( attribute[ key ] );
        }

      }); // Object.keys( attribute ).forEach

    }); // Object.keys( expected ).forEach

    request[ 'Expected' ] = expected;

  } // if ( expected )
  if ( returnValues ) request[ 'ReturnValues' ] = returnValues;

  var requestHeaders = constructRequestHeaders( "PutItem", region );  

  var body = JSON.stringify( request );

  requestHeaders[ 'content-length' ] = Buffer.byteLength( body );

  return executeAction( {
    awsAccessKeyId : awsAccessKeyId,
    body : body,
    queryString : "",
    region : region,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // putItem

var query = function query ( params, callback ) {

  if ( ! callback ) { return; } // nothing to do
  
  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      hashKeyValue = params.hashKeyValue,
      region = params.region,
      secretAccessKey = params.secretAccessKey,
      tableName = params.tableName;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! hashKeyValue ) return callback( { message : "missing hashKeyValue" } );
  if ( ! region ) return callback( { message : "missing region" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );
  if ( ! tableName ) return callback( { message : "missing tableName" } );

  var request = {
    HashKeyValue : toAwsTypedJson( hashKeyValue ),
    TableName : tableName
  };

  //
  // optional params
  //
  var attributesToGet = params.attributesToGet,
      consistentRead = params.consistentRead,
      count = params.count,
      exclusiveStartKey = params.exclusiveStartKey,
      limit = params.limit,
      rangeKeyCondition = params.rangeKeyCondition,
      scanIndexForward = params.scanIndexForward;

  if ( attributesToGet ) request[ 'AttributesToGet' ] = attributesToGet;
  if ( consistentRead ) request[ 'ConsistentRead' ] = consistentRead;
  if ( count ) request[ 'Count' ] = count;
  if ( exclusiveStartKey ) {
    
    request[ 'ExclusiveStartKey' ] = {
      HashKeyElement : toAwsTypedJson( exclusiveStartKey.hashKeyElement )
    };

    if ( exclusiveStartKey.rangeKeyElement ) {

      request[ 'ExclusiveStartKey' ].RangeKeyElement = 
         toAwsTypedJson( exclusiveStartKey.rangeKeyElement );

    } // if ( exclusiveStartKey.rangeKeyElement )

  } // if ( exclusiveStartKey )
  if ( limit ) request[ 'Limit' ] = limit;
  if ( rangeKeyCondition ) {

    request[ 'RangeKeyCondition' ] = {
      ComparisonOperator : rangeKeyCondition.comparisonOperator
    };
    
    if ( rangeKeyCondition.attributeValueList ) {

      request[ 'RangeKeyCondition' ].AttributeValueList = [];

      rangeKeyCondition.attributeValueList.forEach( function ( value ) {

        request[ 'RangeKeyCondition' ].AttributeValueList
           .push( toAwsTypedJson( value ) );

      }); // rangeKeyCondition.attributeValueList.forEach

    } // if ( rangeKeyCondition.attributeValueList )

  } // if ( rangeKeyCondition )
  if ( scanIndexForward ) request[ 'ScanIndexForward' ] = scanIndexForward;

  var requestHeaders = constructRequestHeaders( "Query", region );

  var body = JSON.stringify( request );

  requestHeaders[ 'content-length' ] = Buffer.byteLength( body );

  return executeAction( {
    awsAccessKeyId : awsAccessKeyId,
    body : body,
    queryString : "",
    region : region,
    requestHeaders : requestHeaders,
    secretAccessKey : secretAccessKey
  }, callback );

}; // query

//
// binary values not supported
//
var toAwsTypedJson = function toAwsTypedJson( value ) {

  if ( typeof( value ) === "number" ) {
    return { "N" : value };
  } else if ( typeof( value ) === "string" ) {
    return { "S" : value };
  } else if ( Array.isArray( value ) && typeof( value[ 0 ] ) === "number" ) {
    return { "NN" : value };
  } else if ( Array.isArray( value ) && typeof( value[ 0 ] ) === "string" ) {
    return { "SS" : value };
  } else if ( typeof( value ) === "object" ) {

    Object.keys( value ).forEach( function ( key ) {

      var val = value[ key ];
      value[ key ] = toAwsTypedJson( val );

    }); // Object.keys( obj ).forEach

  } // else if ( typeof( value ) === "object" )

  return value;

}; // toAwsTypedJson

crosstalk.on( 'api.aws.dynamodb.batchWriteItem@v1', 'public', batchWriteItem );
crosstalk.on( 'api.aws.dynamodb.deleteItem@v1', 'public', deleteItem );
crosstalk.on( 'api.aws.dynamodb.getItem@v1', 'public', getItem );
crosstalk.on( 'api.aws.dynamodb.putItem@v1', 'public', putItem );
crosstalk.on( 'api.aws.dynamodb.query@v1', 'public', query );