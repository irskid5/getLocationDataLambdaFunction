const AWS = require('aws-sdk');
const uuid = require('uuid');

AWS.config.update({
  region: "us-east-2",
});

const ddb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
const s3 = new AWS.S3({apiVersion: '2006-03-01', signatureVersion: 'v4'});

console.log("Querying for coordinates");

exports.handler = async (event) => {

  var tsToday = new Date(new Date().getDate()).getTime();
  var params = {
    TableName: "vele_thesis_location_deviceID_timestamp_v2",
    ExpressionAttributeNames: {
      "#timestamp": "timestamp"
    },
    ExpressionAttributeValues: {
      ":ts": 
        { 
	  N: tsToday.toString() 
	}
    },
    FilterExpression: "#timestamp >= :ts", 
  }
  
  var data;
  try {
    data = await ddb.scan(params).promise();
    //console.log(data);
  } catch (err) {
    console.log(err);
    const response = {
      statusCode: 500,
      body: JSON.stringify(err),
    };
    return response;
  }
	
  // path1 = { device_id: "DID", key: "key", path: [{lat: "lat", long: "long"}, ...] }
  // path2 = { device_id: "DID", key: "key", path: [{lat: "lat", long: "long"}, ...] }
  var path1 = {device_id: "", key: "", path: []};
  var path2 = {device_id: "", key: "", path: []};
  for (var i = 0; i < data.Items.length; i++){
    var row = data.Items[i];
    var id = row.id.N;
    var device_id = row.device_id.S;
    params = {
      RequestItems: {
    	"vele_thesis_location_latitude": {
    	  Keys: [{ id: { N: id } }]
    	},
        "vele_thesis_location_longitude": {
    	  Keys: [{ id: { N: id } }]
    	}
      }
    }
    var tmp;
    try {
      tmp = await ddb.batchGetItem(params).promise();
      //console.log(tmp);
    } catch (err) {
      console.log(err);
      const response = {
	statusCode: 500,
	body: JSON.stringify(err),
      };
      return response;
    }
    var lat = tmp.Responses.vele_thesis_location_latitude[0].latitude.S;
    var long = tmp.Responses.vele_thesis_location_longitude[0].longitude.S;
    if (device_id === path1.device_id) {
      path1.path.push({lat: lat, long: long});
    } else {
      if(device_id === path2.device_id){
	path2.path.push({lat: lat, long: long});
      } else {
	if (path1.device_id === ""){
	  path1.device_id = device_id;
	  params = {
	    RequestItems: {
	      "vele_thesis_location_key": {
	        Keys: [{ id: { N: id } }]
	      }
	    }
	  }
	  try {
	    tmp = await ddb.batchGetItem(params).promise();
	    //console.log(tmp);
	  } catch (err) {
	    console.log(err);
	    const response = {
	      statusCode: 500,
	      body: JSON.stringify(err),
	    };
	    return response;
	  }
	  var key = tmp.Responses.vele_thesis_location_key[0].key.S;
	  path1.key = key;
	  path1.path.push({lat: lat, long: long});
	} else {
	  path2.device_id = device_id;
	  params = {
	    RequestItems: {
	      "vele_thesis_location_key": {
		Keys: [{ id: { N: id } }]
	      }
	    }
	  }
	  try {
	    tmp = await ddb.batchGetItem(params).promise();
	    //console.log(tmp);
	  } catch (err) {
	    console.log(err);
	    const response = {
	      statusCode: 500,
	      body: JSON.stringify(err),
	    };
	    return response;
          }
	  var key = tmp.Responses.vele_thesis_location_key[0].key.S;
	  path2.key = key;
          path2.path.push({lat: lat, long: long});
        }
      }
    }
  }
  
  var paths = {path1: path1, path2: path2};
	
  const bucket = "vele-thesis-paths-bucket"; 
  var filename = uuid.v4();
  console.log("The initial filename = " + filename);

  params = {Bucket: bucket, Key: filename, Body: JSON.stringify(paths)}; //, ContentType: "application/json"};
  var uploadRet;
  try {
    uploadRet = await new Promise(function(resolve, reject) {
      s3.upload(params, (err, data) => {
        if (err) {
          reject({ success: false, error: err });
        } else {
          //console.log("The returned key is " + JSON.stringify(data));
          resolve({ success: true, data: data });
        }
      });
    });
    console.log(JSON.stringify(uploadRet));
    filename = uploadRet.success ? uploadRet.data.Key : "error";
  } catch(err) {
    console.log(err);
    const response = {
      statusCode: 500,
      body: JSON.stringify(err),
    };
    return response;
  }

  params = {Bucket: bucket, Key: filename, Expires: 60};
  var url;
  try {
    url = await s3.getSignedUrlPromise('getObject', params);
  } catch(err) {
    console.log(err);
    const response = {
      statusCode: 500,
      body: JSON.stringify(err),
    };
    return response;
  }

  const response = {
    statusCode: 200,
    body: JSON.stringify({url: url}),
  };
  console.log(response);
  return response;
};

