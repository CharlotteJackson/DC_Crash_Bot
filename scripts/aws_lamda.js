var AWS = require('aws-sdk');

var dynamodb = new AWS.DynamoDB();

//var AWS = require('aws-sdk');
var dynamo = new AWS.DynamoDB.DocumentClient();

const https = require('https');
exports.handler = async (event) => {


    let dataString = '';



    var event_r = event.slice(0, 10);
    var processed_arr = []
    await Promise.all(
        event_r.map(async (e) => {

            try {
                await fullDataProcess(e);
            } catch (Err) {
                console.log('Data does not have a good address or something ??')
                console.log(Err)
            }
        }))

};


async function fullDataProcess(e) {
    var dataStringF = await basicGet(e.full_text);
    var parsedLoc = JSON.parse(dataStringF);
    var loc = null;

    try {
        loc = parsedLoc.results[0].geometry.location;
    } catch (Err) {
        console.log(Err)
        return
    }
    var sampleTweet = { tweet: e.full_text, google_geo: loc, id: e.id + "" };

    try {
        const result = await dynamo.put(
            {
                "operation": "create",
                "TableName": "TweetComboTable",
                "Item": sampleTweet
            }).promise();
        console.log(result);
    } catch (Err) {
        console.log(Err);
    }
}

async function basicGet(final_address) {
    let dataString = '';
    await new Promise((resolve, reject) => {

        const url = `https://maps.googleapis.com/maps/api/geocode/json?address=${final_address}&key=${process.env.google_key}`
        const req = https.get(url, function (res) {
            res.on('data', chunk => {
                dataString += chunk;
            });
            res.on('end', () => {
                resolve(
                    dataString
                    // {
                    //statusCode: 200,
                    //  body: JSON.parse(dataString)
                    //}

                );
            });
        });

        req.on('error', (e) => {
            reject({
                statusCode: 500,
                body: 'Something went wrong!'
            });
        });
    });
    return dataString;
}
