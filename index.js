// ##################################################################################################################################### //
// aws configure set default.s3.max_concurrent_requests 1000
// aws configure set default.s3.max_queue_size 100000
// ##################################################################################################################################### //
const AWS = require('aws-sdk')
const async = require('async');
const fs = require('fs');
const bucketName = "video-engineering"  // For development video-engineering bucket (testing purpose) -- For production change it to iflix-content-qa bucket
const oldPrefix = 'sources/';
const newPrefix = 'new-sources/';
const s3 = new AWS.S3({ params: { Bucket: bucketName }, region: 'ap-southeast-1' })

function init() {
    s3.listObjects({
        Delimiter: '/',
        Prefix: oldPrefix
    }, function (err, assetObject) {
        if (err) console.log("Error :: ", err)
        if (assetObject.Contents.length !== 0) {
            let successMessage = ''
            let errorMessage = ''
            async.each(assetObject.Contents, function (element, cb) {
                let firstCharOfContent = element.Key.split('/').pop()[0]
                if (firstCharOfContent !== undefined) {
                    const bucketContentFolder = newPrefix + firstCharOfContent.toUpperCase() + '/'
                    const params = {
                        CopySource: bucketName + '/' + element.Key,
                        Key: element.Key.replace(oldPrefix, bucketContentFolder)
                    };
                    s3.copyObject(params, function (copyErr, copyData) {
                        if (copyErr) {
                            errorMessage = errorMessage + 'Source: ' + element.Key + '   --   Error: ' + copyErr + '\n'
                            saveLog('error.txt', 'Copied: ' + errorMessage)
                            console.log("Error File", copyErr);
                        }
                        else {
                            successMessage = successMessage + 'Source: ' + element.Key + '   --   Destination: ' + params.Key + '\n'
                            saveLog('success.txt', successMessage)
                            console.log('Copied: ', params.Key);
                            cb()
                        }
                    });
                }
            })
        } else {
            console.log("Empty Bucket")
        }
    });
}

function saveLog(file, message) {
    const stream = fs.createWriteStream(file);
    stream.once('open', function (fd) {
        stream.write(message);
        stream.end();
    });
}

init();