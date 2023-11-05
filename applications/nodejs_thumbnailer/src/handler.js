const sharp = require('sharp');
const fs = require('fs');
const path = require('path');
const https = require('follow-redirects').https;

const filename = "tesla.jpg";
const url = "https://github.com/spcl/serverless-benchmarks-data/blob/6a17a460f289e166abb47ea6298fb939e80e8beb/400.inference/411.image-recognition/fake-resnet/800px-20180630_Tesla_Model_S_70D_2015_midnight_blue_left_front.jpg?raw=true";
const local_path = "./";

function streamToPromise(stream) {
    return new Promise((resolve, reject) => {
        stream.on("close", () => {
        resolve()
        });
        stream.on("error", reject);
    })
}
  
module.exports = async function(event, context = null) {
    let width = 1000;
    let height  = 1000;

    // Create resizer
    const sharp_resizer = sharp().resize(width, height).png();

    // Download file
    var file = fs.createWriteStream(local_path + filename);
    https.get(url, (res) => {
        res.pipe(sharp_resizer).pipe(file)
    });

    let check = streamToPromise(file).then(async () => {
        fs.stat(local_path + filename, (err) => {
            if (err != null) {
                return false
            } else {
                return true
            }
        });
    });
    await check;
    return {"result": check};
}
  
// var event = {
//     "width": 100,
//     "height": 100
// }
// var res = require("./handler")(event, null)
// console.log(res)
